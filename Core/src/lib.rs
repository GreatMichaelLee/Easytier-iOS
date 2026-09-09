use std::{
    ffi::CString,
    fs::File,
    io::{self, Seek, SeekFrom, Write},
    sync::{Arc, Mutex, Once},
    time::Duration,
};

use easytier::{
    common::{
        config::{ConfigLoader, TomlConfigLoader},
        global_ctx::{EventBusSubscriber, GlobalCtxEvent},
    },
    instance::factory::{
        native_instance_manager_with_runtime, subscribe_native_instance_event, NativeInstanceManager,
    },
};
use easytier_core::{config::normalize_secure_mode_config, instance::manager::ConfigFileControl};
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use tokio::runtime::{Builder, Runtime};
use tracing_oslog::OsLogger;
use tracing_subscriber::layer::SubscriberExt as _;
use uuid::Uuid;

struct Ctx {
    rt: Runtime,
    manager: Arc<NativeInstanceManager>,
}

static CTX: Lazy<Ctx> = Lazy::new(|| {
    // Network Extensions have a tight memory budget, but 2 workers let the
    // core saturate them and starve any block_on() from the FFI thread.
    let rt = Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("tokio runtime for easytier-ios");
    let manager = Arc::new(native_instance_manager_with_runtime(rt.handle().clone()));
    Ctx { rt, manager }
});

static CURRENT: Lazy<Mutex<Option<Uuid>>> = Lazy::new(|| Mutex::new(None));

/// Last serialized running-info snapshot. Refreshed by a background task on
/// CTX.rt so the FFI getters never block the caller (startTunnel() calls
/// get_running_info() synchronously on its settings queue).
static INFO_CACHE: Lazy<Mutex<Option<String>>> = Lazy::new(|| Mutex::new(None));
static REFRESHER: Once = Once::new();

fn start_info_refresher() {
    REFRESHER.call_once(|| {
        CTX.rt.spawn(async {
            let mut tick = tokio::time::interval(Duration::from_millis(1000));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let uuid = CURRENT.lock().ok().and_then(|g| *g);
                let Some(uuid) = uuid else {
                    if let Ok(mut c) = INFO_CACHE.lock() {
                        *c = None;
                    }
                    continue;
                };
                if let Some(info) = CTX.manager.network_info(uuid).await {
                    if let Ok(s) = serde_json::to_string(&info) {
                        let s = densify_running_info(&s);
                        if let Ok(mut c) = INFO_CACHE.lock() {
                            *c = Some(s);
                        }
                    }
                }
            }
        });
    });
}

type SharedLogFile = Arc<Mutex<File>>;
static LOGGER_FILE: Lazy<Arc<Mutex<Option<SharedLogFile>>>> =
    Lazy::new(|| Arc::new(Mutex::new(None)));

/// pbjson drops any field equal to its proto default (empty list, 0, "",
/// false). The bundled iOS Swift models decode every key as required, so we
/// re-inflate the shapes they read before returning the snapshot.
fn obj_default(v: &mut Value, key: &str, default: Value) {
    if let Some(o) = v.as_object_mut() {
        o.entry(key).or_insert(default);
    }
}

fn densify_stun(v: &mut Value) {
    obj_default(v, "udp_nat_type", json!(0));
    obj_default(v, "tcp_nat_type", json!(0));
    obj_default(v, "last_update_time", json!(0));
    obj_default(v, "public_ip", json!([]));
}

fn densify_node(v: &mut Value) {
    obj_default(v, "hostname", json!(""));
    obj_default(v, "version", json!(""));
    if let Some(si) = v.get_mut("stun_info") {
        densify_stun(si);
    }
}

fn densify_route(v: &mut Value) {
    obj_default(v, "peer_id", json!(0));
    obj_default(v, "next_hop_peer_id", json!(0));
    obj_default(v, "cost", json!(0));
    obj_default(v, "path_latency", json!(0));
    obj_default(v, "proxy_cidrs", json!([]));
    obj_default(v, "hostname", json!(""));
    obj_default(v, "inst_id", json!(""));
    obj_default(v, "version", json!(""));
    if let Some(si) = v.get_mut("stun_info") {
        densify_stun(si);
    }
}

fn densify_conn(v: &mut Value) {
    obj_default(v, "conn_id", json!(""));
    obj_default(v, "my_peer_id", json!(0));
    obj_default(v, "is_client", json!(false));
    obj_default(v, "peer_id", json!(0));
    obj_default(v, "features", json!([]));
    obj_default(v, "loss_rate", json!(0.0));
    if let Some(st) = v.get_mut("stats") {
        for k in ["rx_bytes", "tx_bytes", "rx_packets", "tx_packets", "latency_us"] {
            obj_default(st, k, json!(0));
        }
    }
}

fn densify_peer(v: &mut Value) {
    obj_default(v, "peer_id", json!(0));
    obj_default(v, "conns", json!([]));
    obj_default(v, "directly_connected_conns", json!([]));
    if let Some(cs) = v.get_mut("conns").and_then(Value::as_array_mut) {
        for c in cs {
            densify_conn(c);
        }
    }
}

fn densify_running_info(s: &str) -> String {
    let Ok(mut v) = serde_json::from_str::<Value>(s) else {
        return s.to_string();
    };
    obj_default(&mut v, "dev_name", json!(""));
    obj_default(&mut v, "events", json!([]));
    obj_default(&mut v, "routes", json!([]));
    obj_default(&mut v, "peers", json!([]));
    obj_default(&mut v, "peer_route_pairs", json!([]));
    obj_default(&mut v, "running", json!(false));
    if let Some(n) = v.get_mut("my_node_info") {
        densify_node(n);
    }
    if let Some(rs) = v.get_mut("routes").and_then(Value::as_array_mut) {
        for r in rs {
            densify_route(r);
        }
    }
    if let Some(ps) = v.get_mut("peers").and_then(Value::as_array_mut) {
        for p in ps {
            densify_peer(p);
        }
    }
    if let Some(prs) = v.get_mut("peer_route_pairs").and_then(Value::as_array_mut) {
        for pr in prs {
            if let Some(r) = pr.get_mut("route") {
                densify_route(r);
            } else {
                obj_default(pr, "route", json!({}));
                if let Some(r) = pr.get_mut("route") {
                    densify_route(r);
                }
            }
            if let Some(p) = pr.get_mut("peer") {
                densify_peer(p);
            }
        }
    }
    serde_json::to_string(&v).unwrap_or_else(|_| s.to_string())
}

fn current_uuid() -> Result<Uuid, String> {
    CURRENT
        .lock()
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "no running instance".to_string())
}

fn prepare_network_config(cfg_str: &str) -> Result<TomlConfigLoader, String> {
    let cfg = TomlConfigLoader::new_from_str(cfg_str).map_err(|e| e.to_string())?;
    if let Some(secure_mode) = cfg.get_secure_mode() {
        let secure_mode = normalize_secure_mode_config(secure_mode).map_err(|e| e.to_string())?;
        if secure_mode.enabled {
            let private_key = secure_mode.private_key().map_err(|e| e.to_string())?;
            let public_key = secure_mode.public_key().map_err(|e| e.to_string())?;
            let derived_public_key = x25519_dalek::PublicKey::from(&private_key);
            if public_key.as_bytes() != derived_public_key.as_bytes() {
                return Err("local public key does not match local private key".to_string());
            }
        }
        cfg.set_secure_mode(Some(secure_mode));
    }
    Ok(cfg)
}

#[derive(Clone)]
struct SharedLogWriter {
    file: SharedLogFile,
}

struct SharedLogWriteGuard {
    file: SharedLogFile,
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SharedLogWriter {
    type Writer = SharedLogWriteGuard;
    fn make_writer(&'a self) -> Self::Writer {
        SharedLogWriteGuard {
            file: self.file.clone(),
        }
    }
}

impl Write for SharedLogWriteGuard {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut file = self
            .file
            .lock()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        file.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        let mut file = self
            .file
            .lock()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        file.flush()
    }
}

fn ret(err_msg: *mut *const std::ffi::c_char, r: Result<(), String>) -> std::ffi::c_int {
    match r {
        Ok(()) => 0,
        Err(e) => {
            if !err_msg.is_null() {
                if let Ok(cstr) = CString::new(e) {
                    unsafe { *err_msg = cstr.into_raw() };
                }
            }
            -1
        }
    }
}

/// # Safety
/// Initialize logger
#[no_mangle]
pub extern "C" fn init_logger(
    path: *const std::ffi::c_char,
    level: *const std::ffi::c_char,
    subsystem: *const std::ffi::c_char,
    err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    let path = unsafe { std::ffi::CStr::from_ptr(path).to_string_lossy().into_owned() };
    let level = unsafe { std::ffi::CStr::from_ptr(level).to_string_lossy().into_owned() };
    let subsystem = unsafe {
        std::ffi::CStr::from_ptr(subsystem)
            .to_string_lossy()
            .into_owned()
    };

    let impl_func = || {
        if LOGGER_FILE.lock().map_err(|e| e.to_string())?.is_some() {
            return Ok::<(), String>(());
        }
        let file = Arc::new(Mutex::new(File::create(path).map_err(|e| e.to_string())?));
        let collector = tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new(level))
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(SharedLogWriter { file: file.clone() })
                    .with_ansi(false),
            )
            .with(OsLogger::new(&subsystem, "rust"));
        tracing::subscriber::set_global_default(collector).map_err(|e| e.to_string())?;
        *LOGGER_FILE.lock().map_err(|e| e.to_string())? = Some(file);
        Ok(())
    };
    ret(err_msg, impl_func())
}

/// # Safety
/// Clear the currently initialized file logger and reset its file offset.
#[no_mangle]
pub extern "C" fn clear_logger(err_msg: *mut *const std::ffi::c_char) -> std::ffi::c_int {
    let impl_func = || -> Result<(), String> {
        let file = LOGGER_FILE
            .lock()
            .map_err(|e| e.to_string())?
            .clone()
            .ok_or("logger is not initialized".to_string())?;
        let mut file = file.lock().map_err(|e| e.to_string())?;
        file.set_len(0).map_err(|e| e.to_string())?;
        file.seek(SeekFrom::Start(0)).map_err(|e| e.to_string())?;
        file.flush().map_err(|e| e.to_string())?;
        Ok(())
    };
    ret(err_msg, impl_func())
}

/// # Safety
/// Set the tun fd for the running instance.
#[no_mangle]
pub extern "C" fn set_tun_fd(
    fd: std::ffi::c_int,
    err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    let impl_func = || -> Result<(), String> {
        let uuid = current_uuid()?;
        CTX.manager
            .attach_tun_fd(uuid, fd)
            .map_err(|e| e.to_string())?;
        Ok(())
    };
    ret(err_msg, impl_func())
}

/// # Safety
/// Free a string previously returned by this library.
#[no_mangle]
pub extern "C" fn free_string(s: *const std::ffi::c_char) {
    if s.is_null() {
        return;
    }
    unsafe {
        let _ = CString::from_raw(s as *mut std::ffi::c_char);
    }
}

/// # Safety
/// Run the network instance from TOML config text.
#[no_mangle]
pub extern "C" fn run_network_instance(
    cfg_str: *const std::ffi::c_char,
    err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    let impl_func = || -> Result<(), String> {
        if cfg_str.is_null() {
            return Err("cfg_str is nullptr".to_string());
        }
        let cfg_str = unsafe {
            std::ffi::CStr::from_ptr(cfg_str)
                .to_string_lossy()
                .into_owned()
        };
        let cfg = prepare_network_config(&cfg_str)?;
        let uuid = CTX
            .manager
            .run_network_instance(cfg, ConfigFileControl::STATIC_CONFIG)
            .map_err(|e| e.to_string())?;
        *CURRENT.lock().map_err(|e| e.to_string())? = Some(uuid);
        start_info_refresher();
        Ok(())
    };
    ret(err_msg, impl_func())
}

/// # Safety
/// Stop the running network instance.
#[no_mangle]
pub extern "C" fn stop_network_instance() -> std::ffi::c_int {
    let uuid = match CURRENT.lock() {
        Ok(mut g) => g.take(),
        Err(_) => return -1,
    };
    if let Ok(mut c) = INFO_CACHE.lock() {
        *c = None;
    }
    if let Some(uuid) = uuid {
        let manager = CTX.manager.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        CTX.rt.spawn(async move {
            let _ = manager.delete_network_instances([uuid]).await;
            let _ = tx.send(());
        });
        // Don't let a wedged runtime hang stopTunnel(); the task above still
        // runs to completion in the background if this times out.
        let _ = rx.recv_timeout(Duration::from_secs(3));
    }
    0
}

fn subscribe_current() -> Result<EventBusSubscriber, String> {
    let uuid = current_uuid()?;
    let instance = CTX
        .manager
        .instance(uuid)
        .ok_or("instance not found".to_string())?;
    subscribe_native_instance_event(&instance).ok_or("no event subscriber".to_string())
}

/// # Safety
/// Register a callback invoked once when the instance stops.
#[no_mangle]
pub extern "C" fn register_stop_callback(
    callback: Option<extern "C" fn()>,
    err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    let impl_func = || -> Result<(), String> {
        let callback = callback.ok_or("callback is null".to_string())?;
        let mut ev = subscribe_current()?;
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build();
            if let Ok(rt) = rt {
                rt.block_on(async move {
                    loop {
                        match ev.recv().await {
                            Ok(_) => {}
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        }
                    }
                });
                callback();
            }
        });
        Ok(())
    };
    ret(err_msg, impl_func())
}

/// # Safety
/// Register a callback invoked whenever running info may have changed.
#[no_mangle]
pub extern "C" fn register_running_info_callback(
    callback: Option<extern "C" fn()>,
    err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    let impl_func = || -> Result<(), String> {
        let callback = callback.ok_or("callback is null".to_string())?;
        let mut ev = subscribe_current()?;
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build();
            if let Ok(rt) = rt {
                rt.block_on(async move {
                    loop {
                        match ev.recv().await {
                            Ok(event) => match event {
                                GlobalCtxEvent::DhcpIpv4Changed(_, _)
                                | GlobalCtxEvent::ProxyCidrsUpdated(_, _, _, _)
                                | GlobalCtxEvent::ConfigPatched(_) => {
                                    callback();
                                }
                                _ => {}
                            },
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        }
                    }
                });
            }
        });
        Ok(())
    };
    ret(err_msg, impl_func())
}

/// # Safety
/// Get running info as a JSON string.
#[no_mangle]
pub extern "C" fn get_running_info(
    json: *mut *const std::ffi::c_char,
    err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    let impl_func = || -> Result<(), String> {
        if json.is_null() {
            return Err("json is a nullptr".to_string());
        }
        current_uuid()?;
        start_info_refresher();
        let info = INFO_CACHE
            .lock()
            .map_err(|e| e.to_string())?
            .clone()
            .ok_or("running info not ready yet".to_string())?;
        let cstr = CString::new(info).map_err(|e| e.to_string())?;
        unsafe { *json = cstr.into_raw() };
        Ok(())
    };
    ret(err_msg, impl_func())
}

/// # Safety
/// Get the latest error message for the running instance, or null.
#[no_mangle]
pub extern "C" fn get_latest_error_msg(
    msg: *mut *const std::ffi::c_char,
    err_msg: *mut *const std::ffi::c_char,
) -> std::ffi::c_int {
    let impl_func = || -> Result<(), String> {
        if msg.is_null() {
            return Err("msg is a nullptr".to_string());
        }
        let latest = INFO_CACHE
            .lock()
            .ok()
            .and_then(|c| c.clone())
            .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
            .and_then(|v| v.get("error_msg").and_then(|e| e.as_str().map(str::to_owned)));
        match latest {
            Some(latest) => {
                let cstr = CString::new(latest).map_err(|e| e.to_string())?;
                unsafe { *msg = cstr.into_raw() };
            }
            None => unsafe { *msg = std::ptr::null() },
        }
        Ok(())
    };
    ret(err_msg, impl_func())
}
