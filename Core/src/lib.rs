use std::{
    collections::HashMap,
    ffi::CString,
    fs::File,
    io::{self, Seek, SeekFrom, Write},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, Once,
    },
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
use arc_swap::ArcSwapOption;
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

static CURRENT: Lazy<ArcSwapOption<Uuid>> = Lazy::new(ArcSwapOption::empty);

/// Overlay IPv6 from the running config; MyNodeInfo has no field for it.
static CONFIGURED_IPV6: Lazy<ArcSwapOption<String>> = Lazy::new(ArcSwapOption::empty);

/// Last serialized running-info snapshot. Refreshed by a background task on
/// CTX.rt so the FFI getters never block the caller (startTunnel() calls
/// get_running_info() synchronously on its settings queue).
static INFO_CACHE: Lazy<ArcSwapOption<String>> = Lazy::new(ArcSwapOption::empty);
static REFRESHER: Once = Once::new();

/// Lifetime count of overlay bytes received, accumulated per connection so it
/// never goes backwards when a peer conn is torn down and re-dialed -- our
/// force_reconnect and EasyTier's own reconnect both mint a fresh conn_id with
/// a zeroed byte counter. Written only by the single info-refresher task; read
/// lock-free by overlay_rx_total() from the FFI thread.
static RX_ACCUM: AtomicU64 = AtomicU64::new(0);

/// Fold one densified running-info snapshot into RX_ACCUM. `prev` is the
/// refresher task's private per-conn rx from the previous tick.
fn accumulate_rx(densified: &str, prev: &mut HashMap<String, u64>) {
    let Ok(v) = serde_json::from_str::<Value>(densified) else {
        return;
    };
    let mut cur: HashMap<String, u64> = HashMap::new();
    collect_conn_rx(&v, &mut cur);
    let mut delta: u64 = 0;
    for (id, &rx) in &cur {
        // new conn -> full rx; existing conn -> growth; a decrease within one
        // conn_id (shouldn't happen) contributes 0.
        delta = delta.saturating_add(rx.saturating_sub(prev.get(id).copied().unwrap_or(0)));
    }
    if delta > 0 {
        RX_ACCUM.fetch_add(delta, Ordering::Relaxed);
    }
    *prev = cur;
}

/// conn_id -> stats.rx_bytes for every PeerConnInfo in the tree, deduped by
/// conn_id (a conn appears under both `peers` and `peer_route_pairs`).
fn collect_conn_rx(v: &Value, out: &mut HashMap<String, u64>) {
    match v {
        Value::Object(m) => {
            if let Some(Value::String(id)) = m.get("conn_id") {
                let rx = m
                    .get("stats")
                    .and_then(|st| st.get("rx_bytes"))
                    .and_then(|x| match x {
                        Value::Number(n) => n.as_u64(),
                        Value::String(s) => s.parse().ok(),
                        _ => None,
                    })
                    .unwrap_or(0);
                out.insert(id.clone(), rx);
            }
            for child in m.values() {
                collect_conn_rx(child, out);
            }
        }
        Value::Array(a) => a.iter().for_each(|x| collect_conn_rx(x, out)),
        _ => {}
    }
}

fn start_info_refresher() {
    REFRESHER.call_once(|| {
        CTX.rt.spawn(async {
            let mut tick = tokio::time::interval(Duration::from_millis(1000));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut prev_rx: HashMap<String, u64> = HashMap::new();
            loop {
                tick.tick().await;
                let uuid = CURRENT.load().as_ref().map(|u| **u);
                let Some(uuid) = uuid else {
                    INFO_CACHE.store(None);
                    prev_rx.clear();
                    continue;
                };
                // Run the snapshot in a child task: a hang is bounded by the
                // timeout and a panic surfaces as a JoinError, either way this
                // loop keeps going and the cache never freezes.
                let manager = CTX.manager.clone();
                let work = CTX.rt.spawn(async move {
                    let mut info =
                        tokio::time::timeout(Duration::from_secs(5), manager.network_info(uuid))
                            .await
                            .ok()??;
                    for peer in &mut info.peers {
                        for conn in &mut peer.conns {
                            if !conn.loss_rate.is_finite() {
                                conn.loss_rate = 0.0;
                            }
                        }
                    }
                    for pair in &mut info.peer_route_pairs {
                        if let Some(peer) = pair.peer.as_mut() {
                            for conn in &mut peer.conns {
                                if !conn.loss_rate.is_finite() {
                                    conn.loss_rate = 0.0;
                                }
                            }
                        }
                    }
                    let s = serde_json::to_string(&info).ok()?;
                    Some(densify_running_info(&s))
                });
                if let Ok(Some(s)) = work.await {
                    accumulate_rx(&s, &mut prev_rx);
                    INFO_CACHE.store(Some(Arc::new(s)));
                }
            }
        });
    });
}

type SharedLogFile = Arc<Mutex<File>>;
static LOGGER_FILE: Lazy<Arc<Mutex<Option<SharedLogFile>>>> =
    Lazy::new(|| Arc::new(Mutex::new(None)));

/// pbjson drops any proto field equal to its default (empty list, 0, "",
/// false). The bundled iOS Swift models (NetworkStatus / RunningInfo) decode
/// every key as required, so re-inflate the shapes they read.
fn od(v: &mut Value, key: &str, default: Value) {
    if let Some(o) = v.as_object_mut() {
        o.entry(key).or_insert(default);
    }
}

fn d_url(v: &mut Value) {
    od(v, "url", json!(""));
}

fn d_cidr(v: &mut Value) {
    od(v, "network_length", json!(0));
    if let Some(a) = v.get_mut("address") {
        if a.get("part1").is_some()
            || a.get("part2").is_some()
            || a.get("part3").is_some()
            || a.get("part4").is_some()
        {
            for k in ["part1", "part2", "part3", "part4"] {
                od(a, k, json!(0));
            }
        } else {
            od(a, "addr", json!(0));
        }
    }
}

/// pbjson serializes proto enums as their name; the Swift NATType is `Int`.
fn nat_name_to_int(name: &str) -> i64 {
    match name {
        "OpenInternet" => 1,
        "NoPAT" => 2,
        "FullCone" => 3,
        "Restricted" => 4,
        "PortRestricted" => 5,
        "Symmetric" => 6,
        "SymUdpFirewall" => 7,
        "SymmetricEasyInc" => 8,
        "SymmetricEasyDec" => 9,
        _ => 0, // "Unknown" or anything unexpected
    }
}

/// Coerce a NAT-type field into a valid `NATType` raw value (0..=9). pbjson
/// emits the enum name; a stale/unknown value (name or number) becomes 0.
fn coerce_nat(v: &mut Value, key: &str) {
    let n = match v.get(key) {
        Some(Value::String(s)) => nat_name_to_int(s),
        Some(Value::Number(num)) => match num.as_i64() {
            Some(x) if (0..=9).contains(&x) => x,
            _ => 0,
        },
        None => 0,
        _ => 0,
    };
    if let Some(o) = v.as_object_mut() {
        o.insert(key.to_string(), json!(n));
    }
}

/// pbjson serializes 64-bit ints as decimal strings; Swift decodes them as
/// `Int` (Int64 on every supported device). Fold string / float / missing into
/// a non-negative i64, clamping an above-i64::MAX counter instead of zeroing.
fn coerce_u64(v: &mut Value, key: &str) {
    let n: i64 = match v.get(key) {
        Some(Value::String(s)) => s
            .parse::<u64>()
            .map(|u| u.min(i64::MAX as u64) as i64)
            .or_else(|_| s.parse::<i64>())
            .unwrap_or(0)
            .max(0),
        Some(Value::Number(num)) => num
            .as_u64()
            .map(|u| u.min(i64::MAX as u64) as i64)
            .or_else(|| num.as_i64())
            .or_else(|| num.as_f64().map(|f| f.max(0.0).min(i64::MAX as f64) as i64))
            .unwrap_or(0)
            .max(0),
        None => 0,
        _ => 0,
    };
    if let Some(o) = v.as_object_mut() {
        o.insert(key.to_string(), json!(n));
    }
}

fn d_stun(v: &mut Value) {
    coerce_nat(v, "udp_nat_type");
    coerce_nat(v, "tcp_nat_type");
    coerce_u64(v, "last_update_time");
    od(v, "public_ip", json!([]));
}

fn d_feature_flag(v: &mut Value) {
    for k in [
        "is_public_server",
        "avoid_relay_data",
        "kcp_input",
        "no_relay_kcp",
        "support_conn_list_sync",
        "quic_input",
        "no_relay_quic",
    ] {
        od(v, k, json!(false));
    }
}

fn d_node(v: &mut Value) {
    od(v, "hostname", json!(""));
    od(v, "version", json!(""));
    if let Some(ip6) = CONFIGURED_IPV6.load_full() {
        od(v, "virtual_ipv6", json!(ip6.as_str()));
    }
    if let Some(x) = v.get_mut("virtual_ipv4") {
        d_cidr(x);
    }
    if let Some(x) = v.get_mut("stun_info") {
        d_stun(x);
    }
    if let Some(x) = v.get_mut("ips") {
        for k in [
            "interface_ipv4s",
            "interface_ipv6s",
            "listeners",
        ] {
            od(x, k, json!([]));
        }
    }
}

fn d_route(v: &mut Value) {
    od(v, "peer_id", json!(0));
    od(v, "next_hop_peer_id", json!(0));
    od(v, "cost", json!(0));
    od(v, "path_latency", json!(0));
    od(v, "proxy_cidrs", json!([]));
    od(v, "hostname", json!(""));
    od(v, "inst_id", json!(""));
    od(v, "version", json!(""));
    for k in ["ipv4_addr", "ipv6_addr"] {
        if let Some(x) = v.get_mut(k) {
            d_cidr(x);
        }
    }
    if let Some(x) = v.get_mut("stun_info") {
        d_stun(x);
    }
    if let Some(x) = v.get_mut("feature_flag") {
        d_feature_flag(x);
    }
}

fn d_conn(v: &mut Value) {
    od(v, "conn_id", json!(""));
    od(v, "my_peer_id", json!(0));
    od(v, "is_client", json!(false));
    od(v, "peer_id", json!(0));
    od(v, "features", json!([]));
    od(v, "loss_rate", json!(0.0));
    if let Some(t) = v.get_mut("tunnel") {
        od(t, "tunnel_type", json!(""));
        od(t, "local_addr", json!({ "url": "" }));
        od(t, "remote_addr", json!({ "url": "" }));
        if let Some(x) = t.get_mut("local_addr") {
            d_url(x);
        }
        if let Some(x) = t.get_mut("remote_addr") {
            d_url(x);
        }
    }
    if let Some(st) = v.get_mut("stats") {
        for k in ["rx_bytes", "tx_bytes", "rx_packets", "tx_packets", "latency_us"] {
            coerce_u64(st, k);
        }
    }
}

fn d_peer(v: &mut Value) {
    od(v, "peer_id", json!(0));
    od(v, "conns", json!([]));
    od(v, "directly_connected_conns", json!([]));
    if let Some(cs) = v.get_mut("conns").and_then(Value::as_array_mut) {
        for c in cs {
            d_conn(c);
        }
    }
}

fn densify_running_info(src: &str) -> String {
    let Ok(mut v) = serde_json::from_str::<Value>(src) else {
        return src.to_string();
    };
    if !v.is_object() {
        v = json!({});
    }
    od(&mut v, "dev_name", json!(""));
    od(&mut v, "events", json!([]));
    od(&mut v, "routes", json!([]));
    od(&mut v, "peers", json!([]));
    od(&mut v, "peer_route_pairs", json!([]));
    od(&mut v, "running", json!(false));
    if let Some(n) = v.get_mut("my_node_info") {
        d_node(n);
    }
    if let Some(rs) = v.get_mut("routes").and_then(Value::as_array_mut) {
        for r in rs {
            d_route(r);
        }
    }
    if let Some(ps) = v.get_mut("peers").and_then(Value::as_array_mut) {
        for pp in ps {
            d_peer(pp);
        }
    }
    if let Some(prs) = v.get_mut("peer_route_pairs").and_then(Value::as_array_mut) {
        for pr in prs {
            if pr.get("route").is_none() {
                od(pr, "route", json!({}));
            }
            if let Some(r) = pr.get_mut("route") {
                d_route(r);
            }
            if let Some(pp) = pr.get_mut("peer") {
                d_peer(pp);
            }
        }
    }
    serde_json::to_string(&v).unwrap_or_else(|_| src.to_string())
}

fn current_uuid() -> Result<Uuid, String> {
    CURRENT
        .load()
        .as_ref()
        .map(|u| **u)
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
        CONFIGURED_IPV6.store(cfg.get_ipv6().map(|inet| Arc::new(inet.to_string())));
        let uuid = CTX
            .manager
            .run_network_instance(cfg, ConfigFileControl::STATIC_CONFIG)
            .map_err(|e| e.to_string())?;
        CURRENT.store(Some(Arc::new(uuid)));
        start_info_refresher();
        Ok(())
    };
    ret(err_msg, impl_func())
}

/// # Safety
/// Stop the running network instance.
#[no_mangle]
pub extern "C" fn stop_network_instance() -> std::ffi::c_int {
    let uuid = CURRENT.swap(None).as_ref().map(|u| **u);
    // INFO_CACHE / CONFIGURED_IPV6 are left to their single producers: the
    // refresher clears the cache on its next tick, and get_running_info gates
    // on current_uuid() so a stale snapshot is never served after a stop.
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

/// # Safety
/// Close every peer connection; the manual connectors then re-dial on their
/// next tick. Instance, TUN, routes and the assigned address are kept.
#[no_mangle]
pub extern "C" fn force_reconnect(err_msg: *mut *const std::ffi::c_char) -> std::ffi::c_int {
    let impl_func = || -> Result<(), String> {
        let uuid = current_uuid()?;
        let instance = CTX
            .manager
            .instance(uuid)
            .ok_or_else(|| "instance not found".to_string())?;
        CTX.rt.spawn(async move {
            let closed = tokio::time::timeout(Duration::from_secs(5), async {
                let mut n = 0usize;
                for snap in instance.peer_snapshots().await {
                    let mut ids = snap.directly_connected_conns.clone();
                    if let Some(d) = snap.default_conn_id {
                        if !ids.contains(&d) {
                            ids.push(d);
                        }
                    }
                    for cid in ids {
                        if instance.close_peer_conn(snap.peer_id, &cid).await.is_ok() {
                            n += 1;
                        }
                    }
                }
                n
            })
            .await
            .unwrap_or(0);
            tracing::warn!(closed, "force_reconnect: dropped peer conns");
        });
        Ok(())
    };
    ret(err_msg, impl_func())
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
            let Ok(rt) = rt else { return };
            rt.block_on(async move {
                // Two one-shot nudges so a split-tunnel config settles its
                // learned routes once. No repeating timer: applyNetworkSettings
                // flaps `reasserting` even on a no-op, so a steady stream of
                // callbacks makes iOS drop/re-establish the tunnel forever.
                let nudge = callback;
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    nudge();
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    nudge();
                });
                loop {
                    match ev.recv().await {
                        Ok(event) => match event {
                            GlobalCtxEvent::DhcpIpv4Changed(_, _)
                            | GlobalCtxEvent::DhcpIpv4Conflicted(_)
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
        });
        Ok(())
    };
    ret(err_msg, impl_func())
}

/// # Safety
/// Lifetime count of overlay bytes received, monotonic across peer-conn churn.
/// The iOS extension's wake() heal check samples this before/after ~20s to tell
/// "recovered on its own" from "stalled, needs force_reconnect".
#[no_mangle]
pub extern "C" fn overlay_rx_total() -> u64 {
    start_info_refresher();
    RX_ACCUM.load(Ordering::Relaxed)
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
        let guard = INFO_CACHE.load();
        let cstr = match guard.as_deref() {
            Some(s) => CString::new(s.as_str()),
            None => CString::new(densify_running_info("{}")),
        }
        .map_err(|e| e.to_string())?;
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
            .load()
            .as_deref()
            .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
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
