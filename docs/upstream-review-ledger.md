# Upstream review ledger

We don't auto-merge upstream `EasyTier/EasyTier-iOS` — someone has to look at
each commit and decide case-by-case (see `windows/easytier-fork-maintenance.md`
in the operator's memory). This file is the record of that review, so the same
commit never gets re-litigated from scratch in a later session. Modeled on
`protect.list`/`prefer-theirs.list` in the lede_x86 repo, but this one is a
plain read-it-yourself ledger, not something a script consumes automatically
-- there's no automated upstream-merge job for this repo to wire it into.

**Before merging anything new from upstream, check here first.** If a commit
is already listed, don't re-review it -- follow the recorded decision. Add an
entry for every upstream commit you evaluate, taken or not.

## How to update this file

When you review a new batch of upstream commits, append a dated section below
using the same table shape. `Commit` is the upstream short hash (from
`origin/main`, i.e. `EasyTier/EasyTier-iOS`). `Decision` is one of:

- **merged (cherry-pick)** -- taken as-is via `git cherry-pick`, our commit hash noted
- **merged (adapted)** -- the underlying idea taken but reimplemented against our own code/architecture, not cherry-picked; our commit hash noted
- **skipped** -- not taken, with the reason

## 2026-09-21 review (12 commits, `f218b9a..6a96b65` on `origin/main`)

| Commit | Subject | Decision | Reason |
|---|---|---|---|
| `a9d0628` | fix: handle protobuf JSON network status snapshots | merged (adapted) | Real bug, same class as the `default_conn_id: {}` crash we'd already patched for one field -- protobuf JSON omits fields at their zero/default value, and our synthesized `Codable` conformances required every key present. Reimplemented the same defensive-decode idea directly against our own `StatusModels.swift` (not cherry-picked -- upstream's version is against their own, differently-structured file). Our commit: `0d3048d` |
| `45ddef8` | chore: bump version and fix translation | skipped | Regenerates `Core/Cargo.lock` for upstream's own core dependency graph. Coupled to the `Core/Cargo.toml` source-pin change below. |
| `b8db144` | fix: revert to 2.6.4 | skipped | Repins `Core/Cargo.toml`'s `easytier` dependency to `https://github.com/EasyTier/EasyTier.git`. **Never merge this one** -- our `Core/Cargo.toml` deliberately points at our own private fork (`GreatMichaelLee/Easytier.git`, branch `main`), which is the entire reason this repo is forked. Taking this would silently switch our core dependency back to upstream's official repo. |
| `872d4d5` | fix: adapt core to EasyTier 2.6.4 and bump version to 1.2.1 | skipped | Adapts `Core/src/lib.rs` call sites to whatever Rust API shape upstream's pinned `EasyTier/EasyTier.git@v2.6.4` exposes (`NativeInstanceManager` -> `NetworkInstanceManager`, etc). Pure churn from the dependency-source change above; irrelevant since we don't share that pin. Our own `Core/src/lib.rs` has been compiling clean against our fork's current API (verified: iOS CI builds green through this date), so there's nothing broken here to fix. |
| `364aa76` | refactor(tunnel): share instance lifecycle across local and web modes | skipped | Part of the "Web management mode" feature (started upstream `c4f1547`, 2026-09-03, never merged into our fork at all -- we have zero of this feature, so there's no partial state to patch). Also: massively rewrites `Core/src/lib.rs` (720 lines) and `PacketTunnelProvider.swift` (464 lines), the exact tunnel-lifecycle files we've already independently patched (reasserting-spin fix, `.task(id:)` timer, etc) -- high conflict/regression risk even setting the missing-base-feature issue aside. |
| `941563f` | fix(profiles): preserve connection options across asynchronous selections | skipped | Depends on the Web-mode refactor above (local/Web mode profile switching). Same "we don't have the base feature" reasoning. |
| `e8770cf` | fix(ui): keep web management status and controls consistent | skipped | Web-mode UI, depends on the feature block above. |
| `275c831` | docs: record web mode audit and lifecycle validation | skipped | Documents the Web-mode feature we don't have. |
| `6240ebc` | feat(web): add secure mode toggle for management connections | skipped | Web-mode feature addition. Touches `EasyTierShared.swift` but only adds a `WebManagementOptions.secureMode` field -- that whole struct doesn't exist in our fork, so this doesn't even apply. |
| `ab2fd5b` | fix(ui): allow multiline web management server input | skipped | Web-mode UI. |
| `e0a8155` | refactor(ui): extract web management configuration editor | skipped | Web-mode UI. |
| `6a96b65` | chore: bump version | skipped | Version bump tied to the Web-mode feature landing; nothing to take on its own. |

**If we ever decide we *do* want Web management mode**: don't cherry-pick
these 8 piecemeal -- go back to `c4f1547` (2026-09-03, "feat: add web
configuration management") and evaluate the whole feature as one unit, then
this whole tail of fixups likely comes along with it or gets re-derived
against our own `Core/src/lib.rs`/`PacketTunnelProvider.swift` state at that
time (which will have moved on from today's).

## Also merged this pass (not from this 12-commit batch, but same session)

| Commit | Subject | Decision | Reason |
|---|---|---|---|
| `79342bd` | fix: handle omitted protobuf defaults in tunnel running info | merged (cherry-pick) | Fully isolated -- only touches `EasyTierNetworkExtension/InfoModels.swift`, no dependency on anything else in the batch, no overlap with our own code. Cherry-picked clean, no conflicts. Our commit: `f38366a` |
