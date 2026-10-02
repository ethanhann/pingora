// Copyright 2026 Cloudflare, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Guest pool
//!
//! A guest runs one callback at a time, so each plugin gets a pool of guests, one per slot. A
//! request stays on the slot it started on because its context is in that slot's guest.

pub(crate) mod events;
mod guest_start;
mod loaded;

pub(crate) use loaded::Loaded;

use crate::callout::PluginCalloutConf;
use crate::plugin_unavailable;
use crate::root_callbacks::RootCallbackPluginState;
use events::RootCallbackSender;
use guest_start::StartedGuest;
use log::{error, info, warn};
use parking_lot::{Mutex, MutexGuard};
use pingora_error::Result;
use proxy_wasm_host::abi::v0_2_1::{Callback, GuestError, GuestId, GuestSpec, PluginConfig};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const REBUILD_BACKOFF: Duration = Duration::from_secs(1);

/// The body and trailer phases a plugin runs in, and its body limits.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PluginPhases {
    pub(crate) request: bool,
    pub(crate) response: bool,
    pub(crate) trailers: bool,
    pub(crate) request_limit: usize,
    pub(crate) response_limit: usize,
}

impl PluginPhases {
    /// Return the phases the plugin runs in as a comma-separated list for logging.
    fn list(&self) -> String {
        let phases = [
            (true, "headers"),
            (self.request, "request bodies"),
            (self.response, "response bodies"),
            (self.trailers, "response trailers"),
        ];
        let names: Vec<_> = phases
            .iter()
            .filter_map(|(runs, name)| runs.then_some(*name))
            .collect();
        names.join(", ")
    }
}

pub(crate) type SlotGuard<'a> = MutexGuard<'a, Option<Loaded>>;

#[derive(Default)]
struct Slot {
    guest: Mutex<Option<Loaded>>,
    open: AtomicUsize,
    held: Arc<AtomicUsize>,
    failed_at: Mutex<Option<Instant>>,
}

/// The outcome of a non-blocking attempt to lock a guest's slot.
pub(crate) enum SlotLockAttempt<'a> {
    LockedGuest(SlotGuard<'a>),
    /// The slot is locked by another thread.
    Busy,
    /// The slot now holds a different guest, or none.
    GuestGone,
}

/// Settings for a pool and the guests it starts.
pub(crate) struct GuestPoolConf {
    pub(crate) pool_index: usize,
    pub(crate) name: String,
    pub(crate) spec: GuestSpec,
    pub(crate) plugin_config: PluginConfig,
    pub(crate) slot_count: usize,
    pub(crate) phases: PluginPhases,
    pub(crate) callout_conf: PluginCalloutConf,
    pub(crate) root_callback_plugin: Arc<RootCallbackPluginState>,
    pub(crate) root_callback_sender: RootCallbackSender,
}

pub(crate) struct GuestPool {
    pub(crate) name: String,
    pub(crate) phases: PluginPhases,
    pub(crate) callout_conf: Arc<PluginCalloutConf>,
    pool_index: usize,
    spec: GuestSpec,
    plugin: PluginConfig,
    root_callback_plugin: Arc<RootCallbackPluginState>,
    root_callback_sender: RootCallbackSender,
    next: AtomicUsize,
    slots: Vec<Slot>,
}

impl GuestPool {
    pub(crate) fn new(conf: GuestPoolConf) -> Result<Self> {
        let mut phases = conf.phases;
        let mut pool = GuestPool {
            name: conf.name,
            phases,
            callout_conf: Arc::new(conf.callout_conf),
            pool_index: conf.pool_index,
            spec: conf.spec,
            plugin: conf.plugin_config,
            root_callback_plugin: conf.root_callback_plugin,
            root_callback_sender: conf.root_callback_sender,
            next: AtomicUsize::new(0),
            slots: (0..conf.slot_count).map(|_| Slot::default()).collect(),
        };
        for index in 0..pool.slots.len() {
            let started = pool.start_guest(index)?;
            let mut guard = pool.slots[index].guest.lock();
            install_started_guest(&mut guard, started);
        }
        // A phase is only worth running if the plugin exports its callback
        if let Some(loaded) = pool.slots[0].guest.lock().as_ref() {
            let exports = |callback| loaded.guest.exports_callback(callback);
            phases.request &= exports(Callback::RequestBody);
            phases.response &= exports(Callback::ResponseBody);
            phases.trailers &= exports(Callback::ResponseTrailers);
        }
        pool.phases = phases;
        info!("wasm plugin {}: runs on {}", pool.name, phases.list());
        Ok(pool)
    }

    /// Lock a slot for a new request.
    ///
    /// Slots are tried round-robin, and the first unlocked slot with a guest wins. Failing that,
    /// one slot that lost its guest is rebuilt if its backoff has elapsed. Otherwise this blocks
    /// until a slot with a guest is unlocked.
    ///
    /// # Errors
    ///
    /// Returns [ERR_PLUGIN_FAILED](crate::ERR_PLUGIN_FAILED) if no slot has a guest.
    pub(crate) fn pick(&self) -> Result<(usize, SlotGuard<'_>)> {
        let count = self.slots.len();
        let first = self.next.fetch_add(1, Ordering::Relaxed) % count;
        let order = || (0..count).map(move |i| (first + i) % count);
        for index in order() {
            if let Some(guard) = self.slots[index].guest.try_lock() {
                if guard.is_some() {
                    return Ok((index, guard));
                }
            }
        }
        if let Some(index) = order().find(|index| self.rebuild_due(*index)) {
            self.rebuild(index, None);
            if let Some(guard) = self.slots[index].guest.try_lock() {
                if guard.is_some() {
                    return Ok((index, guard));
                }
            }
        }
        for index in order() {
            let guard = self.slots[index].guest.lock();
            if guard.is_some() {
                return Ok((index, guard));
            }
        }
        Err(plugin_unavailable(&self.name, "no guest available"))
    }

    /// Try to lock the slot holding `guest` without blocking.
    pub(crate) fn try_lock_guest(&self, index: usize, guest: GuestId) -> SlotLockAttempt<'_> {
        let Some(guard) = self.slots[index].guest.try_lock() else {
            return SlotLockAttempt::Busy;
        };
        match guard.as_ref() {
            Some(loaded) if loaded.guest.id() == guest => SlotLockAttempt::LockedGuest(guard),
            _ => SlotLockAttempt::GuestGone,
        }
    }

    /// Lock the slot a request is running on.
    ///
    /// Returns `None` if the guest holding the request's context is no longer in the slot.
    pub(crate) fn lock(&self, index: usize, guest: GuestId) -> Option<SlotGuard<'_>> {
        let guard = self.slots[index].guest.lock();
        match guard.as_ref() {
            Some(loaded) if loaded.guest.id() == guest => Some(guard),
            _ => None,
        }
    }

    /// Replace the guest in a slot if `err` left it unusable.
    ///
    /// A guest is unusable once it has stopped serving, e.g. after a trap, or has run out of
    /// context ids. Returns `true` if the guest was removed from its slot, even when building its
    /// replacement failed.
    pub(crate) fn replace_if_unusable(
        &self,
        index: usize,
        mut guard: SlotGuard<'_>,
        err: &GuestError,
    ) -> bool {
        let lost = match guard.as_ref() {
            Some(loaded) => {
                !loaded.guest.is_serving() || matches!(err, GuestError::ContextIdsExhausted)
            }
            None => false,
        };
        if !lost {
            return false;
        }
        guard.take();
        self.slots[index].open.store(0, Ordering::Relaxed);
        self.slots[index].held.store(0, Ordering::Relaxed);
        drop(guard);
        self.rebuild(index, Some(err));
        true
    }

    fn rebuild_due(&self, index: usize) -> bool {
        let slot = &self.slots[index];
        let due = slot
            .failed_at
            .lock()
            .is_none_or(|at| at.elapsed() >= REBUILD_BACKOFF);
        due && slot.guest.try_lock().is_some_and(|guard| guard.is_none())
    }

    /// Start a new guest in an empty slot.
    ///
    /// `failure` is the error that emptied the slot when the rebuild immediately follows it, and
    /// is only used for logging. A failed rebuild starts the slot's backoff.
    fn rebuild(&self, index: usize, failure: Option<&GuestError>) {
        let slot = &self.slots[index];
        let name = &self.name;
        match self.start_guest(index) {
            Ok(started) => {
                let mut guard = slot.guest.lock();
                if guard.is_none() {
                    slot.open.store(0, Ordering::Relaxed);
                    slot.held.store(0, Ordering::Relaxed);
                    install_started_guest(&mut guard, started);
                    *slot.failed_at.lock() = None;
                    match failure {
                        Some(failure) => warn!(
                            "wasm plugin {name}: guest in slot {index} replaced after failure: {failure}"
                        ),
                        None => warn!("wasm plugin {name}: guest in slot {index} rebuilt"),
                    }
                }
            }
            Err(e) => {
                *slot.failed_at.lock() = Some(Instant::now());
                match failure {
                    Some(failure) => error!(
                        "wasm plugin {name}: guest in slot {index} lost after failure: {failure}, rebuild failed: {e}"
                    ),
                    None => error!("wasm plugin {name}: failed to rebuild slot {index}: {e}"),
                }
            }
        }
    }

    pub(crate) fn opened(&self, index: usize) {
        self.slots[index].open.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn deleted(&self, index: usize) {
        let _ = self.slots[index]
            .open
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1));
    }

    pub(crate) fn open_contexts(&self) -> usize {
        self.slots
            .iter()
            .map(|s| s.open.load(Ordering::Relaxed))
            .sum()
    }

    pub(crate) fn held_contexts(&self) -> usize {
        self.slots
            .iter()
            .map(|s| s.held.load(Ordering::Relaxed))
            .sum()
    }
}

/// Install a started guest in its slot and forward the ticks, queues, and callouts it set up
/// during startup to the root callback thread.
fn install_started_guest(guard: &mut SlotGuard<'_>, started: StartedGuest) {
    let mut loaded = started.loaded;
    loaded.report_to_root_callbacks();
    loaded.send_callouts_to_root_callbacks(loaded.root, started.root_callouts);
    **guard = Some(loaded);
}

#[cfg(test)]
mod tests {
    use super::*;

    impl GuestPool {
        pub(crate) fn slot_count(&self) -> usize {
            self.slots.len()
        }

        /// Return `true` if the slot is not currently locked.
        pub(crate) fn is_slot_free(&self, index: usize) -> bool {
            self.slots[index].guest.try_lock().is_some()
        }

        pub(crate) fn lock_slot(&self, index: usize) -> SlotGuard<'_> {
            self.slots[index].guest.lock()
        }

        /// Replace the guest in a slot, as a trap would.
        pub(crate) fn replace_slot(&self, index: usize) {
            self.slots[index].guest.lock().take();
            self.rebuild(index, None);
        }

        pub(crate) fn fail_slot(&self, index: usize) {
            self.slots[index].guest.lock().take();
            *self.slots[index].failed_at.lock() = Some(Instant::now());
        }
    }

    use crate::test_support::{body_plugin, fixture, plugin, Wat, CONTINUE, HOLD};
    use crate::WasmRuntime;
    use std::thread;

    #[test]
    fn phase_list_has_headers_and_enabled_phases() {
        let headers = PluginPhases {
            request: false,
            response: false,
            trailers: false,
            request_limit: 1,
            response_limit: 1,
        };
        let cases = [
            (headers, "headers"),
            (
                PluginPhases {
                    response: true,
                    ..headers
                },
                "headers, response bodies",
            ),
            (
                PluginPhases {
                    request: true,
                    response: true,
                    trailers: true,
                    ..headers
                },
                "headers, request bodies, response bodies, response trailers",
            ),
        ];

        for (phases, expected) in cases {
            assert_eq!(phases.list(), expected);
        }
    }

    #[test]
    fn body_phase_runs_only_when_enabled_and_exported() {
        let exported = Wat {
            request_body: Some(HOLD),
            response_trailers: Some(CONTINUE),
            ..Wat::default()
        };
        let mut conf = body_plugin("a", exported);
        conf.response_trailers = false;

        let runtime = WasmRuntime::new(vec![conf]).unwrap();

        let phases = runtime.inner.pools[0].phases;
        assert!(phases.request, "enabled and exported");
        assert!(!phases.response, "enabled but not exported");
        assert!(!phases.trailers, "exported but disabled");
    }

    #[test]
    fn pick_skips_locked_and_empty_slots() {
        let runtime =
            WasmRuntime::new(vec![plugin("a", fixture("add-request-header"), 3)]).unwrap();
        let pool = &runtime.inner.pools[0];
        pool.fail_slot(1);
        let held = pool.lock_slot(0);

        let picked: Vec<_> = (0..4).map(|_| pool.pick().unwrap().0).collect();

        drop(held);
        assert_eq!(picked, [2, 2, 2, 2]);
    }

    #[test]
    fn pick_waits_for_busy_slot_during_rebuild_backoff() {
        let runtime =
            WasmRuntime::new(vec![plugin("a", fixture("add-request-header"), 2)]).unwrap();
        let pool = &runtime.inner.pools[0];
        pool.fail_slot(0);
        let busy = pool.lock_slot(1);

        let picked = thread::scope(|scope| {
            let picker = scope.spawn(|| pool.pick().map(|(slot, _)| slot).ok());
            thread::sleep(Duration::from_millis(50));
            drop(busy);
            picker.join().unwrap()
        });

        assert_eq!(picked, Some(1));
        assert!(pool.lock_slot(0).is_none());
    }

    #[test]
    fn plugin_reads_fixed_property_in_configure() {
        let wat = Wat {
            data_segments: r#"(data (i32.const 700) "node\00name")"#,
            configure: "(call $log_property (i32.const 700) (i32.const 9)) i32.const 1",
            ..Wat::default()
        };
        let logs = Arc::new(crate::test_support::RecordedGuestLogs::default());
        let mut fixed_properties = crate::WasmProperties::new();
        fixed_properties.insert(&["node", "name"], "edge-1");
        let services = crate::WasmServices {
            log_sink: logs.clone(),
            fixed_properties,
            ..crate::WasmServices::default()
        };
        let conf = plugin(
            "fixed-in-configure",
            crate::test_support::wat_guest("fixed", wat),
            1,
        );

        let _runtime = WasmRuntime::new_with_services(vec![conf], services).unwrap();

        assert_eq!(*logs.0.lock(), ["edge-1"]);
    }
}
