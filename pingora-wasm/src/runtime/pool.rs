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

//! The guests of one plugin.
//!
//! A guest runs one callback at a time, so a plugin keeps several guests, one in each slot. A
//! request stays on the slot it started on, because its plugin contexts live in that guest.

use crate::{plugin_failure, plugin_unavailable};
use log::{error, warn};
use parking_lot::{Mutex, MutexGuard};
use pingora_error::Result;
use proxy_wasm_host::abi::v0_2_1::{
    ContextId, Guest, GuestError, GuestId, GuestSpec, PluginConfig, Started,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const REBUILD_BACKOFF: Duration = Duration::from_secs(1);

/// A started guest and the root context of its plugin.
pub(crate) struct Loaded {
    pub(crate) guest: Guest,
    pub(crate) root: ContextId,
}

pub(crate) type SlotGuard<'a> = MutexGuard<'a, Option<Loaded>>;

#[derive(Default)]
struct Slot {
    guest: Mutex<Option<Loaded>>,
    open: AtomicUsize,
    held: AtomicUsize,
    failed_at: Mutex<Option<Instant>>,
}

pub(crate) struct GuestPool {
    pub(crate) name: String,
    spec: GuestSpec,
    plugin: PluginConfig,
    next: AtomicUsize,
    slots: Vec<Slot>,
}

impl GuestPool {
    pub(crate) fn new(
        name: String,
        spec: GuestSpec,
        plugin: PluginConfig,
        slots: usize,
    ) -> Result<Self> {
        let pool = GuestPool {
            name,
            spec,
            plugin,
            next: AtomicUsize::new(0),
            slots: (0..slots).map(|_| Slot::default()).collect(),
        };
        for slot in &pool.slots {
            *slot.guest.lock() = Some(pool.start()?);
        }
        Ok(pool)
    }

    fn start(&self) -> Result<Loaded> {
        let mut guest = self
            .spec
            .build()
            .map_err(|e| plugin_failure(&self.name, "could not be built", e))?;
        match guest.start(self.plugin.clone()) {
            Ok(Started::Serving(root)) => Ok(Loaded { guest, root }),
            Ok(Started::Refused { callback, .. }) => Err(plugin_unavailable(
                &self.name,
                &format!("refused its start in {callback}"),
            )),
            Err(e) => Err(plugin_failure(&self.name, "failed to start", e)),
        }
    }

    /// Picks a slot for a new request and locks it.
    ///
    /// A free slot with a guest comes first. Otherwise one slot whose guest was lost is rebuilt
    /// when its backoff has passed. When every slot is busy, this waits for one.
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
            self.rebuild(index);
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
        Err(plugin_unavailable(&self.name, "has no guest"))
    }

    /// Locks the slot of a request, or returns `None` when a new guest replaced the one that
    /// holds the request's context.
    pub(crate) fn lock(&self, index: usize, guest: GuestId) -> Option<SlotGuard<'_>> {
        let guard = self.slots[index].guest.lock();
        match guard.as_ref() {
            Some(loaded) if loaded.guest.id() == guest => Some(guard),
            _ => None,
        }
    }

    /// Replaces the guest of a slot when it cannot serve after `err`, that is after a trap or
    /// when it has no context ids left.
    pub(crate) fn check(&self, index: usize, mut guard: SlotGuard<'_>, err: &GuestError) {
        let lost = match guard.as_ref() {
            Some(loaded) => {
                !loaded.guest.is_serving() || matches!(err, GuestError::ContextIdsExhausted)
            }
            None => false,
        };
        if !lost {
            return;
        }
        error!("wasm plugin {} lost its guest: {err}", self.name);
        guard.take();
        self.slots[index].open.store(0, Ordering::Relaxed);
        self.slots[index].held.store(0, Ordering::Relaxed);
        drop(guard);
        self.rebuild(index);
    }

    fn rebuild_due(&self, index: usize) -> bool {
        let slot = &self.slots[index];
        let due = slot
            .failed_at
            .lock()
            .is_none_or(|at| at.elapsed() >= REBUILD_BACKOFF);
        due && slot.guest.try_lock().is_some_and(|guard| guard.is_none())
    }

    fn rebuild(&self, index: usize) {
        let slot = &self.slots[index];
        match self.start() {
            Ok(loaded) => {
                let mut guard = slot.guest.lock();
                if guard.is_none() {
                    *guard = Some(loaded);
                    slot.open.store(0, Ordering::Relaxed);
                    slot.held.store(0, Ordering::Relaxed);
                    *slot.failed_at.lock() = None;
                    warn!(
                        "wasm plugin {} rebuilt the guest of slot {index}",
                        self.name
                    );
                }
            }
            Err(e) => {
                *slot.failed_at.lock() = Some(Instant::now());
                error!(
                    "wasm plugin {} could not rebuild slot {index}: {e}",
                    self.name
                );
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

    pub(crate) fn held(&self, index: usize) {
        self.deleted(index);
        self.slots[index].held.fetch_add(1, Ordering::Relaxed);
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

    #[cfg(test)]
    pub(crate) fn slot_count(&self) -> usize {
        self.slots.len()
    }

    #[cfg(test)]
    pub(crate) fn lock_slot(&self, index: usize) -> SlotGuard<'_> {
        self.slots[index].guest.lock()
    }

    /// Replaces the guest of a slot, as a trap does.
    #[cfg(test)]
    pub(crate) fn replace_slot(&self, index: usize) {
        self.slots[index].guest.lock().take();
        self.rebuild(index);
    }

    #[cfg(test)]
    pub(crate) fn fail_slot(&self, index: usize) {
        self.slots[index].guest.lock().take();
        *self.slots[index].failed_at.lock() = Some(Instant::now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{fixture, plugin};
    use crate::WasmRuntime;
    use std::thread;

    #[test]
    fn pick_skips_a_locked_slot_and_a_slot_with_no_guest() {
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
    fn a_recent_failed_rebuild_waits_for_its_backoff() {
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
}
