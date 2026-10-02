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

//! Slot locking for filters
//!
//! A filter locks the slot holding a plugin's guest for the length of each guest call. The
//! same lock is used to replace a guest that a failure left unusable.

use super::ctx::PluginRecord;
use crate::plugin_unavailable;
use crate::runtime::pool::{GuestPool, Loaded, SlotGuard};
use pingora_error::Result;
use proxy_wasm_host::abi::v0_2_1::GuestError;

/// A locked slot in a plugin's guest pool.
pub(super) struct LockedSlot<'a> {
    pub(super) pool: &'a GuestPool,
    pub(super) slot: usize,
    guard: SlotGuard<'a>,
}

impl<'a> LockedSlot<'a> {
    /// Lock a slot for a new request.
    ///
    /// Returns `None` if none of the plugin's slots has a guest.
    pub(super) fn for_new_request(pool: &'a GuestPool) -> Option<Self> {
        let (slot, guard) = pool.pick()?;
        Some(LockedSlot { pool, slot, guard })
    }

    /// Lock the slot whose guest holds a request's context.
    ///
    /// Returns `None` if the guest that held the context is no longer in its slot, because it
    /// was replaced or lost.
    pub(super) fn of_request(pool: &'a GuestPool, record: &PluginRecord) -> Option<Self> {
        let guard = pool.lock(record.slot, record.guest)?;
        Some(LockedSlot {
            pool,
            slot: record.slot,
            guard,
        })
    }

    /// Return the guest in the locked slot.
    ///
    /// # Errors
    ///
    /// Returns [ERR_PLUGIN_FAILED](crate::ERR_PLUGIN_FAILED) if the slot is empty.
    pub(super) fn loaded(&mut self) -> Result<&mut Loaded> {
        match self.guard.as_mut() {
            Some(loaded) => Ok(loaded),
            None => Err(plugin_unavailable(
                &self.pool.name,
                &format!("slot {} has no guest", self.slot),
            )),
        }
    }

    /// Replace the slot's guest if `cause` left it unusable.
    pub(super) fn replace_guest_if_unusable(self, cause: &GuestError) {
        self.pool.replace_if_unusable(self.slot, self.guard, cause);
    }
}
