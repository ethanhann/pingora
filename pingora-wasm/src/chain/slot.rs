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

//! Slot locking for request phases
//!
//! A phase locks the slot holding a plugin's guest for the length of each guest call. Guest
//! failures are turned into errors here, since that is also where a broken guest gets replaced.

use super::ctx::PluginRecord;
use crate::runtime::pool::{GuestPool, Loaded, SlotGuard};
use crate::{plugin_failure, plugin_unavailable};
use pingora_error::{Error, Result};
use proxy_wasm_host::abi::v0_2_1::GuestError;

/// A locked slot in a plugin's guest pool.
pub(super) struct LockedSlot<'a> {
    pub(super) pool: &'a GuestPool,
    pub(super) slot: usize,
    guard: SlotGuard<'a>,
}

impl<'a> LockedSlot<'a> {
    /// Lock a slot for a new request.
    pub(super) fn for_new_request(pool: &'a GuestPool) -> Result<Self> {
        let (slot, guard) = pool.pick()?;
        Ok(LockedSlot { pool, slot, guard })
    }

    /// Lock the slot whose guest holds a request's context.
    ///
    /// # Errors
    ///
    /// Returns [ERR_PLUGIN_FAILED](crate::ERR_PLUGIN_FAILED) if the guest that held the context
    /// is no longer in its slot, because it was replaced or lost.
    pub(super) fn of_request(pool: &'a GuestPool, record: &PluginRecord) -> Result<Self> {
        match pool.lock(record.slot, record.guest) {
            Some(guard) => Ok(LockedSlot {
                pool,
                slot: record.slot,
                guard,
            }),
            None => Err(plugin_unavailable(
                &pool.name,
                "guest for this request is gone",
            )),
        }
    }

    /// Return the guest in the locked slot.
    ///
    /// # Errors
    ///
    /// Returns [ERR_PLUGIN_FAILED](crate::ERR_PLUGIN_FAILED) if the slot is empty.
    pub(super) fn loaded(&mut self) -> Result<&mut Loaded> {
        match self.guard.as_mut() {
            Some(loaded) => Ok(loaded),
            None => Err(plugin_unavailable(&self.pool.name, "no guest available")),
        }
    }

    /// Turn a guest error into the [ERR_PLUGIN_FAILED](crate::ERR_PLUGIN_FAILED) error a phase
    /// returns.
    ///
    /// The slot's guest is replaced first if the error left it unusable. `what` is the part of
    /// the message after the plugin name.
    pub(super) fn guest_failure(self, what: &str, cause: GuestError) -> Box<Error> {
        self.pool.replace_if_unusable(self.slot, self.guard, &cause);
        plugin_failure(&self.pool.name, what, cause)
    }
}
