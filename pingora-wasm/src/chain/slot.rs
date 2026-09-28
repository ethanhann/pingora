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

//! The slot lock that each phase takes, and the error for a plugin failure.

use super::ctx::PluginRecord;
use crate::runtime::pool::{GuestPool, Loaded, SlotGuard};
use crate::{plugin_failure, plugin_unavailable};
use pingora_error::{Error, Result};
use proxy_wasm_host::abi::v0_2_1::GuestError;

/// A locked slot of a plugin.
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

    /// Lock the slot that holds the context of a request.
    pub(super) fn of_request(pool: &'a GuestPool, record: &PluginRecord) -> Result<Self> {
        match pool.lock(record.slot, record.guest) {
            Some(guard) => Ok(LockedSlot {
                pool,
                slot: record.slot,
                guard,
            }),
            None => Err(plugin_unavailable(
                &pool.name,
                "lost the guest of this request",
            )),
        }
    }

    pub(super) fn loaded(&mut self) -> Result<&mut Loaded> {
        match self.guard.as_mut() {
            Some(loaded) => Ok(loaded),
            None => Err(plugin_unavailable(&self.pool.name, "has no guest")),
        }
    }

    /// Return the error for a guest failure.
    ///
    /// A failure that leaves the guest unusable also replaces the guest.
    pub(super) fn guest_failure(self, what: &str, cause: GuestError) -> Box<Error> {
        self.pool.check(self.slot, self.guard, &cause);
        plugin_failure(&self.pool.name, what, cause)
    }
}
