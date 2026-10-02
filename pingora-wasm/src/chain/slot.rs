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

use super::ctx::PluginRecord;
use crate::plugin_unavailable;
use crate::runtime::pool::{GuestPool, Loaded, SlotGuard};
use pingora_error::Result;
use proxy_wasm_host::abi::v0_2_1::GuestError;

pub(super) struct LockedSlot<'a> {
    pub(super) pool: &'a GuestPool,
    pub(super) slot: usize,
    guard: SlotGuard<'a>,
}

impl<'a> LockedSlot<'a> {
    pub(super) fn for_new_request(pool: &'a GuestPool) -> Option<Self> {
        let (slot, guard) = pool.pick()?;
        Some(LockedSlot { pool, slot, guard })
    }

    pub(super) fn of_request(pool: &'a GuestPool, record: &PluginRecord) -> Option<Self> {
        // `None` if the guest that held the context was replaced or lost
        let guard = pool.lock(record.slot, record.guest)?;
        Some(LockedSlot {
            pool,
            slot: record.slot,
            guard,
        })
    }

    pub(super) fn loaded(&mut self) -> Result<&mut Loaded> {
        match self.guard.as_mut() {
            Some(loaded) => Ok(loaded),
            None => Err(plugin_unavailable(
                &self.pool.name,
                &format!("slot {} has no guest", self.slot),
            )),
        }
    }

    pub(super) fn replace_guest_if_unusable(self, cause: &GuestError) {
        self.pool.replace_if_unusable(self.slot, self.guard, cause);
    }
}
