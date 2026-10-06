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

//! Fail policy on a TCP connection

use super::TcpPlugins;
use crate::chain::failure::FilterFailure;
use crate::chain::slot::LockedSlot;
use crate::chain::tcp::direction::Direction;
use crate::FailPolicy;
use pingora_error::Result;
use proxy_wasm_host::abi::v0_2_1::{Callback, GuestError};

impl TcpPlugins {
    pub(super) fn guest_failed(
        &mut self,
        position: usize,
        locked: LockedSlot<'_>,
        callback: Callback,
        error: GuestError,
    ) -> Result<()> {
        locked.replace_guest_if_unusable(&error);
        self.skip_or_fail(position, FilterFailure::guest_error(callback, error))
    }

    /// Apply the fail policy of the plugin at `position`. A skipped plugin's paused callbacks
    /// continue at the next plugin.
    pub(super) fn skip_or_fail(&mut self, position: usize, failure: FilterFailure) -> Result<()> {
        if self.ctx.pool_at(position).fail_policy == FailPolicy::Open {
            if let Some(direction) = self.changed_with_bytes_to_come(position) {
                let changed = format!("{} data", direction.name());
                let failure = failure.with_changed_data(&changed);
                return Err(self.ctx.failed_request_error(position, failure));
            }
        }
        self.ctx.skip_plugin_or_fail_request(position, failure)?;
        if self.new_connection_paused == Some(position) {
            self.resumes
                .push_back(super::Resume::NewConnection(position));
        }
        for direction in Direction::BOTH {
            if self.directions[direction.index()].held[position].paused {
                self.resumes
                    .push_back(super::Resume::Data(direction, position));
            }
        }
        self.wait_deadlines[position] = None;
        Ok(())
    }

    /// Return a direction whose data the plugin at `position` changed and whose end has not
    /// passed every plugin, since skipping the plugin would send the rest of it unchanged.
    fn changed_with_bytes_to_come(&self, position: usize) -> Option<Direction> {
        Direction::BOTH.into_iter().find(|direction| {
            let state = &self.directions[direction.index()];
            state.changed[position] && !state.ended
        })
    }
}
