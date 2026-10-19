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

//! End of a TCP connection

use super::TcpPlugins;
use crate::chain::logging::finish;
use crate::chain::slot::LockedSlot;
use crate::chain::tcp::direction::Direction;
use crate::observability::{PluginFailure, PluginFailureOutcome};
use log::{debug, error};
use pingora_error::Result;
use proxy_wasm_host::abi::v0_2_1::types::PeerType;
use proxy_wasm_host::abi::v0_2_1::Callback;

impl TcpPlugins {
    /// Run the close callback of `side` for each plugin. A failure in it is logged, since the
    /// connection is already closing.
    pub(in crate::chain::tcp) fn run_close(
        &mut self,
        side: Direction,
        peer: PeerType,
    ) -> Result<()> {
        let runtime = self.ctx.chain.runtime.clone();
        let callback = match side {
            Direction::Downstream => Callback::DownstreamConnectionClose,
            Direction::Upstream => Callback::UpstreamConnectionClose,
        };
        for position in 0..self.ctx.records.len() {
            let Some(record) = self.ctx.records[position] else {
                continue;
            };
            let pool = &runtime.pools[self.ctx.chain.plugins[position]];
            let Some(mut locked) = LockedSlot::of_request(pool, &record) else {
                continue;
            };
            let Ok(loaded) = locked.loaded() else {
                continue;
            };
            let context = record.context;
            let result = self
                .ctx
                .run_for_context(loaded, context, |scope| match side {
                    Direction::Downstream => scope.on_downstream_connection_close(context, peer),
                    Direction::Upstream => scope.on_upstream_connection_close(context, peer),
                });
            match result {
                Ok(()) => self.apply_callback_requests(position, callback),
                Err(e) => {
                    error!("wasm plugin {}: {callback} failed: {e}", pool.name);
                    locked.replace_guest_if_unusable(&e);
                    let failure = PluginFailure::GuestError;
                    let outcome = PluginFailureOutcome::Failed;
                    self.ctx
                        .report_failure(position, failure, outcome, Some(callback));
                }
            }
        }
        self.run_resumes()
    }

    /// Run `proxy_on_done`, `proxy_on_log`, and `proxy_on_delete` for each plugin, in reverse
    /// chain order.
    pub(in crate::chain::tcp) fn end_contexts(&mut self) {
        let runtime = self.ctx.chain.runtime.clone();
        self.ctx.callouts.clear();
        self.log_bytes_still_held();
        for position in (0..self.ctx.records.len()).rev() {
            let Some(record) = self.ctx.records[position].take() else {
                continue;
            };
            let pool = &runtime.pools[self.ctx.chain.plugins[position]];
            pool.root_callback_plugin
                .remove_tcp_connection(record.guest, record.context);
            let Some(mut locked) = LockedSlot::of_request(pool, &record) else {
                continue;
            };
            let Ok(loaded) = locked.loaded() else {
                continue;
            };
            let result = self.ctx.run_for_context(loaded, record.context, |scope| {
                finish(scope, record.context, true)
            });
            self.ctx
                .end_or_hold_context(position, locked, record.context, result, true);
        }
    }

    fn log_bytes_still_held(&self) {
        for direction in Direction::BOTH {
            for (position, held) in self.directions[direction.index()].held.iter().enumerate() {
                if !held.bytes.is_empty() {
                    debug!(
                        "wasm plugin {}: connection closed with {} {} bytes still held",
                        self.ctx.pool_at(position).name,
                        held.bytes.len(),
                        direction.name()
                    );
                }
            }
        }
    }
}

impl Drop for TcpPlugins {
    fn drop(&mut self) {
        // A dropped task leaves records that `end_contexts` did not take
        for position in 0..self.ctx.records.len() {
            if let Some(record) = self.ctx.records[position] {
                let plugin = &self.ctx.pool_at(position).root_callback_plugin;
                plugin.remove_tcp_connection(record.guest, record.context);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::chain::tcp::testing::{tcp_plugin, ConnectOptions, TcpRuntime};
    use crate::test_support::{eventually, Wat};

    #[tokio::test]
    async fn context_kept_by_plugin_outlives_connection() {
        let wat = Wat {
            done: "i32.const 0",
            ..Wat::default()
        };
        let tcp = TcpRuntime::new(vec![tcp_plugin("a", wat)]);
        let connection = tcp.connect(ConnectOptions::default());

        drop((connection.client, connection.server));

        assert!(eventually(|| tcp.runtime.held_contexts() == 1).await);
    }
}
