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

//! Callout waits on a TCP connection

use super::TcpPlugins;
use crate::callout::CalloutDelivery;
use crate::chain::failure::FilterFailure;
use crate::chain::slot::LockedSlot;
use crate::chain::tcp::direction::Direction;
use crate::chain::wait::callout_wait_deadline;
use crate::stream_state::BodyBuffer;
use bytes::Bytes;
use pingora_error::Result;
use proxy_wasm_host::abi::v0_2_1::{Callback, CalloutId};
use std::mem;
use std::time::Instant;

impl TcpPlugins {
    pub(in crate::chain::tcp) fn next_wait_deadline(&self) -> Option<Instant> {
        self.wait_deadlines.iter().flatten().min().copied()
    }

    /// Fail each plugin whose callout wait has passed `callout_wait_limit`.
    pub(in crate::chain::tcp) fn expire_waits(&mut self, now: Instant) -> Result<()> {
        for position in 0..self.wait_deadlines.len() {
            if self.wait_deadlines[position].is_none_or(|deadline| deadline > now) {
                continue;
            }
            self.wait_deadlines[position] = None;
            let limit = self.ctx.pool_at(position).callout_conf.wait_limit;
            let callback = self.paused_callback(position);
            self.skip_or_fail(position, FilterFailure::wait_limit(callback, limit))?;
        }
        self.run_resumes()
    }

    /// Fail each plugin that stays paused with no callout once neither side can send more bytes,
    /// since nothing would run it again.
    pub(in crate::chain::tcp) fn fail_stalled_pauses(&mut self) -> Result<()> {
        // A callout result can resume a direction whose data then calls a paused plugin again
        if self.next_wait_deadline().is_some() {
            return Ok(());
        }
        for position in 0..self.wait_deadlines.len() {
            if !self.has_open_pause(position) {
                continue;
            }
            let callback = self.paused_callback(position);
            let detail = "paused with no callout to wait for after both sides stopped sending";
            self.skip_or_fail(position, FilterFailure::paused(callback, detail))?;
        }
        self.run_resumes()
    }

    pub(super) fn deliver_to(
        &mut self,
        position: usize,
        id: CalloutId,
        delivery: &CalloutDelivery,
    ) -> Result<()> {
        let Some(record) = self.ctx.records[position] else {
            return Ok(());
        };
        let callback = delivery.callback();
        let runtime = self.ctx.chain.runtime.clone();
        let pool = &runtime.pools[self.ctx.chain.plugins[position]];
        let Some(mut locked) = LockedSlot::of_request(pool, &record) else {
            return self.skip_or_fail(position, FilterFailure::guest_lost(record.slot, callback));
        };
        let loaded = locked.loaded()?;
        // The plugin can read and change the data of each direction it paused
        for direction in Direction::BOTH {
            let held = &mut self.directions[direction.index()].held[position];
            if held.paused {
                let bytes = Bytes::from(mem::take(&mut held.bytes));
                *self.tcp().data_mut(direction.stream_type()) =
                    Some(BodyBuffer::new(Vec::new(), bytes));
            }
        }
        self.ctx.stream().delivery_callback = Some(self.paused_callback(position));
        let result = self.ctx.run_for_context(loaded, record.context, |scope| {
            delivery.deliver(scope, record.context, id)
        });
        self.ctx.stream().delivery_callback = None;
        for direction in Direction::BOTH {
            let Some(buffer) = self.tcp().data_mut(direction.stream_type()).take() else {
                continue;
            };
            let state = &mut self.directions[direction.index()];
            state.changed[position] |= buffer.was_written_by_guest();
            state.held[position].bytes = buffer.into_vec();
        }
        if let Err(e) = result {
            return self.guest_failed(position, locked, callback, e);
        }
        self.apply_callback_requests(position, callback);
        Ok(())
    }

    /// Start the callouts of the plugin's last callback, and keep, start, or stop its callout
    /// wait to match its open pauses.
    pub(super) fn update_wait(&mut self, position: usize) {
        let open = self.has_open_pause(position);
        self.ctx.start_callouts(position, open);
        if !open {
            self.stop_waiting(position);
        } else if !self.ctx.callouts.cover_for_wait(position) {
            self.wait_deadlines[position] = None;
        } else if self.wait_deadlines[position].is_none() {
            // The limit counts from the first callback that paused with a callout to wait for
            let limit = self.ctx.pool_at(position).callout_conf.wait_limit;
            self.wait_deadlines[position] = Some(callout_wait_deadline(limit));
        }
    }

    pub(super) fn refresh_wait(&mut self, position: usize) {
        if !self.has_open_pause(position) {
            self.stop_waiting(position);
        }
    }

    fn stop_waiting(&mut self, position: usize) {
        self.wait_deadlines[position] = None;
        self.ctx.callouts.drop_stream_events(position);
        self.ctx.callouts.forget_pending(position);
    }

    pub(super) fn has_open_pause(&self, position: usize) -> bool {
        self.new_connection_paused == Some(position)
            || Direction::BOTH
                .iter()
                .any(|direction| self.directions[direction.index()].held[position].paused)
    }

    pub(super) fn open_pause_flags(&self) -> Vec<bool> {
        (0..self.wait_deadlines.len())
            .map(|position| self.has_open_pause(position))
            .collect()
    }

    pub(super) fn paused_callback(&self, position: usize) -> Callback {
        if self.new_connection_paused == Some(position) {
            return Callback::NewConnection;
        }
        let downstream = &self.directions[Direction::Downstream.index()];
        match downstream.held[position].paused {
            true => Callback::DownstreamData,
            false => Callback::UpstreamData,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::chain::tcp::testing::{
        receive, receive_to_end, send, tcp_plugin, ConnectOptions, TcpRuntime,
    };
    use crate::test_support::callouts::FixedSender;
    use crate::test_support::{eventually, Wat};
    use crate::{PluginFailure, PluginFailureOutcome};
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;
    use tokio::sync::Notify;

    fn asks_on_downstream_data() -> Wat {
        Wat {
            downstream_data: Some("(call $call_authz_and_pause)"),
            upstream_data: Some("i32.const 0"),
            http_call_response: Some("(call $continue (i32.const 2))"),
            ..Wat::default()
        }
    }

    #[tokio::test]
    async fn other_direction_flows_during_callout_wait() {
        let gate = Arc::new(Notify::new());
        let sender = FixedSender::responds_after("allowed", gate.clone());
        let plugins = vec![tcp_plugin("a", asks_on_downstream_data())];
        let tcp = TcpRuntime::with_sender(plugins, sender.clone());
        let mut connection = tcp.connect(ConnectOptions::default());
        send(&mut connection.client, "ask").await;
        assert!(eventually(|| sender.sent_count() == 1).await);
        send(&mut connection.server, "x").await;
        assert_eq!(receive(&mut connection.client, 1).await, "x");

        gate.notify_one();

        assert_eq!(receive(&mut connection.server, 3).await, "ask");
    }

    #[tokio::test]
    async fn expired_callout_wait_closes_connection() {
        let sender = FixedSender::responds_after("allowed", Arc::new(Notify::new()));
        let mut conf = tcp_plugin("a", asks_on_downstream_data());
        conf.callout_timeout_limit = Duration::from_millis(10);
        conf.callout_wait_limit = Duration::from_millis(50);
        let tcp = TcpRuntime::with_sender(vec![conf], sender);
        let mut connection = tcp.connect(ConnectOptions::default());

        send(&mut connection.client, "ask").await;

        assert_eq!(receive_to_end(&mut connection.client).await, "");
        let failure = PluginFailure::WaitLimit;
        let callback = Some("proxy_on_downstream_data".to_string());
        let want = [(
            "a".to_string(),
            failure,
            PluginFailureOutcome::Failed,
            callback,
        )];
        assert_eq!(tcp.failures.failures(), want);
    }

    #[tokio::test]
    async fn grpc_stream_stops_at_end_of_connection_context() {
        let wat = Wat {
            new_connection: Some("(call $open_grpc_stream) (i32.const 0)"),
            ..Wat::default()
        };
        let sender = FixedSender::grpc_released_by(Arc::new(Notify::new()), Vec::new());
        let tcp = TcpRuntime::with_sender(vec![tcp_plugin("a", wat)], sender.clone());
        let connection = tcp.connect(ConnectOptions::default());
        let streams = || sender.running_streams.load(Ordering::Relaxed);
        assert!(eventually(|| streams() == 1).await);

        drop((connection.client, connection.server));

        assert!(eventually(|| streams() == 0).await);
    }

    #[tokio::test]
    async fn waiting_plugin_gets_result_after_both_peers_end() {
        let gate = Arc::new(Notify::new());
        let sender = FixedSender::responds_after("allowed", gate.clone());
        let plugins = vec![tcp_plugin("a", asks_on_downstream_data())];
        let tcp = TcpRuntime::with_sender(plugins, sender.clone());
        let mut connection = tcp.connect(ConnectOptions::default());
        send(&mut connection.client, "x").await;
        assert!(eventually(|| sender.sent_count() == 1).await);
        connection.server.shutdown().await.unwrap();
        assert_eq!(receive_to_end(&mut connection.client).await, "");
        connection.client.shutdown().await.unwrap();

        gate.notify_one();

        assert_eq!(receive_to_end(&mut connection.server).await, "x");
    }

    #[tokio::test]
    async fn pause_is_not_stalled_while_another_plugin_waits_for_callout() {
        let pauses = Wat {
            downstream_data: Some("i32.const 1"),
            upstream_data: Some("(call $continue (i32.const 2)) (i32.const 0)"),
            ..Wat::default()
        };
        let asks = Wat {
            upstream_data: Some("(call $call_authz_and_pause)"),
            http_call_response: Some("(call $continue (i32.const 3))"),
            ..Wat::default()
        };
        let gate = Arc::new(Notify::new());
        let sender = FixedSender::responds_after("allowed", gate.clone());
        let plugins = vec![tcp_plugin("a", pauses), tcp_plugin("b", asks)];
        let tcp = TcpRuntime::with_sender(plugins, sender.clone());
        let mut connection = tcp.connect(ConnectOptions::default());
        send(&mut connection.client, "x").await;
        send(&mut connection.server, "y").await;
        assert!(eventually(|| sender.sent_count() == 1).await);
        connection.client.shutdown().await.unwrap();
        connection.server.shutdown().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        gate.notify_one();

        assert_eq!(receive_to_end(&mut connection.server).await, "x");
        assert_eq!(receive_to_end(&mut connection.client).await, "y");
        assert_eq!(tcp.failures.failures(), []);
    }
}
