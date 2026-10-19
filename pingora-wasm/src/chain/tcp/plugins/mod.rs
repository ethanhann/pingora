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

//! Plugin callbacks of a TCP connection

mod end;
mod failure;
mod wait;

use super::direction::{Direction, DirectionState, HeldData};
use crate::callout::CalloutDelivery;
use crate::chain::ctx::PluginRecord;
use crate::chain::failure::FilterFailure;
use crate::chain::slot::LockedSlot;
use crate::chain::WasmCtx;
use crate::properties::built_in::RequestFacts;
use crate::runtime::RuntimeInner;
use crate::stream_state::{BodyBuffer, TcpCallbackState};
use bytes::Bytes;
use pingora_error::Result;
use proxy_wasm_host::abi::v0_2_1::types::Action;
use proxy_wasm_host::abi::v0_2_1::{Callback, CalloutId, StreamKind};
use std::collections::VecDeque;
use std::mem;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

/// A paused callback that its plugin asked to continue from a later callback.
enum Resume {
    NewConnection(usize),
    Data(Direction, usize),
}

/// The plugins of one TCP connection and the bytes each one holds.
///
/// Bytes a plugin pauses stay at its chain position, so the plugins before it see each byte
/// once, as they do for a body.
pub(super) struct TcpPlugins {
    ctx: WasmCtx,
    directions: [DirectionState; 2],
    new_connection_paused: Option<usize>,
    new_connection_done: bool,
    /// Deadline of each plugin's callout wait, set while it waits for a covered callout.
    wait_deadlines: Vec<Option<Instant>>,
    resumes: VecDeque<Resume>,
    close_requested: bool,
    /// The bytes of each direction that have passed every plugin.
    output: [Vec<u8>; 2],
    facts: Arc<RequestFacts>,
}

impl TcpPlugins {
    pub(super) fn new(mut ctx: WasmCtx, facts: RequestFacts) -> Self {
        let plugins = ctx.records.len();
        let stream = ctx.stream();
        stream.request_facts = facts.clone();
        stream.tcp = Some(TcpCallbackState::default());
        TcpPlugins {
            ctx,
            directions: [DirectionState::new(plugins), DirectionState::new(plugins)],
            new_connection_paused: None,
            new_connection_done: false,
            wait_deadlines: vec![None; plugins],
            resumes: VecDeque::new(),
            close_requested: false,
            output: [Vec::new(), Vec::new()],
            facts: Arc::new(facts),
        }
    }

    pub(super) fn runtime(&self) -> &Arc<RuntimeInner> {
        &self.ctx.chain.runtime
    }

    pub(super) fn new_connection_done(&self) -> bool {
        self.new_connection_done
    }

    pub(super) fn close_requested(&self) -> bool {
        self.close_requested
    }

    pub(super) fn start(&mut self) -> Result<()> {
        self.run_new_connection_from(0)?;
        self.run_resumes()
    }

    /// Run the plugins on bytes read from the source of `direction`.
    pub(super) fn push(&mut self, direction: Direction, bytes: Vec<u8>, end: bool) -> Result<()> {
        self.run_data(direction, None, bytes, end)?;
        self.run_resumes()
    }

    pub(super) fn deliver(
        &mut self,
        position: usize,
        id: CalloutId,
        delivery: &CalloutDelivery,
    ) -> Result<()> {
        self.deliver_to(position, id, delivery)?;
        self.run_resumes()
    }

    /// Wait for the next callout result of a plugin with an open pause. Never returns while no
    /// such plugin waits for a callout, so it can be used in a `select!` branch.
    pub(super) async fn next_callout(&mut self) -> (usize, CalloutId, CalloutDelivery) {
        let open = self.open_pause_flags();
        let has_open_pause = |position: usize| open.get(position).copied().unwrap_or(false);
        match self.ctx.callouts.next_result_at(has_open_pause).await {
            Some(next) => next,
            None => std::future::pending().await,
        }
    }

    pub(super) fn take_output(&mut self, direction: Direction) -> Vec<u8> {
        mem::take(&mut self.output[direction.index()])
    }

    pub(super) fn ended(&self, direction: Direction) -> bool {
        self.directions[direction.index()].ended
    }

    pub(super) fn held_bytes(&self, direction: Direction) -> usize {
        self.directions[direction.index()].held_bytes()
    }

    pub(super) fn upstream_connected(&mut self, address: Option<SocketAddr>) {
        self.ctx.stream().request_facts.upstream_address = address;
        self.facts = Arc::new(self.ctx.stream().request_facts.clone());
        for position in 0..self.ctx.records.len() {
            if let Some(record) = self.ctx.records[position] {
                let plugin = &self.ctx.pool_at(position).root_callback_plugin;
                plugin.add_tcp_connection(record.guest, record.context, self.facts.clone());
            }
        }
    }

    fn run_new_connection_from(&mut self, start: usize) -> Result<()> {
        for position in start..self.ctx.records.len() {
            let Some(action) = self.open_context(position)? else {
                continue;
            };
            let paused = action == Action::Pause && !self.continue_requested(Direction::Downstream);
            if paused {
                self.new_connection_paused = Some(position);
            }
            self.apply_callback_requests(position, Callback::NewConnection);
            if !paused {
                continue;
            }
            if self.wait_deadlines[position].is_some() {
                return Ok(());
            }
            // No data is read until every plugin has continued, so nothing would call this plugin again
            self.new_connection_paused = None;
            let detail = "paused on new connection with no callout to wait for";
            let failure = FilterFailure::paused(Callback::NewConnection, detail);
            self.skip_or_fail(position, failure)?;
        }
        self.new_connection_done = true;
        Ok(())
    }

    fn open_context(&mut self, position: usize) -> Result<Option<Action>> {
        let runtime = self.ctx.chain.runtime.clone();
        let pool = &runtime.pools[self.ctx.chain.plugins[position]];
        let Some(mut locked) = LockedSlot::for_new_request(pool) else {
            self.skip_or_fail(position, FilterFailure::unavailable())?;
            return Ok(None);
        };
        let slot = locked.slot;
        let loaded = locked.loaded()?;
        let (root, guest) = (loaded.root, loaded.guest.id());
        let created = self.ctx.run(&mut loaded.guest, |scope| {
            scope.on_context_create(Some(root))
        });
        let context = match created {
            Ok(context) => context,
            Err(e) => {
                self.guest_failed(position, locked, Callback::ContextCreate, e)?;
                return Ok(None);
            }
        };
        pool.opened(slot);
        self.ctx.records[position] = Some(PluginRecord {
            slot,
            guest,
            context,
        });
        let plugin = &pool.root_callback_plugin;
        plugin.add_tcp_connection(guest, context, self.facts.clone());
        let action = self.ctx.run_for_context(loaded, context, |scope| {
            scope.expect_stream_kind(context, StreamKind::Tcp)?;
            scope.on_new_connection(context)
        });
        match action {
            Ok(action) => Ok(Some(action)),
            Err(e) => {
                self.guest_failed(position, locked, Callback::NewConnection, e)?;
                Ok(None)
            }
        }
    }

    fn run_data(
        &mut self,
        direction: Direction,
        after: Option<usize>,
        mut bytes: Vec<u8>,
        end: bool,
    ) -> Result<()> {
        let plugins = self.ctx.records.len();
        for position in direction.positions_after(plugins, after) {
            if self.ctx.records[position].is_none() || self.ctx.is_skipped(position) {
                continue;
            }
            match self.run_data_at(direction, position, bytes, end)? {
                Some(passed) => bytes = passed,
                None => return Ok(()),
            }
        }
        self.output[direction.index()].extend_from_slice(&bytes);
        if end {
            self.directions[direction.index()].ended = true;
        }
        Ok(())
    }

    /// Run the data callback of the plugin at `position` with its held bytes and `bytes`, and
    /// return the bytes to pass on, or `None` if the plugin paused them.
    fn run_data_at(
        &mut self,
        direction: Direction,
        position: usize,
        bytes: Vec<u8>,
        end: bool,
    ) -> Result<Option<Vec<u8>>> {
        let index = direction.index();
        let held = mem::take(&mut self.directions[index].held[position]);
        let mut data = held.bytes;
        data.extend_from_slice(&bytes);
        let runtime = self.ctx.chain.runtime.clone();
        let pool = &runtime.pools[self.ctx.chain.plugins[position]];
        let Some(record) = self.ctx.records[position] else {
            return Ok(Some(data));
        };
        let callback = direction.data_callback();
        let Some(mut locked) = LockedSlot::of_request(pool, &record) else {
            self.skip_or_fail(position, FilterFailure::guest_lost(record.slot, callback))?;
            return Ok(Some(data));
        };
        let loaded = locked.loaded()?;
        let size = u32::try_from(data.len()).unwrap_or(u32::MAX);
        *self.tcp().data_mut(direction.stream_type()) =
            Some(BodyBuffer::new(Vec::new(), Bytes::from(data)));
        let context = record.context;
        let action = self
            .ctx
            .run_for_context(loaded, context, |scope| match direction {
                Direction::Downstream => scope.on_downstream_data(context, size, end),
                Direction::Upstream => scope.on_upstream_data(context, size, end),
            });
        let buffer = self.tcp().data_mut(direction.stream_type()).take();
        let buffer = buffer.unwrap_or_default();
        if buffer.was_written_by_guest() {
            self.directions[index].changed[position] = true;
        }
        let data = buffer.into_vec();
        let action = match action {
            Ok(action) => action,
            Err(e) => {
                self.guest_failed(position, locked, callback, e)?;
                return Ok(Some(data));
            }
        };
        let paused = action == Action::Pause && !self.continue_requested(direction);
        let passed = match paused {
            true => {
                self.directions[index].held[position] = HeldData {
                    bytes: data,
                    end,
                    paused: true,
                };
                None
            }
            false => Some(data),
        };
        self.apply_callback_requests(position, callback);
        Ok(passed)
    }

    /// Act on what the plugin at `position` asked for in its last callback, `ran`.
    fn apply_callback_requests(&mut self, position: usize, ran: Callback) {
        for direction in Direction::BOTH {
            let paused = self.directions[direction.index()].held[position].paused;
            // A continue from the callback that paused a direction was applied in that callback
            if ran != direction.data_callback() && paused && self.continue_requested(direction) {
                self.resumes.push_back(Resume::Data(direction, position));
            }
        }
        if ran != Callback::NewConnection
            && self.new_connection_paused == Some(position)
            && self.continue_requested(Direction::Downstream)
        {
            self.resumes.push_back(Resume::NewConnection(position));
        }
        if self.tcp().take_close_request() {
            self.close_requested = true;
        }
        self.update_wait(position);
    }

    fn run_resumes(&mut self) -> Result<()> {
        while let Some(resume) = self.resumes.pop_front() {
            match resume {
                Resume::NewConnection(position) => {
                    if self.new_connection_paused != Some(position) {
                        continue;
                    }
                    self.new_connection_paused = None;
                    self.refresh_wait(position);
                    self.run_new_connection_from(position + 1)?;
                }
                Resume::Data(direction, position) => {
                    let held = &mut self.directions[direction.index()].held[position];
                    if !held.paused {
                        continue;
                    }
                    let HeldData { bytes, end, .. } = mem::take(held);
                    self.refresh_wait(position);
                    self.run_data(direction, Some(position), bytes, end)?;
                }
            }
        }
        Ok(())
    }

    fn continue_requested(&mut self, direction: Direction) -> bool {
        self.ctx
            .stream()
            .continue_requested(direction.stream_type())
    }

    fn tcp(&mut self) -> &mut TcpCallbackState {
        let tcp = &mut self.ctx.stream().tcp;
        tcp.as_mut()
            .expect("set when the plugins of a connection are created")
    }
}

#[cfg(test)]
mod tests {
    use crate::chain::tcp::testing::{
        receive, receive_to_end, send, stays_silent, tcp_plugin, ConnectOptions, TcpRuntime,
        PAUSE_UNDER_SIX,
    };
    use crate::test_support::{eventually, Wat};
    use crate::{FailPolicy, PluginFailure, PluginFailureOutcome};
    use tokio::io::AsyncWriteExt;

    const PAUSE_AND_LOG: &str = "(call $log_text (i32.const 790) (i32.const 6)) (i32.const 1)";
    const MARK_A_DOWNSTREAM: &str = "(call $mark_a (i32.const 2))";
    const MARK_A_UPSTREAM: &str = "(call $mark_a (i32.const 3))";
    const MARK_B_UPSTREAM: &str = "(call $mark_b (i32.const 3))";
    const CONTINUE_DOWNSTREAM: &str = "(call $continue (i32.const 2)) (i32.const 0)";
    const CONTINUE_UPSTREAM: &str = "(call $continue (i32.const 3)) (i32.const 0)";

    #[tokio::test]
    async fn paused_plugin_sees_grown_buffer_and_continues_in_order() {
        let wat = Wat {
            downstream_data: Some(PAUSE_UNDER_SIX),
            ..Wat::default()
        };
        let tcp = TcpRuntime::new(vec![tcp_plugin("a", wat)]);
        let mut connection = tcp.connect(ConnectOptions::default());
        send(&mut connection.client, "abc").await;
        assert!(eventually(|| tcp.logs() == ["paused"]).await);
        send(&mut connection.client, "def").await;

        let upstream = receive(&mut connection.server, 6).await;

        assert_eq!(upstream, "abcdef");
        assert_eq!(tcp.logs(), ["paused"]);
    }

    #[tokio::test]
    async fn earlier_plugin_sees_each_byte_once_while_later_plugin_pauses() {
        let marks = Wat {
            downstream_data: Some(MARK_A_DOWNSTREAM),
            ..Wat::default()
        };
        let pauses = Wat {
            downstream_data: Some(PAUSE_UNDER_SIX),
            ..Wat::default()
        };
        let tcp = TcpRuntime::new(vec![tcp_plugin("a", marks), tcp_plugin("b", pauses)]);
        let mut connection = tcp.connect(ConnectOptions::default());
        send(&mut connection.client, "xy").await;
        assert!(eventually(|| tcp.logs() == ["paused"]).await);
        send(&mut connection.client, "zw").await;

        let upstream = receive(&mut connection.server, 6).await;

        assert_eq!(upstream, "axyazw");
    }

    #[tokio::test]
    async fn upstream_data_runs_plugins_in_reverse_order() {
        let a = Wat {
            upstream_data: Some(MARK_A_UPSTREAM),
            ..Wat::default()
        };
        let b = Wat {
            upstream_data: Some(MARK_B_UPSTREAM),
            ..Wat::default()
        };
        let tcp = TcpRuntime::new(vec![tcp_plugin("a", a), tcp_plugin("b", b)]);
        let mut connection = tcp.connect(ConnectOptions::default());
        send(&mut connection.server, "s").await;

        let downstream = receive(&mut connection.client, 3).await;

        assert_eq!(downstream, "abs");
    }

    #[tokio::test]
    async fn continue_stream_resumes_paused_direction() {
        struct Case {
            name: &'static str,
            wat: Wat,
            first_from_client: bool,
            first: &'static str,
            second: Option<&'static str>,
            to_server: &'static str,
            to_client: &'static str,
        }
        let cases = [
            Case {
                name: "downstream from upstream callback",
                wat: Wat {
                    downstream_data: Some(PAUSE_AND_LOG),
                    upstream_data: Some(CONTINUE_DOWNSTREAM),
                    ..Wat::default()
                },
                first_from_client: true,
                first: "ping",
                second: Some("go"),
                to_server: "ping",
                to_client: "go",
            },
            Case {
                name: "upstream from downstream callback",
                wat: Wat {
                    downstream_data: Some(CONTINUE_UPSTREAM),
                    upstream_data: Some(PAUSE_AND_LOG),
                    ..Wat::default()
                },
                first_from_client: false,
                first: "pong",
                second: Some("go"),
                to_server: "go",
                to_client: "pong",
            },
            Case {
                name: "inside the pausing callback",
                wat: Wat {
                    downstream_data: Some("(call $continue_and_pause (i32.const 2))"),
                    ..Wat::default()
                },
                first_from_client: true,
                first: "now",
                second: None,
                to_server: "now",
                to_client: "",
            },
        ];

        for case in cases {
            let tcp = TcpRuntime::new(vec![tcp_plugin("a", case.wat)]);
            let mut connection = tcp.connect(ConnectOptions::default());
            let (first, second) = match case.first_from_client {
                true => (&mut connection.client, &mut connection.server),
                false => (&mut connection.server, &mut connection.client),
            };
            send(first, case.first).await;
            if let Some(bytes) = case.second {
                assert!(
                    eventually(|| tcp.logs() == ["paused"]).await,
                    "{}",
                    case.name
                );
                send(second, bytes).await;
            }

            let to_server = receive(&mut connection.server, case.to_server.len()).await;

            let to_client = receive(&mut connection.client, case.to_client.len()).await;
            assert_eq!(
                (to_server.as_str(), to_client.as_str()),
                (case.to_server, case.to_client)
            );
        }
    }

    #[tokio::test]
    async fn continue_stream_has_no_effect_from_other_plugin_or_for_http_stream() {
        let other_plugin = vec![
            tcp_plugin(
                "a",
                Wat {
                    downstream_data: Some(PAUSE_AND_LOG),
                    ..Wat::default()
                },
            ),
            tcp_plugin(
                "b",
                Wat {
                    upstream_data: Some(CONTINUE_DOWNSTREAM),
                    ..Wat::default()
                },
            ),
        ];
        let http_stream = vec![tcp_plugin(
            "a",
            Wat {
                downstream_data: Some("(call $continue (i32.const 0)) (i32.const 1)"),
                ..Wat::default()
            },
        )];

        for plugins in [other_plugin, http_stream] {
            let tcp = TcpRuntime::new(plugins);
            let mut connection = tcp.connect(ConnectOptions::default());
            send(&mut connection.client, "ping").await;
            send(&mut connection.server, "go").await;
            assert_eq!(receive(&mut connection.client, 2).await, "go");

            let silent = stays_silent(&mut connection.server).await;

            assert!(silent);
        }
    }

    #[tokio::test]
    async fn paused_direction_stops_reading_at_limit_while_other_direction_runs() {
        let wat = Wat {
            downstream_data: Some(PAUSE_AND_LOG),
            upstream_data: Some(CONTINUE_DOWNSTREAM),
            ..Wat::default()
        };
        let tcp = TcpRuntime::new(vec![tcp_plugin("a", wat)]);
        let mut options = ConnectOptions::default();
        options.limits.buffer_limit = 4;
        let mut connection = tcp.connect(options);
        send(&mut connection.client, "0123456789").await;
        assert!(eventually(|| tcp.logs() == ["paused"]).await);
        send(&mut connection.client, "abc").await;
        assert!(stays_silent(&mut connection.server).await);
        assert_eq!(tcp.logs(), ["paused"], "no read past the limit");

        send(&mut connection.server, "x").await;

        assert_eq!(receive(&mut connection.client, 1).await, "x");
        assert_eq!(receive(&mut connection.server, 10).await, "0123456789");
        assert!(eventually(|| tcp.logs() == ["paused", "paused"]).await);
    }

    #[tokio::test]
    async fn failure_closes_connection_or_skips_plugin_by_policy() {
        struct Case {
            name: &'static str,
            wat: Wat,
            policy: FailPolicy,
            failure: PluginFailure,
            outcome: PluginFailureOutcome,
            callback: &'static str,
            at_server: &'static str,
        }
        let data = |body| Wat {
            downstream_data: Some(body),
            ..Wat::default()
        };
        let cases = [
            Case {
                name: "trap under fail closed",
                wat: data("unreachable"),
                policy: FailPolicy::Closed,
                failure: PluginFailure::GuestError,
                outcome: PluginFailureOutcome::Failed,
                callback: "proxy_on_downstream_data",
                at_server: "",
            },
            Case {
                name: "trap under fail open",
                wat: data("unreachable"),
                policy: FailPolicy::Open,
                failure: PluginFailure::GuestError,
                outcome: PluginFailureOutcome::Skipped,
                callback: "proxy_on_downstream_data",
                at_server: "data",
            },
            Case {
                name: "trap after changing data under fail open",
                wat: data("(drop (call $mark_a (i32.const 2))) unreachable"),
                policy: FailPolicy::Open,
                failure: PluginFailure::BodyChanged,
                outcome: PluginFailureOutcome::Failed,
                callback: "proxy_on_downstream_data",
                at_server: "",
            },
            Case {
                name: "new connection paused without callout",
                wat: Wat {
                    new_connection: Some("i32.const 1"),
                    ..Wat::default()
                },
                policy: FailPolicy::Closed,
                failure: PluginFailure::PausedWithoutCallout,
                outcome: PluginFailureOutcome::Failed,
                callback: "proxy_on_new_connection",
                at_server: "",
            },
            Case {
                name: "trap in close callback",
                wat: Wat {
                    downstream_close: Some("unreachable"),
                    ..Wat::default()
                },
                policy: FailPolicy::Closed,
                failure: PluginFailure::GuestError,
                outcome: PluginFailureOutcome::Failed,
                callback: "proxy_on_downstream_connection_close",
                at_server: "data",
            },
            Case {
                name: "trap in callout delivery",
                wat: Wat {
                    downstream_data: Some("(call $call_authz_and_pause)"),
                    http_call_response: Some("unreachable"),
                    ..Wat::default()
                },
                policy: FailPolicy::Closed,
                failure: PluginFailure::GuestError,
                outcome: PluginFailureOutcome::Failed,
                callback: "proxy_on_http_call_response",
                at_server: "",
            },
            Case {
                name: "data paused without callout after both directions ended",
                wat: data("i32.const 1"),
                policy: FailPolicy::Closed,
                failure: PluginFailure::PausedWithoutCallout,
                outcome: PluginFailureOutcome::Failed,
                callback: "proxy_on_downstream_data",
                at_server: "",
            },
        ];

        for case in cases {
            let mut conf = tcp_plugin("a", case.wat);
            conf.fail_policy = case.policy;
            let tcp = TcpRuntime::new(vec![conf]);
            let mut connection = tcp.connect(ConnectOptions::default());
            send(&mut connection.client, "data").await;
            connection.client.shutdown().await.unwrap();
            connection.server.shutdown().await.unwrap();

            let at_server = receive_to_end(&mut connection.server).await;

            assert_eq!(at_server, case.at_server, "{}", case.name);
            let callback = Some(case.callback.to_string());
            let want = [("a".to_string(), case.failure, case.outcome, callback)];
            assert_eq!(tcp.failures.failures(), want, "{}", case.name);
        }
    }

    #[tokio::test]
    async fn skipped_stalled_pause_sends_its_bytes() {
        let mut conf = tcp_plugin(
            "a",
            Wat {
                downstream_data: Some("i32.const 1"),
                ..Wat::default()
            },
        );
        conf.fail_policy = FailPolicy::Open;
        let tcp = TcpRuntime::new(vec![conf]);
        let mut connection = tcp.connect(ConnectOptions::default());
        send(&mut connection.client, "data").await;
        connection.server.shutdown().await.unwrap();
        assert_eq!(receive_to_end(&mut connection.client).await, "");
        connection.client.shutdown().await.unwrap();

        let at_server = receive_to_end(&mut connection.server).await;

        assert_eq!(at_server, "data");
    }

    #[tokio::test]
    async fn close_request_of_failed_call_is_dropped() {
        let mut closes_and_traps = tcp_plugin(
            "a",
            Wat {
                downstream_data: Some("(drop (call $close_stream (i32.const 2))) unreachable"),
                ..Wat::default()
            },
        );
        closes_and_traps.fail_policy = FailPolicy::Open;
        let continues = tcp_plugin(
            "b",
            Wat {
                downstream_data: Some("i32.const 0"),
                ..Wat::default()
            },
        );
        let tcp = TcpRuntime::new(vec![closes_and_traps, continues]);
        let mut connection = tcp.connect(ConnectOptions::default());
        send(&mut connection.client, "data").await;
        assert_eq!(receive(&mut connection.server, 4).await, "data");

        let still_open = stays_silent(&mut connection.client).await;

        assert!(still_open);
    }
}
