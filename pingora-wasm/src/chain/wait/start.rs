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

//! Callouts started by a plugin callback

use crate::callout::PendingResult;
use crate::chain::WasmCtx;
use proxy_wasm_host::abi::v0_2_1::types::{Action, StreamType};

impl WasmCtx {
    pub(in crate::chain) fn plugin_stays_paused(
        &mut self,
        action: Action,
        direction: StreamType,
    ) -> bool {
        action == Action::Pause && !self.stream().continue_requested(direction)
    }

    pub(in crate::chain) fn waits_for_callout(&mut self, position: usize) -> bool {
        self.callouts.cover_for_wait(position)
    }

    pub(in crate::chain) fn start_callouts(&mut self, position: usize, paused: bool) {
        let runtime = self.chain.runtime.clone();
        for callout in self.callouts.take_accepted() {
            let id = callout.id;
            match runtime.callout_launcher.spawn(callout) {
                Some(PendingResult::Grpc(stream)) if stream.is_stream() => {
                    self.callouts
                        .add_pending(position, id, PendingResult::Grpc(stream));
                }
                Some(result) if paused => self.callouts.add_pending(position, id, result),
                // The callout of a plugin that is not paused is still sent, and its result is
                // discarded
                _ => {}
            }
        }
        match paused {
            true => self.callouts.keep_stream_events(position),
            false => self.callouts.drop_stream_events(position),
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::test_support::callouts::{
        authz_services, callout_ctx, callout_ctx_with_services, grpc_ctx, FixedSender,
        CALL_AND_LOG_STATUS, CALL_AND_TRAP, CALL_TWICE_AND_PAUSE, CALL_WITHOUT_PAUSE,
        CALL_WITH_NO_RESULT, CONTINUE_REQUEST_AND_PAUSE, CONTINUE_RESPONSE_AND_PAUSE, LOG_RESULT,
        OPEN_STREAM_AND_CONTINUE, RELAY_CALLOUT_BODY, STAY_PAUSED,
    };
    use crate::test_support::phases::{
        plugin_with_callback_in, run_phase, run_request_headers, Phase, PhaseInputs,
    };
    use crate::test_support::{
        body_chunk, body_plugin, eventually, session, RecordedGuestLogs, Wat, GET, PAUSE, POST,
        TEAPOT, TRAP,
    };
    use crate::{RequestOutcome, WasmServices, ERR_PLUGIN_FAILED};

    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use tokio::sync::Notify;

    const CALL_AND_RESPOND: &str =
        "(drop (call $call_authz_and_pause)) (call $respond (i32.const 403))";

    #[tokio::test]
    async fn continue_inside_pausing_callback_resumes_request_headers() {
        let wat = Wat {
            request_headers: CONTINUE_REQUEST_AND_PAUSE,
            ..Wat::default()
        };
        let sender = FixedSender::responds("unused");
        let (_runtime, mut ctx) = callout_ctx(vec![body_plugin("a", wat)], sender);
        let (mut session, _client) = session(GET).await;

        let outcome = ctx.request_filter(&mut session).await;

        assert!(matches!(outcome, Ok(RequestOutcome::Continue)));
    }

    #[tokio::test]
    async fn continue_inside_pausing_callback_resumes_phase() {
        let cases = [
            (Phase::RequestBody, CONTINUE_REQUEST_AND_PAUSE),
            (Phase::ResponseHeaders, CONTINUE_RESPONSE_AND_PAUSE),
            (Phase::ResponseBody, CONTINUE_RESPONSE_AND_PAUSE),
        ];

        for (phase, callback) in cases {
            let sender = FixedSender::responds("unused");
            let plugins = vec![plugin_with_callback_in("a", phase, callback, STAY_PAUSED)];
            let (_runtime, mut ctx) = callout_ctx(plugins, sender);
            let (mut session, _client) = session(POST).await;
            run_request_headers(&mut ctx, &mut session).await;
            let mut inputs = PhaseInputs::new();

            let result = run_phase(&mut ctx, &mut session, phase, &mut inputs).await;

            assert!(result.is_ok(), "{phase:?}");
            assert_eq!(inputs.body, body_chunk("x"), "{phase:?}");
        }
    }

    #[tokio::test]
    async fn callout_is_sent_when_plugin_ahead_in_chain_stops_response() {
        let cases = [TEAPOT, PAUSE, TRAP];

        for stops in cases {
            let sender = FixedSender::responds("stored");
            let plugins = vec![
                body_plugin("first", Wat::response_headers(stops)),
                body_plugin("last", Wat::response_headers(CALL_WITHOUT_PAUSE)),
            ];
            let (_runtime, mut ctx) = callout_ctx(plugins, sender.clone());
            let (mut session, _client) = session(GET).await;
            run_request_headers(&mut ctx, &mut session).await;
            let mut inputs = PhaseInputs::new();

            let result =
                run_phase(&mut ctx, &mut session, Phase::ResponseHeaders, &mut inputs).await;

            assert!(result.is_err(), "{stops}");
            assert!(eventually(|| sender.sent_count() == 1).await, "{stops}");
        }
    }

    #[tokio::test]
    async fn callout_is_sent_when_plugin_responds_in_same_callback() {
        let wat = Wat {
            request_headers: CALL_AND_RESPOND,
            ..Wat::default()
        };
        let sender = FixedSender::responds("stored");
        let (_runtime, mut ctx) = callout_ctx(vec![body_plugin("a", wat)], sender.clone());
        let (mut session, _client) = session(GET).await;

        let outcome = ctx.request_filter(&mut session).await.unwrap();

        assert!(matches!(outcome, RequestOutcome::Respond(..)));
        assert!(eventually(|| sender.sent_count() == 1).await);
    }

    #[tokio::test]
    async fn callout_from_trapped_guest_call_is_not_sent() {
        let wat = Wat {
            request_headers: CALL_AND_TRAP,
            ..Wat::default()
        };
        let sender = FixedSender::responds("allowed");
        let (_runtime, mut ctx) = callout_ctx(vec![body_plugin("a", wat)], sender.clone());
        let (mut session, _client) = session(GET).await;
        let trapped = ctx.request_filter(&mut session).await;

        ctx.logging(&mut session).await;

        assert_eq!(trapped.unwrap_err().etype(), &ERR_PLUGIN_FAILED);
        tokio::task::yield_now().await;
        assert_eq!(sender.sent_count(), 0);
    }

    #[tokio::test]
    async fn callout_in_flight_at_request_end_is_failed() {
        let wat = Wat {
            request_headers: CALL_WITHOUT_PAUSE,
            http_call_response: Some(LOG_RESULT),
            ..Wat::default()
        };
        let sender = FixedSender::responds_after("late", Arc::new(Notify::new()));
        let logs = Arc::new(RecordedGuestLogs::default());
        let services = WasmServices {
            log_sink: logs.clone(),
            ..authz_services()
        };
        let plugins = vec![body_plugin("a", wat)];
        let (runtime, mut ctx) = callout_ctx_with_services(plugins, sender.clone(), services);
        let (mut session, _client) = session(GET).await;
        let outcome = ctx.request_filter(&mut session).await.unwrap();

        ctx.logging(&mut session).await;

        assert!(matches!(outcome, RequestOutcome::Continue));
        assert!(eventually(|| sender.sent_count() == 1).await);
        assert_eq!(logs.0.lock()[..], ["failed".to_string()]);
        assert_eq!(runtime.open_contexts(), 0);
        assert_eq!(runtime.callouts_in_flight(), 1);
    }

    #[tokio::test]
    async fn callout_from_log_callback_is_sent() {
        let wat = Wat {
            log: Some(CALL_WITH_NO_RESULT),
            ..Wat::default()
        };
        let sender = FixedSender::responds("stored");
        let (_runtime, mut ctx) = callout_ctx(vec![body_plugin("a", wat)], sender.clone());
        let (mut session, _client) = session(GET).await;
        run_request_headers(&mut ctx, &mut session).await;

        ctx.logging(&mut session).await;

        assert!(eventually(|| sender.sent_count() == 1).await);
    }

    #[tokio::test]
    async fn callout_over_limit_is_failed_without_being_sent() {
        let gate = Arc::new(Notify::new());
        let sender = FixedSender::responds_after("allowed", gate.clone());
        let wat = Wat {
            request_headers: CALL_TWICE_AND_PAUSE,
            http_call_response: Some(RELAY_CALLOUT_BODY),
            ..Wat::default()
        };
        let services = WasmServices {
            max_callouts_in_flight: 1,
            ..authz_services()
        };
        let plugins = vec![body_plugin("a", wat)];
        let (runtime, mut ctx) = callout_ctx_with_services(plugins, sender.clone(), services);
        let (mut session, _client) = session(GET).await;

        let outcome = ctx.request_filter(&mut session).await.unwrap();

        let RequestOutcome::Respond(header, body) = outcome else {
            panic!("{outcome:?}");
        };
        assert_eq!(header.status, 418);
        assert!(body.ends_with(b"reset reason: overflow"), "{body:?}");
        assert!(eventually(|| sender.sent_count() == 1).await);
        assert_eq!(runtime.callouts_in_flight(), 1);
        gate.notify_one();
        assert!(eventually(|| runtime.callouts_in_flight() == 0).await);
        assert_eq!(sender.sent_count(), 1);
    }

    #[test]
    fn dropping_ctx_outside_tokio_runtime_sends_no_callout() {
        let wat = Wat {
            done: CALL_AND_LOG_STATUS,
            ..Wat::default()
        };
        let sender = FixedSender::responds("stored");
        let logs = Arc::new(RecordedGuestLogs::default());
        let services = WasmServices {
            log_sink: logs.clone(),
            ..authz_services()
        };
        let plugins = vec![body_plugin("a", wat)];
        let (runtime, mut ctx) = callout_ctx_with_services(plugins, sender.clone(), services);
        let tokio_runtime = tokio::runtime::Runtime::new().unwrap();
        let started = tokio_runtime.block_on(async {
            let (mut session, _client) = session(GET).await;
            ctx.request_filter(&mut session).await
        });

        drop(ctx);

        assert!(started.is_ok());
        assert_eq!(logs.0.lock()[..], ["accepted".to_string()]);
        assert_eq!(sender.sent_count(), 0);
        assert_eq!(runtime.open_contexts(), 0);
    }

    #[derive(Debug, Clone, Copy)]
    enum RequestEnd {
        Logging,
        DropOfCtx,
    }

    #[tokio::test]
    async fn request_end_stops_open_stream_but_not_call() {
        for end in [RequestEnd::Logging, RequestEnd::DropOfCtx] {
            let wat = Wat {
                request_headers:
                    "(call $open_grpc_stream) (drop (call $grpc_call_and_pause)) (i32.const 0)",
                ..Wat::default()
            };
            let sender = FixedSender::grpc_released_by(Arc::new(Notify::new()), Vec::new());
            let (_runtime, mut ctx, _logs) = grpc_ctx(wat, sender.clone());
            let (mut session, _client) = session(GET).await;
            ctx.request_filter(&mut session).await.unwrap();
            let running = || {
                let calls = sender.running_calls.load(Ordering::Relaxed);
                (calls, sender.running_streams.load(Ordering::Relaxed))
            };
            assert!(eventually(|| running() == (1, 1)).await, "{end:?}");

            match end {
                RequestEnd::Logging => ctx.logging(&mut session).await,
                RequestEnd::DropOfCtx => drop(ctx),
            }

            assert!(eventually(|| running() == (1, 0)).await, "{end:?}");
        }
    }

    #[tokio::test]
    async fn trap_cancels_open_stream_of_guest() {
        let wat = Wat {
            request_headers: OPEN_STREAM_AND_CONTINUE,
            response_headers: Some(TRAP),
            ..Wat::default()
        };
        let sender = FixedSender::grpc_released_by(Arc::new(Notify::new()), Vec::new());
        let (_runtime, mut ctx, _logs) = grpc_ctx(wat, sender.clone());
        let (mut session, _client) = session(GET).await;
        ctx.request_filter(&mut session).await.unwrap();
        let running_streams = || sender.running_streams.load(Ordering::Relaxed);
        assert!(eventually(|| running_streams() == 1).await);
        let mut inputs = PhaseInputs::new();

        let trapped = run_phase(&mut ctx, &mut session, Phase::ResponseHeaders, &mut inputs).await;

        assert!(trapped.is_err());
        assert!(eventually(|| running_streams() == 0).await);
    }
}
