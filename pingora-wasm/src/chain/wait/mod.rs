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

//! Waiting for callouts
//!
//! A plugin that pauses with a callout in flight keeps its filter waiting. Results are delivered
//! to the plugin as they arrive, until it continues, sends a response, or has no callout left to
//! wait for.

mod delivery;
mod start;

pub(super) use delivery::PausedPhase;

use super::failure::FilterFailure;
use super::WasmCtx;
use crate::callout::CalloutDelivery;
use crate::stream_state::PluginResponse;
use pingora_core::protocols::http::custom::server::Session as DownstreamSession;
use pingora_error::{Error, ErrorType, Result};
use pingora_proxy::Session;
use pingora_timeout::timeout;
use proxy_wasm_host::abi::v0_2_1::CalloutId;
use std::future::{poll_fn, Future};
use std::pin::pin;
use std::task::Poll;
use std::time::{Duration, Instant};

const LONGEST_WAIT_LIMIT: Duration = Duration::from_secs(365 * 24 * 60 * 60);

pub(super) enum CalloutWaitOutcome {
    Continued,
    /// The plugin is still paused and has no callout left to wait for.
    StillPaused,
    Respond(Box<PluginResponse>),
    PluginSkipped,
}

impl WasmCtx {
    pub(super) fn refuse_after_cancelled_wait(&mut self) -> Result<()> {
        // A dropped filter future leaves its plugin paused in that filter, so the later header,
        // body, and trailer filters fail the request
        match self.callouts.waiting_position {
            Some(position) => {
                Err(self.failed_request_error(position, FilterFailure::cancelled_wait()))
            }
            None => Ok(()),
        }
    }

    /// Deliver pending callout results to the plugin at `position` as they arrive.
    pub(super) async fn wait_for_callouts<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
        position: usize,
        mut phase: PausedPhase<'_>,
    ) -> Result<CalloutWaitOutcome> {
        // Stays set if this future is dropped during the wait, which
        // `refuse_after_cancelled_wait` checks
        self.callouts.waiting_position = Some(position);
        let limit = self.pool_at(position).callout_conf.wait_limit;
        // `Instant + Duration` panics on overflow
        let deadline = Instant::now() + limit.min(LONGEST_WAIT_LIMIT);
        let delivered = self
            .deliver_results(session, position, &mut phase, deadline)
            .await;
        self.callouts.waiting_position = None;
        self.callouts.forget_pending(position);
        match delivered? {
            Some(outcome) => Ok(outcome),
            None => {
                let failure = FilterFailure::wait_limit(phase.callback(), limit);
                self.skip_plugin_or_fail_request(position, failure)?;
                Ok(CalloutWaitOutcome::PluginSkipped)
            }
        }
    }

    async fn deliver_results<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
        position: usize,
        phase: &mut PausedPhase<'_>,
        deadline: Instant,
    ) -> Result<Option<CalloutWaitOutcome>> {
        loop {
            // The timer is never polled for a result that is ready immediately, such as that of
            // a callout over the in-flight limit, so the deadline is checked before each wait
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(None);
            }
            let next = self.next_result_or_downstream_close(session, position);
            let Ok(next) = timeout(remaining, next).await else {
                return Ok(None);
            };
            let Some((id, delivery)) = next? else {
                return Ok(Some(CalloutWaitOutcome::StillPaused));
            };
            if !self.deliver_callout_result(session, position, phase, id, &delivery)? {
                return Ok(Some(CalloutWaitOutcome::PluginSkipped));
            }
            let sent = self.stream().plugin_response.take();
            let continued = self.stream().continue_requested(phase.direction());
            let paused = sent.is_none() && !continued;
            self.start_callouts(position, paused);
            if paused {
                self.callouts.cover_for_wait(position);
            }
            if let Some(response) = sent {
                return Ok(Some(CalloutWaitOutcome::Respond(Box::new(response))));
            }
            if continued {
                return Ok(Some(CalloutWaitOutcome::Continued));
            }
            // Yield, so that a plugin that sends a new callout from every delivery cannot keep
            // the thread to itself
            tokio::task::yield_now().await;
        }
    }

    async fn next_result_or_downstream_close<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
        position: usize,
    ) -> Result<Option<(CalloutId, CalloutDelivery)>> {
        let plugin = self.pool_at(position).name.clone();
        let mut result = pin!(self.callouts.next_result(position));
        let Some(stream_close) = session.as_downstream_mut().watch_h2_stream_close() else {
            return Ok(result.await);
        };
        let mut stream_close = pin!(stream_close);
        poll_fn(|cx| {
            if let Poll::Ready(result) = result.as_mut().poll(cx) {
                return Poll::Ready(Ok(result));
            }
            stream_close.as_mut().poll(cx).map(|reason| {
                let error = match reason {
                    Ok(reason) => Error::explain(
                        ErrorType::H2Error,
                        format!("wasm plugin {plugin}: downstream H2 stream closed (reason: {reason}) during a callout wait"),
                    ),
                    Err(e) => e,
                };
                Err(error.into_down())
            })
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use crate::callout::grpc::GrpcCommand;
    use crate::callout::GrpcCalloutEvent;
    use crate::test_support::callouts::{
        asks_on_request_headers, authz_services, callout_ctx, callout_ctx_with_services, grpc_ctx,
        FixedSender, CALL_AND_PAUSE, CALL_TWICE_AND_PAUSE, CALL_WITHOUT_PAUSE, CONTINUE_REQUEST,
        CONTINUE_REQUEST_ON_SECOND_DELIVERY, CONTINUE_RESPONSE, MARK_ASKED_AND_CONTINUE,
        OPEN_STREAM_AND_CONTINUE, RELAY_CALLOUT_BODY, STAY_PAUSED,
    };
    use crate::test_support::phases::{
        cancel_a_wait_in, plugin_with_callback_in, run_phase, run_request_headers, Phase,
        PhaseInputs,
    };
    use crate::test_support::{
        body_chunk, body_plugin, eventually, session, RecordedGuestLogs, Wat, CONTINUE, GET, HOLD,
        MARK_A_REQUEST, MARK_B_RESPONSE, POST, REMOVE_LENGTH, SET_TRAILER,
    };
    use crate::{RequestOutcome, ERR_PLUGIN_FAILED};
    use bytes::Bytes;
    use futures::poll;
    use http::header::CONTENT_LENGTH;

    use std::pin::pin;

    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Notify;
    use tokio::time::timeout;

    const BOTH_DIRECTIONS: &str = "(call $continue (i32.const 0)) (call $continue (i32.const 1))";

    #[tokio::test]
    async fn request_body_runs_after_callout_wait_on_headers() {
        let wat = Wat {
            request_headers: CALL_AND_PAUSE,
            http_call_response: Some(CONTINUE_REQUEST),
            request_body: Some(MARK_A_REQUEST),
            ..Wat::default()
        };
        let sender = FixedSender::responds("allowed");
        let (_runtime, mut ctx) = callout_ctx(vec![body_plugin("a", wat)], sender);
        let (mut session, _client) = session(POST).await;
        run_request_headers(&mut ctx, &mut session).await;
        let mut inputs = PhaseInputs::new();

        let result = run_phase(&mut ctx, &mut session, Phase::RequestBody, &mut inputs).await;

        assert!(result.is_ok());
        assert_eq!(inputs.body, body_chunk("ax"));
    }

    #[tokio::test]
    async fn staying_paused_after_callout_fails_request() {
        let sender = FixedSender::responds("allowed");
        let cases = [STAY_PAUSED, CONTINUE_RESPONSE];

        for delivery in cases {
            let plugins = vec![asks_on_request_headers("a", delivery)];
            let (_runtime, mut ctx) = callout_ctx(plugins, sender.clone());
            let (mut session, _client) = session(GET).await;

            let err = ctx.request_filter(&mut session).await.unwrap_err();

            assert_eq!(err.etype(), &ERR_PLUGIN_FAILED, "{delivery}");
            assert!(
                err.to_string().contains("paused on request headers"),
                "{err}"
            );
        }
        assert_eq!(sender.sent_count(), 2);
    }

    #[tokio::test]
    async fn continuing_both_streams_resumes_request() {
        let sender = FixedSender::responds("allowed");
        let plugins = vec![asks_on_request_headers("a", BOTH_DIRECTIONS)];
        let (_runtime, mut ctx) = callout_ctx(plugins, sender);
        let (mut session, _client) = session(GET).await;

        let outcome = ctx.request_filter(&mut session).await;

        assert!(
            matches!(outcome, Ok(RequestOutcome::Continue)),
            "{outcome:?}"
        );
    }

    #[tokio::test]
    async fn plugin_continues_after_second_of_two_callouts() {
        let wat = Wat {
            request_headers: CALL_TWICE_AND_PAUSE,
            http_call_response: Some(CONTINUE_REQUEST_ON_SECOND_DELIVERY),
            ..Wat::default()
        };
        let sender = FixedSender::responds("allowed");
        let (_runtime, mut ctx) = callout_ctx(vec![body_plugin("a", wat)], sender.clone());
        let (mut session, _client) = session(GET).await;

        let outcome = ctx.request_filter(&mut session).await.unwrap();

        assert!(matches!(outcome, RequestOutcome::Continue));
        assert_eq!(sender.sent_count(), 2);
    }

    #[tokio::test]
    async fn each_plugin_receives_its_own_callout_result() {
        let sender = FixedSender::responds("allowed");
        let plugins = vec![
            asks_on_request_headers("a", CONTINUE_REQUEST),
            asks_on_request_headers("b", MARK_ASKED_AND_CONTINUE),
        ];
        let (_runtime, mut ctx) = callout_ctx(plugins, sender.clone());
        let (mut session, _client) = session(GET).await;

        let outcome = ctx.request_filter(&mut session).await.unwrap();

        assert!(matches!(outcome, RequestOutcome::Continue));
        assert_eq!(session.req_header().headers["x-asked"], "yes");
        assert_eq!(sender.sent_count(), 2);
    }

    #[tokio::test]
    async fn slot_is_unlocked_during_callout_wait() {
        let sender = FixedSender::responds_after("allowed", Arc::new(Notify::new()));
        let plugins = vec![asks_on_request_headers("a", CONTINUE_REQUEST)];
        let (runtime, mut ctx) = callout_ctx(plugins, sender);
        let (mut session, _client) = session(GET).await;
        let mut request = pin!(ctx.request_filter(&mut session));

        let waiting = poll!(request.as_mut());

        assert!(waiting.is_pending());
        assert!(runtime.inner.pools[0].is_slot_free(0));
    }

    #[tokio::test]
    async fn staying_paused_after_callout_holds_body() {
        let phase = Phase::RequestBody;
        let plugins = vec![plugin_with_callback_in(
            "a",
            phase,
            CALL_AND_PAUSE,
            STAY_PAUSED,
        )];
        let (_runtime, mut ctx) = callout_ctx(plugins, FixedSender::responds("allowed"));
        let (mut session, _client) = session(POST).await;
        run_request_headers(&mut ctx, &mut session).await;
        let mut first = body_chunk("x");

        let result = ctx
            .request_body_filter(&mut session, &mut first, false)
            .await;

        assert!(result.is_ok());
        assert_eq!(first, Some(Bytes::new()));
    }

    #[tokio::test]
    async fn staying_paused_on_last_chunk_fails_request() {
        let phase = Phase::RequestBody;
        let plugins = vec![plugin_with_callback_in(
            "a",
            phase,
            CALL_AND_PAUSE,
            STAY_PAUSED,
        )];
        let (_runtime, mut ctx) = callout_ctx(plugins, FixedSender::responds("allowed"));
        let (mut session, _client) = session(POST).await;
        run_request_headers(&mut ctx, &mut session).await;
        let mut inputs = PhaseInputs::new();

        let err = run_phase(&mut ctx, &mut session, phase, &mut inputs)
            .await
            .unwrap_err();

        assert_eq!(err.etype(), &ERR_PLUGIN_FAILED);
        assert!(err.to_string().contains("paused on the last body chunk"));
    }

    #[tokio::test]
    async fn body_hold_does_not_wait_for_earlier_callout() {
        let wat = Wat {
            request_headers: CALL_WITHOUT_PAUSE,
            request_body: Some(HOLD),
            ..Wat::default()
        };
        let sender = FixedSender::responds_after("late", Arc::new(Notify::new()));
        let (_runtime, mut ctx) = callout_ctx(vec![body_plugin("a", wat)], sender);
        let (mut session, _client) = session(POST).await;
        run_request_headers(&mut ctx, &mut session).await;
        let mut body = body_chunk("x");

        let result = ctx
            .request_body_filter(&mut session, &mut body, false)
            .await;

        assert!(result.is_ok());
        assert_eq!(body, Some(Bytes::new()));
    }

    #[tokio::test]
    async fn remaining_plugins_run_after_callout_wait() {
        let cases = [
            (Phase::ResponseHeaders, Wat::response_headers(REMOVE_LENGTH)),
            (Phase::ResponseBody, Wat::response_body(MARK_B_RESPONSE)),
            (Phase::ResponseTrailers, Wat::response_trailers(SET_TRAILER)),
        ];

        for (phase, runs_after) in cases {
            let sender = FixedSender::responds("allowed");
            let plugins = vec![
                body_plugin("first", runs_after),
                plugin_with_callback_in("last", phase, CALL_AND_PAUSE, CONTINUE_RESPONSE),
            ];
            let (_runtime, mut ctx) = callout_ctx(plugins, sender.clone());
            let (mut session, _client) = session(GET).await;
            run_request_headers(&mut ctx, &mut session).await;
            let mut inputs = PhaseInputs::new();

            let result = run_phase(&mut ctx, &mut session, phase, &mut inputs).await;

            assert!(result.is_ok(), "{phase:?}");
            assert_eq!(sender.sent_count(), 1, "{phase:?}");
            let changed = match phase {
                Phase::ResponseHeaders => !inputs.response.headers.contains_key(CONTENT_LENGTH),
                Phase::ResponseBody => inputs.body == body_chunk("bx"),
                _ => inputs.trailers["x-trailer"] == "set",
            };
            assert!(changed, "{phase:?}");
        }
    }

    #[tokio::test]
    async fn staying_paused_after_callout_fails_trailers() {
        let phase = Phase::ResponseTrailers;
        let plugins = vec![plugin_with_callback_in(
            "a",
            phase,
            CALL_AND_PAUSE,
            STAY_PAUSED,
        )];
        let (_runtime, mut ctx) = callout_ctx(plugins, FixedSender::responds("allowed"));
        let (mut session, _client) = session(GET).await;
        run_request_headers(&mut ctx, &mut session).await;
        let mut inputs = PhaseInputs::new();

        let err = run_phase(&mut ctx, &mut session, phase, &mut inputs)
            .await
            .unwrap_err();

        assert!(
            err.to_string().contains("paused on response trailers"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn panicking_callout_task_delivers_failure() {
        let plugins = vec![asks_on_request_headers("a", RELAY_CALLOUT_BODY)];
        let (_runtime, mut ctx) = callout_ctx(plugins, FixedSender::panics());
        let (mut session, _client) = session(GET).await;

        let outcome = ctx.request_filter(&mut session).await.unwrap();

        let RequestOutcome::Respond(header, _) = outcome else {
            panic!("{outcome:?}");
        };
        assert_eq!(header.status, 500);
    }

    #[tokio::test]
    async fn phase_after_cancelled_wait_fails() {
        let waits_on_the_request_body = Wat {
            request_body: Some(CALL_AND_PAUSE),
            response_headers: Some(CONTINUE),
            response_body: Some(CONTINUE),
            response_trailers: Some(CONTINUE),
            ..Wat::default()
        };
        let cases = [
            Phase::RequestBody,
            Phase::ResponseHeaders,
            Phase::ResponseBody,
            Phase::ResponseTrailers,
        ];

        for phase in cases {
            let sender = FixedSender::responds_after("late", Arc::new(Notify::new()));
            let plugins = vec![body_plugin("a", waits_on_the_request_body)];
            let (_runtime, mut ctx) = callout_ctx(plugins, sender);
            let (mut session, _client) = session(POST).await;
            cancel_a_wait_in(Phase::RequestBody, &mut ctx, &mut session).await;
            ctx.upstream_attempt();
            let mut inputs = PhaseInputs::new();

            let err = run_phase(&mut ctx, &mut session, phase, &mut inputs)
                .await
                .unwrap_err();

            assert_eq!(err.etype(), &ERR_PLUGIN_FAILED, "{phase:?}");
            assert!(err.to_string().contains("was cancelled"), "{phase:?} {err}");
        }
    }

    #[tokio::test]
    async fn logging_ends_context_after_cancelled_wait() {
        let phase = Phase::RequestBody;
        let sender = FixedSender::responds_after("late", Arc::new(Notify::new()));
        let plugins = vec![plugin_with_callback_in(
            "a",
            phase,
            CALL_AND_PAUSE,
            CONTINUE_REQUEST,
        )];
        let (runtime, mut ctx) = callout_ctx(plugins, sender);
        let (mut session, _client) = session(POST).await;
        cancel_a_wait_in(phase, &mut ctx, &mut session).await;

        ctx.logging(&mut session).await;

        assert_eq!(runtime.open_contexts(), 0);
    }

    #[tokio::test]
    async fn dropping_ctx_after_cancelled_wait_ends_context() {
        let phase = Phase::RequestBody;
        let sender = FixedSender::responds_after("late", Arc::new(Notify::new()));
        let plugins = vec![plugin_with_callback_in(
            "a",
            phase,
            CALL_AND_PAUSE,
            CONTINUE_REQUEST,
        )];
        let (runtime, mut ctx) = callout_ctx(plugins, sender);
        let (mut session, _client) = session(POST).await;
        cancel_a_wait_in(phase, &mut ctx, &mut session).await;

        drop(ctx);

        assert_eq!(runtime.open_contexts(), 0);
    }

    #[tokio::test]
    async fn stream_event_reaches_plugin_only_while_paused() {
        let wat = Wat {
            request_headers: OPEN_STREAM_AND_CONTINUE,
            response_headers: Some("(call $grpc_send_and_pause)"),
            grpc_receive: Some(
                "(call $log_grpc_message (local.get 2)) (call $continue (i32.const 1))",
            ),
            ..Wat::default()
        };
        let early = GrpcCalloutEvent::Message(Bytes::from_static(b"early"));
        let sender = FixedSender::grpc(Vec::new(), vec![early], true);
        let (_runtime, mut ctx, logs) = grpc_ctx(wat, sender.clone());
        let (mut session, _client) = session(GET).await;
        ctx.request_filter(&mut session).await.unwrap();
        assert!(eventually(|| sender.sent_count() == 1).await);
        let mut inputs = PhaseInputs::new();

        let continued =
            run_phase(&mut ctx, &mut session, Phase::ResponseHeaders, &mut inputs).await;

        assert!(continued.is_ok());
        assert_eq!(logs.0.lock()[..], ["ping".to_string()]);
        let ping = GrpcCommand::Send {
            message: Bytes::from_static(b"ping"),
            end_of_stream: false,
        };
        assert_eq!(sender.grpc_commands.lock()[..], [ping]);
    }

    #[tokio::test]
    async fn idle_stream_does_not_keep_paused_plugin_waiting() {
        let cases = [
            ("pause with no callout", "i32.const 1", None),
            (
                "delivery that stays paused",
                "(call $call_authz_and_pause)",
                Some(""),
            ),
        ];
        for (case, request_body, http_call_response) in cases {
            let wat = Wat {
                request_headers: OPEN_STREAM_AND_CONTINUE,
                request_body: Some(request_body),
                http_call_response,
                ..Wat::default()
            };
            let (_runtime, mut ctx, _logs) = grpc_ctx(wat, FixedSender::responds("ok"));
            let (mut session, _client) = session(POST).await;
            run_request_headers(&mut ctx, &mut session).await;
            let mut inputs = PhaseInputs::new();
            let body = run_phase(&mut ctx, &mut session, Phase::RequestBody, &mut inputs);

            let failed = timeout(Duration::from_secs(5), body).await;

            let error = failed.expect(case).unwrap_err().to_string();
            let want = "paused on the last body chunk with no callout to wait for";
            assert!(error.contains(want), "{case}: {error}");
        }
    }

    #[tokio::test]
    async fn overflowing_stream_of_paused_plugin_gets_its_close() {
        let wat = Wat {
            request_headers: "(call $open_grpc_stream) (call $open_grpc_stream) (i32.const 1)",
            grpc_close: Some(
                "(drop (call $log (i32.const 2) (i32.const 340) (i32.const 6)))
                (call $continue (i32.const 0))",
            ),
            ..Wat::default()
        };
        let sender = FixedSender::grpc_released_by(Arc::new(Notify::new()), Vec::new());
        let logs = Arc::new(RecordedGuestLogs::default());
        let mut services = authz_services();
        services.log_sink = logs.clone();
        services.max_callouts_in_flight = 1;
        let plugins = vec![body_plugin("a", wat)];
        let (runtime, mut ctx) = callout_ctx_with_services(plugins, sender, services);
        let (mut session, _client) = session(GET).await;

        let outcome = timeout(Duration::from_secs(5), ctx.request_filter(&mut session)).await;

        assert!(
            matches!(outcome, Ok(Ok(RequestOutcome::Continue))),
            "{outcome:?}"
        );
        assert_eq!(logs.0.lock()[..], ["closed".to_string()]);
        ctx.logging(&mut session).await;
        assert!(eventually(|| runtime.callouts_in_flight() == 0).await);
    }
}
