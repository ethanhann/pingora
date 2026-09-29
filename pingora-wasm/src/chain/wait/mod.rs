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

//! How a phase waits for the callouts of a paused plugin.

mod delivery;

pub(super) use delivery::PausedPhase;

use super::WasmCtx;
use crate::callout::CalloutResult;
use crate::stream::PluginResponse;
use crate::ERR_PLUGIN_FAILED;
use pingora_core::protocols::http::custom::server::Session as DownstreamSession;
use pingora_error::{Error, ErrorType, Result};
use pingora_proxy::Session;
use proxy_wasm_host::abi::v0_2_1::types::{Action, StreamType};
use proxy_wasm_host::abi::v0_2_1::CalloutId;
use std::future::{poll_fn, Future};
use std::pin::pin;
use std::task::Poll;

/// The result of [WasmCtx::wait_for_callouts].
pub(super) enum CalloutWaitOutcome {
    /// The plugin asked to continue.
    Continued,
    /// The plugin has no pending callout and did not ask to continue.
    StillPaused,
    /// The plugin sent its own response.
    Respond(Box<PluginResponse>),
}

impl WasmCtx {
    /// Return an error when the future of an earlier phase of this request was dropped while
    /// a plugin waited for a callout.
    pub(super) fn refuse_after_cancelled_wait(&self) -> Result<()> {
        if self.callouts.in_callout_wait {
            return Error::e_explain(
                ERR_PLUGIN_FAILED,
                "the request cannot continue, because a phase was cancelled while a wasm plugin waited for a callout",
            );
        }
        Ok(())
    }

    /// Return `true` when the plugin that returned `action` from the last guest call stays
    /// paused in `direction`.
    pub(super) fn plugin_stays_paused(&mut self, action: Action, direction: StreamType) -> bool {
        action == Action::Pause && !self.stream().continue_requested(direction)
    }

    /// Return `true` when the plugin at `position` has a callout to wait for.
    pub(super) fn waits_for_callout(&self, position: usize) -> bool {
        self.callouts.has_pending(position)
    }

    /// Start the callouts that the plugin at `position` sent during the last guest call.
    ///
    /// When `paused` is `true`, keep the tasks so that the phase can wait for their results.
    /// Otherwise the tasks run on, and their results are dropped.
    pub(super) fn start_callouts(&mut self, position: usize, paused: bool) {
        let runtime = self.chain.runtime.clone();
        for callout in self.callouts.take_accepted() {
            let id = callout.id;
            match runtime.callout_launcher.spawn(callout) {
                Some(result) if paused => self.callouts.add_pending(position, id, result),
                _ => {}
            }
        }
    }

    /// Deliver the results of the pending callouts of the plugin at `position` as they
    /// arrive, until the plugin asks to continue, sends a response, or has no pending callout.
    pub(super) async fn wait_for_callouts<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
        position: usize,
        mut phase: PausedPhase<'_>,
    ) -> Result<CalloutWaitOutcome> {
        self.callouts.in_callout_wait = true;
        let outcome = self.deliver_results(session, position, &mut phase).await;
        self.callouts.in_callout_wait = false;
        self.callouts.forget_pending(position);
        outcome
    }

    async fn deliver_results<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
        position: usize,
        phase: &mut PausedPhase<'_>,
    ) -> Result<CalloutWaitOutcome> {
        loop {
            let next = self.next_result_or_downstream_close(session, position);
            let Some((id, result)) = next.await? else {
                return Ok(CalloutWaitOutcome::StillPaused);
            };
            self.deliver_callout_result(session, position, phase, id, &result)?;
            let sent = self.stream().plugin_response.take();
            let continued = self.stream().continue_requested(phase.direction());
            let paused = sent.is_none() && !continued;
            self.start_callouts(position, paused);
            if let Some(response) = sent {
                return Ok(CalloutWaitOutcome::Respond(Box::new(response)));
            }
            if continued {
                return Ok(CalloutWaitOutcome::Continued);
            }
        }
    }

    /// Wait for the next callout result of the plugin at `position`.
    ///
    /// On an HTTP/2 downstream, return an error when the client closes the stream first.
    async fn next_result_or_downstream_close<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
        position: usize,
    ) -> Result<Option<(CalloutId, CalloutResult)>> {
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
                        format!("downstream H2 stream closed (reason: {reason}) while a wasm plugin waited for a callout"),
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
    use crate::test_support::callouts::{
        authz_services, callout_ctx, callout_ctx_with_services, FixedSender, CALL_AND_LOG_STATUS,
        CALL_AND_PAUSE, CALL_AND_TRAP, CALL_TWICE_AND_PAUSE, CALL_WITHOUT_PAUSE,
        CALL_WITH_NO_RESULT, CONTINUE_REQUEST, CONTINUE_REQUEST_AND_PAUSE,
        CONTINUE_REQUEST_ON_SECOND_DELIVERY, CONTINUE_RESPONSE, CONTINUE_RESPONSE_AND_PAUSE,
        LOG_RESULT, MARK_ASKED_AND_CONTINUE, RELAY_CALLOUT_BODY, STAY_PAUSED, TEAPOT_IF_ASKED,
    };
    use crate::test_support::{
        body_chunk, body_plugin, eventually, read_downstream, session, RecordedGuestLogs, Wat,
        CONTINUE, GET, HOLD, MARK_A_REQUEST, MARK_B_REQUEST, MARK_B_RESPONSE, PAUSE, POST,
        REMOVE_LENGTH, SET_TRAILER, TEAPOT, TRAP,
    };
    use crate::{RequestOutcome, WasmCtx, WasmPluginConf, WasmServices, ERR_PLUGIN_FAILED};
    use bytes::Bytes;
    use futures::poll;
    use http::header::{CONTENT_LENGTH, TRANSFER_ENCODING};
    use pingora_error::{ErrorType, Result};
    use pingora_http::ResponseHeader;
    use pingora_proxy::Session;
    use std::pin::pin;
    use std::sync::Arc;
    use tokio::sync::Notify;

    const BOTH_DIRECTIONS: &str = "(call $continue (i32.const 0)) (call $continue (i32.const 1))";
    const CALL_AND_RESPOND: &str =
        "(drop (call $call_authz_and_pause)) (call $respond (i32.const 403))";

    /// A phase after the request headers, which a test runs on one chunk or one header.
    #[derive(Debug, Clone, Copy)]
    enum Phase {
        RequestBody,
        ResponseHeaders,
        ResponseBody,
        ResponseTrailers,
    }

    /// The response header, the body chunk, and the trailers that a phase runs on.
    struct PhaseInputs {
        response: ResponseHeader,
        body: Option<Bytes>,
        trailers: http::HeaderMap,
    }

    impl PhaseInputs {
        fn new() -> Self {
            let mut response = ResponseHeader::build(200, None).unwrap();
            response.insert_header(CONTENT_LENGTH, 1).unwrap();
            PhaseInputs {
                response,
                body: body_chunk("x"),
                trailers: http::HeaderMap::new(),
            }
        }
    }

    /// Run the request headers and the first upstream attempt of a new request.
    async fn run_request_headers(ctx: &mut WasmCtx, session: &mut Session) {
        ctx.request_filter(session).await.unwrap();
        ctx.upstream_attempt();
    }

    /// Run `phase` on the last chunk of a body, or on the headers or the trailers.
    async fn run_phase(
        ctx: &mut WasmCtx,
        session: &mut Session,
        phase: Phase,
        inputs: &mut PhaseInputs,
    ) -> Result<()> {
        let PhaseInputs {
            response,
            body,
            trailers,
        } = inputs;
        match phase {
            Phase::RequestBody => ctx.request_body_filter(session, body, true).await,
            Phase::ResponseHeaders => ctx.response_filter(session, response).await,
            Phase::ResponseBody => ctx.response_body_filter(session, body, true).await,
            Phase::ResponseTrailers => ctx
                .response_trailer_filter(session, trailers)
                .await
                .map(|_| ()),
        }
    }

    /// Return a plugin whose `phase` has the callback `callback`, with `delivery` as its
    /// `proxy_on_http_call_response`.
    fn plugin_with_callback_in(
        name: &str,
        phase: Phase,
        callback: &'static str,
        delivery: &'static str,
    ) -> WasmPluginConf {
        let mut wat = Wat {
            http_call_response: Some(delivery),
            ..Wat::default()
        };
        match phase {
            Phase::RequestBody => wat.request_body = Some(callback),
            Phase::ResponseHeaders => wat.response_headers = Some(callback),
            Phase::ResponseBody => wat.response_body = Some(callback),
            Phase::ResponseTrailers => wat.response_trailers = Some(callback),
        }
        body_plugin(name, wat)
    }

    /// Return a plugin that makes a callout from its request headers, with `delivery` as its
    /// `proxy_on_http_call_response`.
    fn asks_on_request_headers(name: &str, delivery: &'static str) -> WasmPluginConf {
        let wat = Wat {
            request_headers: CALL_AND_PAUSE,
            http_call_response: Some(delivery),
            ..Wat::default()
        };
        body_plugin(name, wat)
    }

    #[tokio::test]
    async fn a_plugin_changes_the_request_in_a_delivery_before_the_next_plugin_runs() {
        let sender = FixedSender::responds("allowed");
        let reads_the_header = Wat {
            request_headers: TEAPOT_IF_ASKED,
            ..Wat::default()
        };
        let plugins = vec![
            asks_on_request_headers("a", MARK_ASKED_AND_CONTINUE),
            body_plugin("b", reads_the_header),
        ];
        let (_runtime, mut ctx) = callout_ctx(plugins, sender.clone());
        let (mut session, _client) = session(GET).await;

        let outcome = ctx.request_filter(&mut session).await.unwrap();

        let RequestOutcome::Respond(header, _) = outcome else {
            panic!("{outcome:?}");
        };
        assert_eq!(header.status, 418);
        assert_eq!(session.req_header().headers["x-asked"], "yes");
        let sent = sender.sent.lock();
        assert_eq!(sent[..], [("authz".to_string(), "/check".to_string())]);
    }

    #[tokio::test]
    async fn a_plugin_receives_the_request_body_after_it_waited_on_the_headers() {
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
    async fn a_plugin_responds_with_the_body_of_its_callout() {
        let sender = FixedSender::responds("denied by authz");
        let plugins = vec![
            body_plugin("first", Wat::response_headers(REMOVE_LENGTH)),
            asks_on_request_headers("last", RELAY_CALLOUT_BODY),
        ];
        let (_runtime, mut ctx) = callout_ctx(plugins, sender);
        let (mut session, _client) = session(GET).await;

        let outcome = ctx.request_filter(&mut session).await.unwrap();

        let RequestOutcome::Respond(header, body) = outcome else {
            panic!("{outcome:?}");
        };
        assert_eq!(header.status, 418);
        assert_eq!(&body[..], b"denied by authz");
        assert!(!header.headers.contains_key(CONTENT_LENGTH));
        assert_eq!(header.headers[TRANSFER_ENCODING], "chunked");
        assert!(ctx.plugin_responded());
    }

    #[tokio::test]
    async fn a_plugin_that_stays_paused_after_its_callout_fails_the_request() {
        let sender = FixedSender::responds("allowed");
        let cases = [STAY_PAUSED, CONTINUE_RESPONSE];

        for delivery in cases {
            let plugins = vec![asks_on_request_headers("a", delivery)];
            let (_runtime, mut ctx) = callout_ctx(plugins, sender.clone());
            let (mut session, _client) = session(GET).await;

            let err = ctx.request_filter(&mut session).await.unwrap_err();

            assert_eq!(err.etype(), &ERR_PLUGIN_FAILED, "{delivery}");
            assert!(err.to_string().contains("paused a request"), "{err}");
        }
        assert_eq!(sender.sent_count(), 2);
    }

    #[tokio::test]
    async fn a_plugin_that_continues_both_directions_continues_the_request() {
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
    async fn a_continue_in_the_callback_that_pauses_continues_the_request_headers() {
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
    async fn a_continue_in_the_callback_that_pauses_continues_the_phase() {
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
    async fn a_plugin_continues_after_the_last_of_two_callouts() {
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
    async fn each_plugin_of_a_chain_receives_the_result_of_its_own_callout() {
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
    async fn the_slot_is_free_while_a_plugin_waits() {
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
    async fn a_callout_is_sent_when_a_later_plugin_stops_the_response() {
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
    async fn a_callout_is_sent_when_its_plugin_responds_in_the_same_callback() {
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
    async fn a_callout_of_a_guest_call_that_trapped_is_not_sent() {
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
    async fn a_plugin_releases_its_body_to_the_next_plugin_after_its_callout() {
        let phase = Phase::RequestBody;
        let plugins = vec![
            plugin_with_callback_in("a", phase, CALL_AND_PAUSE, CONTINUE_REQUEST),
            body_plugin("b", Wat::request_body(MARK_B_REQUEST)),
        ];
        let (_runtime, mut ctx) = callout_ctx(plugins, FixedSender::responds("allowed"));
        let (mut session, _client) = session(POST).await;
        run_request_headers(&mut ctx, &mut session).await;
        let mut inputs = PhaseInputs::new();

        let result = run_phase(&mut ctx, &mut session, phase, &mut inputs).await;

        assert!(result.is_ok());
        assert_eq!(inputs.body, body_chunk("bx"));
    }

    #[tokio::test]
    async fn a_plugin_that_stays_paused_after_its_callout_holds_its_body() {
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
    async fn a_plugin_that_stays_paused_at_the_end_of_a_body_fails_the_request() {
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
        assert!(err.to_string().contains("paused a body at its end"));
    }

    #[tokio::test]
    async fn a_plugin_that_holds_a_body_does_not_wait_for_an_earlier_callout() {
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
    async fn a_plugin_responds_to_a_request_body_from_a_delivery() {
        let phase = Phase::RequestBody;
        let plugins = vec![plugin_with_callback_in(
            "a",
            phase,
            CALL_AND_PAUSE,
            RELAY_CALLOUT_BODY,
        )];
        let (_runtime, mut ctx) = callout_ctx(plugins, FixedSender::responds("denied"));
        let (mut session, mut client) = session(POST).await;
        run_request_headers(&mut ctx, &mut session).await;
        let mut inputs = PhaseInputs::new();

        let err = run_phase(&mut ctx, &mut session, phase, &mut inputs)
            .await
            .unwrap_err();

        assert_eq!(err.etype(), &ErrorType::HTTPStatus(418));
        assert!(ctx.plugin_responded());
        let written = read_downstream(&mut client).await;
        assert!(written.starts_with("HTTP/1.1 418"), "{written}");
        assert!(written.ends_with("denied"), "{written}");
    }

    #[tokio::test]
    async fn the_plugins_after_a_plugin_that_waited_run_on_the_response() {
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
    async fn a_plugin_that_stays_paused_after_its_callout_fails_the_trailers() {
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

        assert!(err.to_string().contains("paused the trailers"), "{err}");
    }

    #[tokio::test]
    async fn a_plugin_responds_in_place_of_the_upstream_response_from_a_delivery() {
        let phase = Phase::ResponseHeaders;
        let plugins = vec![plugin_with_callback_in(
            "a",
            phase,
            CALL_AND_PAUSE,
            RELAY_CALLOUT_BODY,
        )];
        let (_runtime, mut ctx) = callout_ctx(plugins, FixedSender::responds("replaced"));
        let (mut session, mut client) = session(GET).await;
        run_request_headers(&mut ctx, &mut session).await;
        let mut inputs = PhaseInputs::new();

        let err = run_phase(&mut ctx, &mut session, phase, &mut inputs)
            .await
            .unwrap_err();

        assert_eq!(err.etype(), &ErrorType::HTTPStatus(418));
        let written = read_downstream(&mut client).await;
        assert!(written.ends_with("replaced"), "{written}");
    }

    #[tokio::test]
    async fn a_response_from_a_delivery_after_the_response_header_is_an_error() {
        let phase = Phase::ResponseBody;
        let plugins = vec![plugin_with_callback_in(
            "a",
            phase,
            CALL_AND_PAUSE,
            RELAY_CALLOUT_BODY,
        )];
        let (_runtime, mut ctx) = callout_ctx(plugins, FixedSender::responds("late"));
        let (mut session, _client) = session(GET).await;
        run_request_headers(&mut ctx, &mut session).await;
        let mut inputs = PhaseInputs::new();

        let err = run_phase(&mut ctx, &mut session, phase, &mut inputs)
            .await
            .unwrap_err();

        assert_eq!(err.etype(), &ERR_PLUGIN_FAILED);
        assert!(err.to_string().contains("after the response header"));
    }

    #[tokio::test]
    async fn a_plugin_that_continues_with_a_callout_receives_a_failure_at_the_end() {
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
    async fn a_callout_from_the_log_callback_is_sent() {
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
    async fn a_trap_in_a_delivery_fails_the_request_and_replaces_the_guest() {
        let sender = FixedSender::responds("allowed");
        let plugins = vec![asks_on_request_headers("a", TRAP)];
        let (runtime, mut ctx) = callout_ctx(plugins, sender);
        let (mut session, _client) = session(GET).await;

        let err = ctx.request_filter(&mut session).await.unwrap_err();

        assert_eq!(err.etype(), &ERR_PLUGIN_FAILED);
        assert!(err.to_string().contains("on_http_call_response"), "{err}");
        assert_eq!(session.req_header().raw_path(), b"/original");
        let slot = runtime.inner.pools[0].lock_slot(0);
        assert!(slot.as_ref().unwrap().guest.is_serving());
    }

    #[tokio::test]
    async fn a_callout_task_that_panics_gives_the_plugin_a_failure() {
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
    async fn a_callout_over_the_limit_receives_a_response_and_is_not_sent() {
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

    /// Start a request whose plugin waits in `phase`, and drop the future of that phase.
    async fn cancel_a_wait_in(phase: Phase, ctx: &mut WasmCtx, session: &mut Session) {
        run_request_headers(ctx, session).await;
        let mut inputs = PhaseInputs::new();
        let mut waits = pin!(run_phase(ctx, session, phase, &mut inputs));
        assert!(poll!(waits.as_mut()).is_pending());
    }

    #[tokio::test]
    async fn a_phase_after_a_cancelled_wait_fails() {
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
    async fn logging_ends_the_context_after_a_cancelled_wait() {
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
    async fn dropping_a_ctx_after_a_cancelled_wait_ends_its_context() {
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

    #[test]
    fn dropping_a_ctx_outside_of_a_tokio_runtime_sends_no_callout() {
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
}
