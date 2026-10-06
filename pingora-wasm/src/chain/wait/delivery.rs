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

//! Callout result delivery
//!
//! A callout result is delivered by running `proxy_on_http_call_response`, or one of the gRPC
//! callbacks, on the paused plugin, with the stream state set up as it was in the callback the
//! plugin paused in.

use crate::callout::CalloutDelivery;
use crate::chain::body::BodyDirection;
use crate::chain::WasmCtx;
use crate::stream_state::{BodyBuffer, ResponseTrailers};
use bytes::Bytes;
use pingora_core::protocols::http::custom::server::Session as DownstreamSession;
use pingora_error::Result;
use pingora_http::ResponseHeader;
use pingora_proxy::Session;
use proxy_wasm_host::abi::v0_2_1::types::StreamType;
use proxy_wasm_host::abi::v0_2_1::{Callback, CalloutId};
use std::mem;

/// The phase a plugin is paused in. The response header and trailer variants hold what their
/// filter was given, since neither can be reached through the session.
pub(in crate::chain) enum PausedPhase<'a> {
    RequestHeaders,
    RequestBody,
    ResponseHeaders(&'a mut ResponseHeader),
    ResponseBody,
    ResponseTrailers(&'a mut http::HeaderMap),
}

impl PausedPhase<'_> {
    pub(super) fn callback(&self) -> Callback {
        match self {
            PausedPhase::RequestHeaders => Callback::RequestHeaders,
            PausedPhase::RequestBody => Callback::RequestBody,
            PausedPhase::ResponseHeaders(_) => Callback::ResponseHeaders,
            PausedPhase::ResponseBody => Callback::ResponseBody,
            PausedPhase::ResponseTrailers(_) => Callback::ResponseTrailers,
        }
    }

    /// Return the stream the plugin has to continue for the phase to resume.
    pub(super) fn direction(&self) -> StreamType {
        match self {
            PausedPhase::RequestHeaders | PausedPhase::RequestBody => StreamType::HttpRequest,
            _ => StreamType::HttpResponse,
        }
    }

    pub(super) fn body_direction(&self) -> Option<BodyDirection> {
        match self {
            PausedPhase::RequestBody => Some(BodyDirection::Request),
            PausedPhase::ResponseBody => Some(BodyDirection::Response),
            _ => None,
        }
    }
}

impl WasmCtx {
    /// Deliver the result of callout `id` to the plugin at `position`.
    ///
    /// Returns `false` if the plugin was skipped.
    pub(super) fn deliver_callout_result<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
        position: usize,
        phase: &mut PausedPhase<'_>,
        id: CalloutId,
        delivery: &CalloutDelivery,
    ) -> Result<bool> {
        let runtime = self.chain.runtime.clone();
        let pool = &runtime.pools[self.chain.plugins[position]];
        let Some(record) = self.records[position] else {
            return Err(self.plugin_error(position, "no context for callout result"));
        };
        let callback = delivery.callback();
        let Some(mut locked) = self.lock_slot_or_skip_plugin(pool, position, &record, callback)?
        else {
            return Ok(false);
        };
        let loaded = locked.loaded()?;
        let delivered = self.with_phase_in_stream(session, position, phase, |ctx| {
            ctx.run_for_context(loaded, record.context, |scope| {
                delivery.deliver(scope, record.context, id)
            })
        });
        match delivered {
            Ok(()) => Ok(true),
            Err(e) => {
                self.guest_call_failed(position, locked, callback, e)?;
                Ok(false)
            }
        }
    }

    fn with_phase_in_stream<DS: DownstreamSession, R>(
        &mut self,
        session: &mut Session<DS>,
        position: usize,
        phase: &mut PausedPhase<'_>,
        guest_call: impl FnOnce(&mut Self) -> R,
    ) -> R {
        // While a result is being delivered, the plugin gets the access it has in the callback
        // it paused in
        self.stream().delivery_callback = Some(phase.callback());
        self.request_in(session.req_header_mut());
        match phase {
            PausedPhase::ResponseHeaders(response) => self.response_in(response),
            PausedPhase::ResponseTrailers(trailers) => {
                self.stream().trailers = Some(ResponseTrailers::new(mem::take(*trailers)));
            }
            _ => {}
        }
        if let Some(direction) = phase.body_direction() {
            let held = self.held.take(direction, position);
            self.stream().body_buffer = BodyBuffer::new(held, Bytes::new());
        }

        let result = guest_call(self);

        if let Some(direction) = phase.body_direction() {
            let buffer = mem::take(&mut self.stream().body_buffer);
            if buffer.was_written_by_guest() {
                self.record_body_change(direction, position);
            }
            self.held.put(direction, position, buffer.into_vec());
        }
        match phase {
            PausedPhase::ResponseHeaders(response) => self.response_out(position, response),
            PausedPhase::ResponseTrailers(trailers) => {
                if let Some(map) = self.stream().trailers.take() {
                    **trailers = map.trailers;
                }
            }
            _ => {}
        }
        self.request_out(position, session.req_header_mut());
        self.stream().delivery_callback = None;
        result
    }
}

#[cfg(test)]
mod tests {

    use crate::callout::GrpcCalloutEvent;
    use crate::test_support::callouts::{
        asks_on_request_headers, callout_ctx, grpc_ctx, FixedSender, CALL_AND_PAUSE,
        CONTINUE_REQUEST, MARK_ASKED_AND_CONTINUE, RELAY_CALLOUT_BODY, TEAPOT_IF_ASKED,
    };
    use crate::test_support::phases::{
        plugin_with_callback_in, run_phase, run_request_headers, Phase, PhaseInputs,
    };
    use crate::test_support::{
        body_chunk, body_plugin, read_downstream, session, Wat, GET, MARK_B_REQUEST, POST,
        REMOVE_LENGTH, TRAP,
    };
    use crate::{RequestOutcome, ERR_PLUGIN_FAILED};
    use bytes::Bytes;

    use http::header::{CONTENT_LENGTH, TRANSFER_ENCODING};
    use pingora_error::ErrorType;

    #[tokio::test]
    async fn next_plugin_sees_request_changed_during_delivery() {
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
    async fn plugin_responds_with_callout_body() {
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
    async fn held_body_moves_to_next_plugin_after_callout() {
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
    async fn plugin_responds_to_request_body_from_delivery() {
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
    async fn plugin_replaces_upstream_response_from_delivery() {
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
    async fn response_from_delivery_after_response_header_fails() {
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
    async fn trap_in_delivery_fails_request_and_replaces_guest() {
        let sender = FixedSender::responds("allowed");
        let plugins = vec![asks_on_request_headers("a", TRAP)];
        let (runtime, mut ctx) = callout_ctx(plugins, sender);
        let (mut session, _client) = session(GET).await;

        let err = ctx.request_filter(&mut session).await.unwrap_err();

        assert_eq!(err.etype(), &ERR_PLUGIN_FAILED);
        assert!(
            err.to_string()
                .contains("proxy_on_http_call_response failed"),
            "{err}"
        );
        assert_eq!(session.req_header().raw_path(), b"/original");
        let slot = runtime.inner.pools[0].lock_slot(0);
        assert!(slot.as_ref().unwrap().guest.is_serving());
    }

    #[tokio::test]
    async fn trap_in_grpc_delivery_fails_request() {
        let wat = Wat {
            request_headers: "(call $grpc_call_and_pause)",
            grpc_receive: Some(TRAP),
            ..Wat::default()
        };
        let answer = GrpcCalloutEvent::Message(Bytes::from_static(b"pong"));
        let sender = FixedSender::grpc(vec![answer], Vec::new(), false);
        let (_runtime, mut ctx, _logs) = grpc_ctx(wat, sender);
        let (mut session, _client) = session(GET).await;

        let error = ctx.request_filter(&mut session).await.unwrap_err();

        assert_eq!(error.etype(), &ERR_PLUGIN_FAILED);
        assert!(
            error.to_string().contains("proxy_on_grpc_receive failed"),
            "{error}"
        );
    }
}
