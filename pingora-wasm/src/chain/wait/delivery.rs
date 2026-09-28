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

//! The delivery of a callout result to the plugin that waits for it.

use crate::callout::CalloutResult;
use crate::chain::body::BodyDirection;
use crate::chain::slot::LockedSlot;
use crate::chain::WasmCtx;
use crate::stream::{BodyBuffer, ResponseTrailers};
use bytes::Bytes;
use pingora_core::protocols::http::custom::server::Session as DownstreamSession;
use pingora_error::Result;
use pingora_http::ResponseHeader;
use pingora_proxy::Session;
use proxy_wasm_host::abi::v0_2_1::types::StreamType;
use proxy_wasm_host::abi::v0_2_1::{Callback, CalloutId, HeaderPairs, HttpCallResponse};
use std::borrow::Cow;
use std::mem;

/// The phase that a plugin paused in.
///
/// A response phase has the response header or the trailers, which are not in the session.
pub(in crate::chain) enum PausedPhase<'a> {
    RequestHeaders,
    RequestBody,
    ResponseHeaders(&'a mut ResponseHeader),
    ResponseBody,
    ResponseTrailers(&'a mut http::HeaderMap),
}

impl PausedPhase<'_> {
    /// Return the callback of the phase. During a delivery, the plugin has the access that it
    /// has in this callback.
    fn callback(&self) -> Callback {
        match self {
            PausedPhase::RequestHeaders => Callback::RequestHeaders,
            PausedPhase::RequestBody => Callback::RequestBody,
            PausedPhase::ResponseHeaders(_) => Callback::ResponseHeaders,
            PausedPhase::ResponseBody => Callback::ResponseBody,
            PausedPhase::ResponseTrailers(_) => Callback::ResponseTrailers,
        }
    }

    /// Return the direction that the plugin must continue to end its pause.
    pub(super) fn direction(&self) -> StreamType {
        match self {
            PausedPhase::RequestHeaders | PausedPhase::RequestBody => StreamType::HttpRequest,
            _ => StreamType::HttpResponse,
        }
    }

    /// Return the direction of the body that the plugin holds, for a body phase.
    fn body_direction(&self) -> Option<BodyDirection> {
        match self {
            PausedPhase::RequestBody => Some(BodyDirection::Request),
            PausedPhase::ResponseBody => Some(BodyDirection::Response),
            _ => None,
        }
    }
}

fn borrowed_header_pairs(pairs: &[(Vec<u8>, Vec<u8>)]) -> HeaderPairs<'_> {
    pairs
        .iter()
        .map(|(name, value)| (Cow::Borrowed(&name[..]), Cow::Borrowed(&value[..])))
        .collect()
}

/// Return `result` as the response that the host gives to `proxy_on_http_call_response`.
fn http_call_response(result: &CalloutResult) -> HttpCallResponse<'_> {
    match result {
        CalloutResult::Response {
            headers,
            body,
            trailers,
        } => HttpCallResponse::received(borrowed_header_pairs(headers))
            .with_body(Cow::Borrowed(&body[..]))
            .with_trailers(borrowed_header_pairs(trailers)),
        CalloutResult::Failed => HttpCallResponse::failed(),
    }
}

impl WasmCtx {
    /// Run `proxy_on_http_call_response` of the plugin at `position` with the result of callout
    /// `id`.
    pub(super) fn deliver_callout_result<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
        position: usize,
        phase: &mut PausedPhase<'_>,
        id: CalloutId,
        result: &CalloutResult,
    ) -> Result<()> {
        let runtime = self.chain.runtime.clone();
        let pool = &runtime.pools[self.chain.plugins[position]];
        let Some(record) = self.records[position] else {
            return Err(self.plugin_error(position, "has no context for a callout result"));
        };
        let mut locked = LockedSlot::of_request(pool, &record)?;
        let loaded = locked.loaded()?;
        let delivery = self.with_phase_in_stream(session, position, phase, |ctx| {
            ctx.run_for_context(loaded, record.context, |scope| {
                scope.on_http_call_response(record.context, id, http_call_response(result))
            })
        });
        match delivery {
            Ok(()) => Ok(()),
            Err(e) => Err(locked.guest_failure("failed in on_http_call_response", e)),
        }
    }

    /// Run `guest_call` while the stream state has what the plugin can read and write in
    /// `phase`.
    fn with_phase_in_stream<DS: DownstreamSession, R>(
        &mut self,
        session: &mut Session<DS>,
        position: usize,
        phase: &mut PausedPhase<'_>,
        guest_call: impl FnOnce(&mut Self) -> R,
    ) -> R {
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
            self.held.put(direction, position, buffer.into_vec());
        }
        match phase {
            PausedPhase::ResponseHeaders(response) => self.response_out(response),
            PausedPhase::ResponseTrailers(trailers) => {
                if let Some(map) = self.stream().trailers.take() {
                    **trailers = map.trailers;
                }
            }
            _ => {}
        }
        self.request_out(session.req_header_mut());
        self.stream().delivery_callback = None;
        result
    }
}
