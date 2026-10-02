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
//! A callout result is delivered by running `proxy_on_http_call_response` on the paused plugin,
//! with the stream state set up as it was in the callback the plugin paused in.

use crate::callout::CalloutResult;
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
        result: &CalloutResult,
    ) -> Result<bool> {
        let runtime = self.chain.runtime.clone();
        let pool = &runtime.pools[self.chain.plugins[position]];
        let Some(record) = self.records[position] else {
            return Err(self.plugin_error(position, "no context for callout result"));
        };
        let callback = Callback::HttpCallResponse;
        let Some(mut locked) = self.lock_slot_or_skip_plugin(pool, position, &record, callback)?
        else {
            return Ok(false);
        };
        let loaded = locked.loaded()?;
        let delivery = self.with_phase_in_stream(session, position, phase, |ctx| {
            ctx.run_for_context(loaded, record.context, |scope| {
                scope.on_http_call_response(record.context, id, result.as_http_call_response())
            })
        });
        match delivery {
            Ok(()) => Ok(true),
            Err(e) => {
                self.guest_call_failed(position, locked, Callback::HttpCallResponse, e)?;
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
