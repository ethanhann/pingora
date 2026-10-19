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

//! Phase test support
//!
//! Helpers for running any one of the phases after the request headers, so that a test can be
//! written once and run for each of them.

use super::{body_chunk, body_plugin, Wat};
use crate::{WasmCtx, WasmPluginConf};
use bytes::Bytes;
use futures::poll;
use http::header::CONTENT_LENGTH;
use pingora_error::Result;
use pingora_http::ResponseHeader;
use pingora_proxy::Session;
use std::pin::pin;

/// The phases that `run_phase` can run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    RequestHeaders,
    RequestBody,
    ResponseHeaders,
    ResponseBody,
    ResponseTrailers,
}

/// The response header, body chunk, and trailers passed to the phases.
pub(crate) struct PhaseInputs {
    pub(crate) response: ResponseHeader,
    pub(crate) body: Option<Bytes>,
    pub(crate) trailers: http::HeaderMap,
}

impl PhaseInputs {
    pub(crate) fn new() -> Self {
        let mut response = ResponseHeader::build(200, None).unwrap();
        response.insert_header(CONTENT_LENGTH, 1).unwrap();
        PhaseInputs {
            response,
            body: body_chunk("x"),
            trailers: http::HeaderMap::new(),
        }
    }
}

/// Run the request header phase and record the first upstream attempt.
pub(crate) async fn run_request_headers(ctx: &mut WasmCtx, session: &mut Session) {
    ctx.request_filter(session).await.unwrap();
    ctx.upstream_attempt();
}

/// Run `phase` once on `inputs`, with a body chunk passed as the last one.
pub(crate) async fn run_phase(
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
        Phase::RequestHeaders => ctx.request_filter(session).await.map(|_| ()),
        Phase::RequestBody => ctx.request_body_filter(session, body, true).await,
        Phase::ResponseHeaders => ctx.response_filter(session, response).await,
        Phase::ResponseBody => ctx.response_body_filter(session, body, true).await,
        Phase::ResponseTrailers => ctx
            .response_trailer_filter(session, trailers)
            .await
            .map(|_| ()),
    }
}

/// Return a plugin that runs `callback` in `phase`, with `delivery` as its
/// `proxy_on_http_call_response`.
pub(crate) fn plugin_with_callback_in(
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
        Phase::RequestHeaders => wat.request_headers = callback,
        Phase::RequestBody => wat.request_body = Some(callback),
        Phase::ResponseHeaders => wat.response_headers = Some(callback),
        Phase::ResponseBody => wat.response_body = Some(callback),
        Phase::ResponseTrailers => wat.response_trailers = Some(callback),
    }
    body_plugin(name, wat)
}

/// Start a request, leave its plugin waiting for a callout in `phase`, and drop that phase's
/// future.
pub(crate) async fn cancel_a_wait_in(phase: Phase, ctx: &mut WasmCtx, session: &mut Session) {
    run_request_headers(ctx, session).await;
    let mut inputs = PhaseInputs::new();
    let mut waits = pin!(run_phase(ctx, session, phase, &mut inputs));
    assert!(poll!(waits.as_mut()).is_pending());
}
