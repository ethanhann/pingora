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

//! Test proxy
//!
//! A `ProxyHttp` that forwards each filter to the matching `WasmCtx` method.

use async_trait::async_trait;
use bytes::Bytes;
use pingora_core::protocols::Digest;
use pingora_core::upstreams::peer::HttpPeer;
use pingora_core::{Error, ErrorType, Result};
use pingora_http::{RequestHeader, ResponseHeader};
use pingora_proxy::{ProxyHttp, Session};
use pingora_wasm::{write_plugin_response, RequestOutcome, WasmChain, WasmCtx};
use std::time::Duration;

pub struct TestProxy {
    pub chain: WasmChain,
}

#[derive(Default)]
pub struct TestCtx {
    wasm: Option<WasmCtx>,
    attempts: usize,
}

/// Request header with the origin port for the first upstream attempt.
///
/// A request with this header is retried if the first attempt fails, and the retry goes to the
/// origin in `ORIGIN`.
const FIRST_ORIGIN: &str = "x-test-first-origin";
const ORIGIN: &str = "x-test-origin";
/// Upstream request header listing the plugins that were skipped on the request.
const SKIPPED_PLUGINS: &str = "x-test-skipped";

#[async_trait]
impl ProxyHttp for TestProxy {
    type CTX = TestCtx;

    fn new_ctx(&self) -> Self::CTX {
        TestCtx::default()
    }

    async fn request_filter(&self, session: &mut Session, ctx: &mut Self::CTX) -> Result<bool> {
        let wasm = ctx.wasm.insert(self.chain.new_ctx());
        wasm.set_property(&["xds", "route_name"], "test-route");
        match wasm.request_filter(session).await? {
            RequestOutcome::Respond(header, body) => {
                write_plugin_response(session, header, body).await?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn upstream_peer(
        &self,
        session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        if let Some(wasm) = ctx.wasm.as_mut() {
            wasm.upstream_attempt();
        }
        ctx.attempts += 1;
        let headers = &session.req_header().headers;
        let first = headers.get(FIRST_ORIGIN).filter(|_| ctx.attempts == 1);
        let port: u16 = first
            .or(headers.get(ORIGIN))
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .ok_or_else(|| Error::explain(ErrorType::HTTPStatus(400), "no x-test-origin"))?;
        let peer = HttpPeer::new(("127.0.0.1", port), false, String::new());
        Ok(Box::new(peer))
    }

    async fn connected_to_upstream(
        &self,
        _session: &mut Session,
        _reused: bool,
        peer: &HttpPeer,
        #[cfg(unix)] _fd: std::os::unix::io::RawFd,
        #[cfg(windows)] _sock: std::os::windows::io::RawSocket,
        _digest: Option<&Digest>,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        if let Some(wasm) = ctx.wasm.as_mut() {
            wasm.upstream_connected(peer);
        }
        Ok(())
    }

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        let skipped: Vec<&str> = match ctx.wasm.as_ref() {
            Some(wasm) => wasm.skipped_plugins().collect(),
            None => Vec::new(),
        };
        if !skipped.is_empty() {
            upstream_request.insert_header(SKIPPED_PLUGINS, skipped.join(","))?;
        }
        Ok(())
    }

    fn error_while_proxy(
        &self,
        _peer: &HttpPeer,
        session: &mut Session,
        mut e: Box<Error>,
        _ctx: &mut Self::CTX,
        _client_reused: bool,
    ) -> Box<Error> {
        let retry = session.req_header().headers.contains_key(FIRST_ORIGIN);
        e.set_retry(retry && !session.as_ref().retry_buffer_truncated());
        e
    }

    async fn response_filter(
        &self,
        session: &mut Session,
        upstream_response: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        match ctx.wasm.as_mut() {
            Some(wasm) => wasm.response_filter(session, upstream_response).await,
            None => Ok(()),
        }
    }

    async fn request_body_filter(
        &self,
        session: &mut Session,
        body: &mut Option<Bytes>,
        end_of_stream: bool,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        match ctx.wasm.as_mut() {
            Some(wasm) => wasm.request_body_filter(session, body, end_of_stream).await,
            None => Ok(()),
        }
    }

    async fn response_body_filter(
        &self,
        session: &mut Session,
        body: &mut Option<Bytes>,
        end_of_stream: bool,
        ctx: &mut Self::CTX,
    ) -> Result<Option<Duration>> {
        if let Some(wasm) = ctx.wasm.as_mut() {
            wasm.response_body_filter(session, body, end_of_stream)
                .await?;
        }
        Ok(None)
    }

    async fn response_trailer_filter(
        &self,
        session: &mut Session,
        upstream_trailers: &mut http::HeaderMap,
        ctx: &mut Self::CTX,
    ) -> Result<Option<Bytes>> {
        match ctx.wasm.as_mut() {
            Some(wasm) => {
                wasm.response_trailer_filter(session, upstream_trailers)
                    .await
            }
            None => Ok(None),
        }
    }

    fn suppress_error_log(&self, _session: &Session, ctx: &Self::CTX, _e: &Error) -> bool {
        ctx.wasm.as_ref().is_some_and(WasmCtx::plugin_responded)
    }

    async fn logging(&self, session: &mut Session, _e: Option<&Error>, ctx: &mut Self::CTX) {
        if let Some(wasm) = ctx.wasm.as_mut() {
            wasm.logging(session).await;
        }
    }
}
