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

//! Run [Proxy-Wasm](https://github.com/proxy-wasm/spec) plugins in a Pingora proxy.
//!
//! A [WasmRuntime] compiles your plugins once, before the server starts. A [WasmChain] is an
//! ordered list of those plugins. Build one chain for all traffic, or one chain for each route.
//! For each request, create a [WasmCtx] from a chain and call its phases from your `ProxyHttp`
//! filters:
//!
//! - [WasmCtx::request_filter] from `request_filter`, after the checks your proxy runs itself
//! - [WasmCtx::upstream_attempt] from `upstream_peer`
//! - [WasmCtx::request_body_filter] from `request_body_filter`
//! - [WasmCtx::response_filter] from `response_filter`
//! - [WasmCtx::response_body_filter] from `response_body_filter`
//! - [WasmCtx::response_trailer_filter] from `response_trailer_filter`
//! - [WasmCtx::logging] from `logging`, for every request that created a `WasmCtx`
//!
//! Plugins run on headers by default. To run a plugin on bodies or on response trailers, turn on
//! [WasmPluginConf::request_body], [WasmPluginConf::response_body], or
//! [WasmPluginConf::response_trailers] for that plugin. Pingora does not read request trailers,
//! so plugins do not receive them.
//!
//! # When a plugin sends its own response
//!
//! A plugin can respond to a request itself, for example to deny it with 403. From
//! [WasmCtx::request_filter] you get that response as [RequestOutcome::Respond], and you write
//! it to the downstream. [WasmCtx::request_body_filter] and [WasmCtx::response_filter] write the
//! response for you, and then return an error with the status of the response to stop the
//! request. After that error, [WasmCtx::plugin_responded] returns `true`, which is how you tell
//! it from a plugin failure.
//!
//! Pingora logs the error of a request unless `suppress_error_log` returns `true`. You can
//! return [WasmCtx::plugin_responded] from it, as the example below does.
//!
//! # When a plugin fails
//!
//! A phase returns an error of type [ERR_PLUGIN_FAILED] when a plugin traps or returns an error,
//! and Pingora responds with 503.
//!
//! A plugin that pauses a body holds its bytes, up to a limit. Past
//! [WasmPluginConf::request_body_limit], [WasmCtx::request_body_filter] returns an error of type
//! [ERR_REQUEST_BODY_TOO_LARGE], and Pingora responds with 413. Past
//! [WasmPluginConf::response_body_limit], [WasmCtx::response_body_filter] returns an error of
//! type [ERR_RESPONSE_BODY_TOO_LARGE]. Pingora sent the response header before that point, so
//! the downstream receives a response that ends early.
//!
//! ```no_run
//! use async_trait::async_trait;
//! use bytes::Bytes;
//! use pingora_core::upstreams::peer::HttpPeer;
//! use pingora_core::{Error, Result};
//! use pingora_http::ResponseHeader;
//! use pingora_proxy::{ProxyHttp, Session};
//! use pingora_wasm::{
//!     write_plugin_response, RequestOutcome, WasmChain, WasmCtx, WasmPluginConf, WasmRuntime,
//! };
//! use std::time::Duration;
//!
//! struct MyProxy {
//!     chain: WasmChain,
//! }
//!
//! #[async_trait]
//! impl ProxyHttp for MyProxy {
//!     type CTX = WasmCtx;
//!
//!     fn new_ctx(&self) -> WasmCtx {
//!         self.chain.new_ctx()
//!     }
//!
//!     async fn request_filter(&self, session: &mut Session, ctx: &mut WasmCtx) -> Result<bool> {
//!         match ctx.request_filter(session).await? {
//!             RequestOutcome::Respond(header, body) => {
//!                 write_plugin_response(session, header, body).await?;
//!                 Ok(true)
//!             }
//!             _ => Ok(false),
//!         }
//!     }
//!
//!     async fn upstream_peer(
//!         &self,
//!         _session: &mut Session,
//!         ctx: &mut WasmCtx,
//!     ) -> Result<Box<HttpPeer>> {
//!         ctx.upstream_attempt();
//!         Ok(Box::new(HttpPeer::new(("127.0.0.1", 8080), false, String::new())))
//!     }
//!
//!     async fn request_body_filter(
//!         &self,
//!         session: &mut Session,
//!         body: &mut Option<Bytes>,
//!         end_of_stream: bool,
//!         ctx: &mut WasmCtx,
//!     ) -> Result<()> {
//!         ctx.request_body_filter(session, body, end_of_stream).await
//!     }
//!
//!     async fn response_filter(
//!         &self,
//!         session: &mut Session,
//!         upstream_response: &mut ResponseHeader,
//!         ctx: &mut WasmCtx,
//!     ) -> Result<()> {
//!         ctx.response_filter(session, upstream_response).await
//!     }
//!
//!     async fn response_body_filter(
//!         &self,
//!         session: &mut Session,
//!         body: &mut Option<Bytes>,
//!         end_of_stream: bool,
//!         ctx: &mut WasmCtx,
//!     ) -> Result<Option<Duration>> {
//!         ctx.response_body_filter(session, body, end_of_stream).await?;
//!         Ok(None)
//!     }
//!
//!     async fn response_trailer_filter(
//!         &self,
//!         session: &mut Session,
//!         upstream_trailers: &mut http::HeaderMap,
//!         ctx: &mut WasmCtx,
//!     ) -> Result<Option<Bytes>> {
//!         ctx.response_trailer_filter(session, upstream_trailers).await
//!     }
//!
//!     fn suppress_error_log(&self, _session: &Session, ctx: &WasmCtx, _e: &Error) -> bool {
//!         ctx.plugin_responded()
//!     }
//!
//!     async fn logging(&self, session: &mut Session, _e: Option<&Error>, ctx: &mut WasmCtx) {
//!         ctx.logging(session).await
//!     }
//! }
//!
//! # fn main() -> Result<()> {
//! let mut redact = WasmPluginConf::new("redact", "redact.wasm");
//! redact.response_body = true;
//! let runtime = WasmRuntime::new(vec![WasmPluginConf::new("auth", "auth.wasm"), redact])?;
//! let proxy = MyProxy {
//!     chain: runtime.chain(&["auth", "redact"])?,
//! };
//! # Ok(())
//! # }
//! ```

mod chain;
mod runtime;
mod stream;
#[cfg(test)]
mod test_support;

pub use chain::{RequestOutcome, WasmChain, WasmCtx};
pub use proxy_wasm_host::abi::v0_2_1::types::LogLevel;
pub use proxy_wasm_host::abi::v0_2_1::{LogContext, LogSink};
pub use proxy_wasm_host::Limits;
pub use runtime::{WasmPluginConf, WasmRuntime};
pub use stream::write_plugin_response;

use http::StatusCode;
use pingora_error::{Error, ErrorType};
use proxy_wasm_host::abi::v0_2_1::GuestError;

/// The error type that a phase returns when a plugin fails.
///
/// The default `fail_to_proxy` of `ProxyHttp` responds with 503 for this type. Check for it in
/// your own `fail_to_proxy` to send a different response.
///
/// A plugin can send its own response with the status 503, and the error for that response has this
/// type too. Check [WasmCtx::plugin_responded] before you treat the error as a failure.
pub const ERR_PLUGIN_FAILED: ErrorType =
    ErrorType::HTTPStatus(StatusCode::SERVICE_UNAVAILABLE.as_u16());

/// The error type for a request body that a plugin holds past its limit.
///
/// [WasmCtx::request_body_filter] returns it, and the default `fail_to_proxy` responds with 413.
pub const ERR_REQUEST_BODY_TOO_LARGE: ErrorType =
    ErrorType::HTTPStatus(StatusCode::PAYLOAD_TOO_LARGE.as_u16());

/// The error type for a response body that a plugin holds past its limit.
///
/// [WasmCtx::response_body_filter] returns it. Pingora sent the response header before the body, so
/// the default `fail_to_proxy` sends nothing, and the downstream receives a response that ends
/// early.
pub const ERR_RESPONSE_BODY_TOO_LARGE: ErrorType =
    ErrorType::HTTPStatus(StatusCode::INTERNAL_SERVER_ERROR.as_u16());

pub(crate) fn plugin_failure(name: &str, what: &str, cause: GuestError) -> Box<Error> {
    Error::because(
        ERR_PLUGIN_FAILED,
        format!("wasm plugin {name} {what}"),
        cause,
    )
}

pub(crate) fn plugin_unavailable(name: &str, what: &str) -> Box<Error> {
    Error::explain(ERR_PLUGIN_FAILED, format!("wasm plugin {name} {what}"))
}
