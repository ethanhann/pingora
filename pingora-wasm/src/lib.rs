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
//! - [WasmCtx::upstream_connected] from `connected_to_upstream`
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
//! # When a plugin calls another service
//!
//! A plugin can pause a request, send an HTTP request of its own to another service, and use
//! the response to decide whether the request continues. That HTTP request is a callout. A
//! typical example is an authorization plugin that asks a policy service about each request.
//!
//! The plugin refers to the service by an upstream name, such as `authz`. List the upstreams
//! that your plugins can call in [WasmServices::callout_upstreams]:
//!
//! ```no_run
//! use pingora_core::upstreams::peer::HttpPeer;
//! use pingora_wasm::{StaticCalloutUpstreams, WasmPluginConf, WasmRuntime, WasmServices};
//! use std::sync::Arc;
//!
//! # fn main() -> pingora_core::Result<()> {
//! let mut upstreams = StaticCalloutUpstreams::new();
//! upstreams.insert("authz", HttpPeer::new("10.0.0.5:8181", false, String::new()));
//! let mut services = WasmServices::default();
//! services.callout_upstreams = Arc::new(upstreams);
//! let plugins = vec![WasmPluginConf::new("auth", "auth.wasm")];
//! let runtime = WasmRuntime::new_with_services(plugins, services)?;
//! # Ok(())
//! # }
//! ```
//!
//! To select a backend for each callout, for example with a load balancer, implement
//! [CalloutUpstreams]. To send callouts to a peer over TLS, turn on one of the TLS features of
//! this crate, such as `openssl` or `rustls`. Without one, a callout to a TLS peer fails at its
//! timeout.
//!
//! While a plugin waits for a callout, the phase that ran the plugin waits with it, and your
//! filter returns once the plugin continues or sends a response. A callout can take as long as
//! the timeout that the plugin passes, up to [WasmPluginConf::callout_timeout_limit]. If an
//! HTTP/2 client resets its stream during the wait, the phase returns an error at once. Pingora
//! cannot see that an HTTP/1 client disconnected until it writes to it, so the phase waits for
//! the callout to end, and the write of the response then fails.
//!
//! [WasmCtx::request_body_filter], [WasmCtx::response_filter], and
//! [WasmCtx::response_body_filter] run while Pingora reads from the upstream. Pingora fails the
//! request when the upstream is silent for the `read_timeout` of its peer. For a plugin that
//! sends callouts from these phases, keep [WasmPluginConf::callout_timeout_limit] below that
//! timeout.
//!
//! If the future of a phase is dropped while a plugin waits for a callout, the request cannot
//! continue. Pingora drops the future of a body filter when the upstream fails, and you may drop
//! one with a timeout of your own. Every later phase of that request except [WasmCtx::logging]
//! then returns an error of type [ERR_PLUGIN_FAILED], so end the request.
//!
//! A plugin can also send a callout and continue, for example to report a request to an audit
//! service. The callout is sent, but the plugin does not receive its response. It receives a
//! failed result for the callout when its context ends. A callout from `proxy_on_log` may not
//! be sent when the server stops.
//!
//! A plugin can send a callout from its root context too, for example from a tick, and the
//! runtime delivers the response on the thread that runs the ticks. A callout from
//! `proxy_on_vm_start` or `proxy_on_configure` is sent once the first request arrives.
//!
//! The runtime sends at most [WasmServices::max_callouts_in_flight] callouts at the same time.
//! Each guest can have at most `max_open_callouts` callouts open, which is one of the
//! [limits](WasmPluginConf::limits) of its plugin. A plugin can therefore have that number of
//! waiting requests in each of its [slots](WasmPluginConf::slots). If you expect more, raise
//! the number of slots or that limit.
//!
//! # Periodic work and shared queues
//!
//! Some plugins do work on a timer, such as refilling a rate limit bucket or sending a batch of
//! log entries, and some wait for items on a shared queue. The runtime runs this work on a
//! thread of its own, named `wasm-root-calls`, so it never runs on the threads of your Pingora
//! services. The thread starts with the first request and stops when the runtime is dropped.
//!
//! Each slot of a plugin is a separate guest, so each slot gets its own ticks, as each worker
//! of Envoy does. A tick waits while a request runs in the same slot, and a request waits while
//! a tick runs. When a queue gets an item, the guest that registered the queue last receives
//! `proxy_on_queue_ready`.
//!
//! Plugins with the same [VM id](WasmPluginConf::vm_id) share data, queues, and metrics, and
//! each plugin still has its own guests.
//!
//! A plugin can keep its context after the request ends, for example to wait for the response
//! to a callout that it sent from `proxy_on_done`. Its `proxy_on_done` then returns `false`,
//! and the plugin calls `proxy_done` later. The runtime delivers the results of the callouts of
//! such a context on the same thread, and then runs its `proxy_on_log` and `proxy_on_delete`.
//! [WasmRuntime::held_contexts] counts the contexts that plugins hold.
//!
//! # Metrics
//!
//! Plugins define counters, gauges, and histograms. To publish them, pass a [WasmMetricSink] in
//! [WasmServices::metric_sink]. [PrometheusMetricSink] registers them in a Prometheus registry,
//! for example the default registry that `pingora-prometheus` serves:
//!
//! ```no_run
//! use pingora_wasm::{PrometheusMetricSink, WasmPluginConf, WasmRuntime, WasmServices};
//! use std::sync::Arc;
//!
//! # fn main() -> pingora_core::Result<()> {
//! let registry = pingora_wasm::prometheus::default_registry().clone();
//! let sink = PrometheusMetricSink::new(registry).expect("a registry with no wasm metrics");
//! let mut services = WasmServices::default();
//! services.metric_sink = Arc::new(sink);
//! let plugins = vec![WasmPluginConf::new("stats", "stats.wasm")];
//! let runtime = WasmRuntime::new_with_services(plugins, services)?;
//! # Ok(())
//! # }
//! ```
//!
//! The sink also counts the callouts that fail, by plugin and by reason, in
//! `wasm_callout_failures_total`. A registry accepts each name once, so when you reload plugins,
//! pass the same sink to the new runtime.
//!
//! # Properties
//!
//! A plugin reads facts about its request with `proxy_get_property`, such as `source.address`
//! or `request.path`, and the runtime provides the ones that Pingora knows. For a fact that only
//! your proxy knows, such as the route it chose, set a property on the request before the phase
//! in which a plugin reads it:
//!
//! ```
//! use pingora_wasm::WasmCtx;
//!
//! fn after_routing(ctx: &mut WasmCtx, route: &str) {
//!     ctx.set_property(&["xds", "route_name"], route);
//! }
//! ```
//!
//! For facts that do not change, such as the name of the node, use
//! [WasmServices::fixed_properties]. A plugin cannot change a property that your proxy set or
//! one that the runtime provides. Ticks and the other callbacks that run with no request read
//! only the fixed properties.
//!
//! The runtime provides these properties, with the encoding of Envoy, where an integer is 8
//! little-endian bytes and a bool is one byte:
//!
//! - `source.address`, `source.port`, `destination.address`, and `destination.port`
//! - `request.path`, `request.url_path`, `request.host`, `request.scheme`, `request.method`,
//!   and `request.protocol`
//! - `request.time` in nanoseconds since the Unix epoch, and `request.size`
//! - `response.code` in the response phases and in `logging`
//! - `request.duration` and `response.size` in `logging`
//! - `connection.mtls` and `connection.tls_version` for a TLS downstream
//! - `upstream.address` and `upstream.port` after [WasmCtx::upstream_connected]
//! - `plugin_name`, `plugin_root_id`, and `plugin_vm_id`
//!
//! Pingora does not count header bytes and does not keep the server name of a TLS connection,
//! so `request.total_size`, `response.total_size`, and `connection.requested_server_name` are
//! not provided.
//!
//! # When a plugin fails
//!
//! A phase returns an error of type [ERR_PLUGIN_FAILED] when a plugin traps or returns an error,
//! and Pingora responds with 503.
//!
//! A plugin that pauses and has no callout to wait for cannot continue, so the phase returns
//! the same error. The body phases are different. A plugin can pause a body to hold its bytes
//! until the last chunk arrives, up to a limit. Past
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

mod callout;
mod chain;
mod metrics;
mod properties;
mod root_callbacks;
mod runtime;
mod stream;
#[cfg(test)]
mod test_support;

pub use callout::{CalloutTarget, CalloutUpstreams, StaticCalloutUpstreams};
pub use chain::{RequestOutcome, WasmChain, WasmCtx};
pub use metrics::{
    CalloutFailure, PrometheusMetricSink, WasmMetric, WasmMetricKind, WasmMetricRecorder,
    WasmMetricSink,
};
/// The `prometheus` crate that [PrometheusMetricSink] uses, so that you pass a registry of the
/// same version.
pub use prometheus;
pub use properties::{WasmProperties, WasmPropertyValue};
pub use proxy_wasm_host::abi::v0_2_1::types::LogLevel;
pub use proxy_wasm_host::abi::v0_2_1::{LogContext, LogSink};
pub use proxy_wasm_host::Limits;
pub use runtime::{WasmPluginConf, WasmRuntime, WasmServices};
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
