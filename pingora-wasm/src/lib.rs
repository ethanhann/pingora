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
//! While a plugin waits for a callout, the filter that ran the plugin waits with it, and returns
//! once the plugin continues or sends a response. Each callout can take as long as the timeout
//! the plugin passes, up to [WasmPluginConf::callout_timeout_limit]. A plugin may send another
//! callout when a response arrives, so the wait as a whole has a limit of its own,
//! [WasmPluginConf::callout_wait_limit]. A wait that reaches this limit counts as a plugin
//! failure. If an HTTP/2 client resets its stream during the wait, the filter returns an error
//! at once. Pingora cannot see that an HTTP/1 client disconnected until it writes to it, so the
//! filter keeps waiting, and the write of the response then fails.
//!
//! [WasmCtx::request_body_filter], [WasmCtx::response_filter], and
//! [WasmCtx::response_body_filter] run while Pingora reads from the upstream. Pingora fails the
//! request when the upstream is silent for the `read_timeout` of its peer. For a plugin that
//! sends callouts from these filters, keep [WasmPluginConf::callout_wait_limit] below that
//! timeout.
//!
//! If the future of a filter is dropped while a plugin waits for a callout, the request cannot
//! continue. Pingora drops the future of a body filter when the upstream fails, and a timeout of
//! your own around a filter drops it as well. Every later header or trailer filter of that request,
//! and every later body filter that has a plugin to run, then returns an [ERR_PLUGIN_FAILED]
//! error under both fail policies, so end the request. [WasmCtx::logging] still runs. A timeout of your own can therefore only fail the
//! request. To let a request continue without a slow plugin, set
//! [WasmPluginConf::callout_wait_limit] and [FailPolicy::Open] on that plugin instead.
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
//! Each slot of a plugin is a separate guest, so each slot gets its own ticks. A tick will wait
//! for a request that is running in the same slot, and a request will wait for a running tick.
//! When a queue gets an item, the guest that registered the queue last receives
//! `proxy_on_queue_ready`.
//!
//! Plugins with the same [VM id](WasmPluginConf::vm_id) share data, queues, and metrics, and
//! each plugin still has its own guests.
//!
//! A plugin can keep its context after the request ends, for example to wait for the response
//! to a callout that it sent from `proxy_on_done`. Its `proxy_on_done` then returns `false`,
//! and the plugin calls `proxy_done` later. Callout results for such a context are delivered on
//! the same thread, and the runtime then runs its `proxy_on_log` and `proxy_on_delete`.
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
//! The sink also publishes three counters of its own:
//!
//! - `wasm_callout_failures_total` counts failed callouts, by plugin and by reason
//! - `wasm_plugin_failures_total` counts plugin failures, by plugin, by kind of failure, and by
//!   whether the request failed or the plugin was skipped
//! - `wasm_guests_replaced_total` counts the guests replaced after a failure, by plugin
//!
//! A registry accepts each name once, so when you reload plugins, pass the same sink to the new
//! runtime.
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
//! [WasmServices::fixed_properties]. A plugin cannot change a property that your proxy set on
//! the request, one that the runtime provides, or a fixed property. During a request, a
//! `proxy_set_property` call on such a path still succeeds, but the plugin reads the same value
//! as before, and what it wrote is only returned by [WasmCtx::guest_property]. Ticks and the
//! other callbacks that run outside of a request read only the fixed properties, and
//! `proxy_set_property` fails in them for every path.
//!
//! The runtime provides these properties, where an integer is 8 little-endian bytes and a bool is
//! one byte:
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
//! # Configuration from a file
//!
//! [WasmConf] and the types inside it implement serde's `Deserialize`, so you can keep your
//! plugins and chains in a configuration file. This crate does not read the file for you. Add a
//! [WasmConf] field to your own configuration struct, or parse one from the top level of a file.
//! The format has to be self-describing, such as YAML, JSON, or TOML. At its top level a
//! `WasmConf` ignores the keys it does not know, as Pingora's `ServerConf` does, so one file can
//! hold the settings for Pingora, your proxy, and your plugins:
//!
//! ```yaml
//! version: 1
//! threads: 4
//!
//! plugins:
//!   - name: auth
//!     path: /etc/proxy/plugins/auth.wasm
//!     configuration: '{"mode": "strict"}'
//!   - name: stats
//!     path: /etc/proxy/plugins/stats.wasm
//!     fail_policy: open
//!
//! chains:
//!   default: [auth, stats]
//!
//! static_callout_upstreams:
//!   authz:
//!     address: 10.0.0.5:8181
//! ```
//!
//! [WasmConf::services] returns the [WasmServices] the file describes. Set on it whatever a file
//! cannot hold, such as a metric sink, then build the runtime and its chains:
//!
//! ```no_run
//! use pingora_wasm::{PrometheusMetricSink, WasmConf, WasmRuntime};
//! use std::sync::Arc;
//!
//! # fn main() -> pingora_core::Result<()> {
//! let yaml = std::fs::read_to_string("proxy.yaml").expect("a readable configuration file");
//! let conf: WasmConf = serde_yaml::from_str(&yaml).expect("a valid configuration");
//! let mut services = conf.services();
//! let registry = pingora_wasm::prometheus::default_registry().clone();
//! let sink = PrometheusMetricSink::new(registry).expect("a registry with no wasm metrics");
//! services.metric_sink = Arc::new(sink);
//! let runtime = WasmRuntime::new_with_services(conf.plugins.clone(), services)?;
//! let chain = runtime.chain(&conf.chain_plugins("default")?)?;
//! # Ok(())
//! # }
//! ```
//!
//! A plugin entry needs a `name` and a `path`, and every other key defaults to what
//! [WasmPluginConf::new] sets. [WasmConf::plugins] describes the keys.
//!
//! A misspelled key inside a plugin entry, a missing `name` or `path`, an unknown `log_level`,
//! a hostname as the address of a callout upstream, or a fixed property that is not a string is
//! an error from your parser when the file is read. A mistake the parser cannot see, such as a
//! limit of zero or a chain that is not in the file, is returned as an [ERR_INVALID_CONF] error
//! by [WasmRuntime::new_with_services], [WasmConf::chain_plugins], or [WasmRuntime::chain].
//!
//! # When a plugin fails
//!
//! A plugin fails when one of its callbacks traps or returns an error, when it has no guest to
//! run a request on, when it pauses on headers, on trailers, or on the last chunk of a body with
//! no callout to wait for, or when its callouts take longer than
//! [WasmPluginConf::callout_wait_limit]. What happens to the request is decided by the plugin's
//! [fail policy](WasmPluginConf::fail_policy).
//!
//! With [FailPolicy::Closed], which is the default, the failure fails the request. The filter
//! returns an [ERR_PLUGIN_FAILED] error and Pingora responds with 503, or ends the response
//! early if its header has already been sent. The exception is
//! [WasmCtx::response_trailer_filter]. Pingora only logs an error from `response_trailer_filter`
//! and still sends the trailers, and if a plugin was holding body bytes, the filter logs the
//! failure and returns those bytes in place of the error.
//!
//! The 503 that Pingora's `fail_to_proxy` writes for a failed request is not run through
//! `proxy_on_response_headers` of the other plugins. Those plugins still run `proxy_on_log`,
//! where `response.code` reads 503.
//!
//! With [FailPolicy::Open], the failure is logged, the plugin is skipped for the rest of the
//! request, and the request continues with the other plugins. Per plugin, one skip every 10
//! seconds is logged as a warning, and the rest at debug level. On a plugin that
//! authorizes requests, `Open` lets a request through each time the plugin crashes, hangs, or is
//! too slow. Use it for plugins a request can do without, such as one that collects statistics.
//! [WasmCtx::skipped_plugins] returns the plugins that were skipped on a request, so your proxy
//! can still apply a rule of its own, e.g. deny a request that skipped its authorization plugin.
//!
//! | Event | `Closed` | `Open` |
//! |---|---|---|
//! | A callback traps or returns an error | request fails | plugin skipped |
//! | The plugin has no guest when the request starts | request fails | plugin skipped |
//! | The guest running the request is replaced or lost | request fails | plugin skipped |
//! | The plugin pauses with no callout, except to hold a body | request fails | plugin skipped |
//! | A callout wait reaches `callout_wait_limit` | request fails | plugin skipped |
//! | The plugin fails while a body it changed has bytes to come | request fails | request fails |
//! | The plugin holds more body bytes than its limit | request fails | request fails |
//! | The plugin sends a response after the response header | request fails | request fails |
//! | An earlier filter was cancelled during a callout wait | request fails | request fails |
//!
//! The last four events fail the request under both policies, and
//! [WasmPluginConf::fail_policy] gives the reason for each. A plugin changes a body when it
//! writes to the body bytes, or when it changes the value of the `content-length` or
//! `transfer-encoding` header of that message. If it fails while that body can still have bytes
//! to come, skipping it would send the rest of the body without its changes. This only applies
//! to a body the plugin runs on, and only until that body has ended.
//!
//! A plugin can pause a body to hold its bytes until the last chunk arrives, up to a limit. Past
//! [WasmPluginConf::request_body_limit], [WasmCtx::request_body_filter] returns an
//! [ERR_REQUEST_BODY_TOO_LARGE] error, and Pingora responds with 413. Past
//! [WasmPluginConf::response_body_limit], [WasmCtx::response_body_filter] returns an
//! [ERR_RESPONSE_BODY_TOO_LARGE] error. Pingora has usually sent the response header before that
//! point, so the downstream receives a response that ends early. If the header has not been sent
//! yet, Pingora responds with 500.
//!
//! A guest that can no longer be used is replaced under both policies. The first failure of each
//! plugin on a request is reported to the [metric sink](WasmMetricSink::plugin_failed). A plugin
//! that cannot start fails [WasmRuntime::new] whatever its policy.
//!
//! The proxy below calls each filter of its [WasmCtx] and leaves its plugins on the default
//! policy:
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
mod configuration;
mod observability;
mod properties;
mod root_callbacks;
mod runtime;
mod stream_state;
#[cfg(test)]
mod test_support;

pub use callout::{CalloutTarget, CalloutUpstreams, StaticCalloutUpstreams};
pub use chain::write_plugin_response;
pub use chain::{RequestOutcome, WasmChain, WasmCtx};
pub use configuration::{CalloutUpstreamConf, WasmConf};
pub use observability::{
    CalloutFailure, FailureOutcome, PluginFailure, PluginFailureReport, PrometheusMetricSink,
    WasmMetric, WasmMetricKind, WasmMetricRecorder, WasmMetricSink,
};
/// Re-export of the `prometheus` crate that [PrometheusMetricSink] is built against.
///
/// Create your registry through this re-export to make sure its version matches.
pub use prometheus;
pub use properties::{WasmProperties, WasmPropertyValue};
pub use proxy_wasm_host::abi::v0_2_1::types::LogLevel;
pub use proxy_wasm_host::abi::v0_2_1::{LogContext, LogSink};
pub use proxy_wasm_host::Limits;
pub use runtime::{FailPolicy, WasmPluginConf, WasmRuntime, WasmServices};

use http::StatusCode;
use pingora_error::{Error, ErrorType};

/// The error type returned by a filter when a plugin failure fails the request.
///
/// Whether a failure fails the request depends on the plugin's
/// [fail_policy](WasmPluginConf::fail_policy). A plugin with [FailPolicy::Open] is skipped
/// after most failures, and the filter returns no error for them.
///
/// The default `fail_to_proxy` of `ProxyHttp` responds with 503 for this type. Match on it in
/// your own `fail_to_proxy` if you want to send a different response.
///
/// The error returned after a plugin sends its own 503 response has the same type, so check
/// [WasmCtx::plugin_responded] before treating the error as a failure.
pub const ERR_PLUGIN_FAILED: ErrorType =
    ErrorType::HTTPStatus(StatusCode::SERVICE_UNAVAILABLE.as_u16());

/// The error type returned when a plugin holds more of a request body than its limit allows.
///
/// Returned by [WasmCtx::request_body_filter], under both fail policies. The default
/// `fail_to_proxy` responds with 413.
pub const ERR_REQUEST_BODY_TOO_LARGE: ErrorType =
    ErrorType::HTTPStatus(StatusCode::PAYLOAD_TOO_LARGE.as_u16());

/// The error type returned when a plugin holds more of a response body than its limit allows.
///
/// Returned by [WasmCtx::response_body_filter], under both fail policies. The response header
/// has usually been sent by then, in which case the default `fail_to_proxy` writes nothing and
/// the downstream sees the response end early. If the header has not been sent yet, the default
/// `fail_to_proxy` responds with 500.
pub const ERR_RESPONSE_BODY_TOO_LARGE: ErrorType =
    ErrorType::HTTPStatus(StatusCode::INTERNAL_SERVER_ERROR.as_u16());

/// The error type returned for a mistake in the configuration.
///
/// Returned by [WasmRuntime::new], [WasmRuntime::new_with_services], [WasmRuntime::chain], and
/// [WasmConf::chain_plugins]. A plugin whose file cannot be read or compiled, or whose guest does
/// not start, is reported with this type as well.
pub const ERR_INVALID_CONF: ErrorType = ErrorType::new("WasmInvalidConf");

pub(crate) fn invalid_conf(detail: impl Into<String>) -> Box<Error> {
    Error::explain(ERR_INVALID_CONF, detail.into())
}

pub(crate) fn plugin_unavailable(name: &str, what: &str) -> Box<Error> {
    Error::explain(ERR_PLUGIN_FAILED, format!("wasm plugin {name}: {what}"))
}
