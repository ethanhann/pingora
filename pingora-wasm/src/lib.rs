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
//! For each request, create a [WasmCtx] from a chain and call its filters from your `ProxyHttp`
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
//! A proxy can also:
//!
//! - Write the response that a plugin sends itself, for example a 403. See [RequestOutcome].
//! - Let plugins call other services, for example a policy service. See [CalloutUpstreams].
//! - Run plugins that work on a timer or wait on a shared queue. See [WasmRuntime].
//! - Publish the counters, gauges, and histograms that plugins define. See [WasmMetricSink].
//! - Give plugins facts that only your proxy knows, such as the route it chose. See
//!   [WasmProperties].
//! - Keep its plugins and chains in a configuration file. See [WasmConf].
//! - Choose whether a request fails or continues when a plugin fails. See [FailPolicy].
//!
//! See `examples/wasm_proxy.rs` for a full proxy that calls every filter.

mod callout;
mod chain;
mod configuration;
mod observability;
mod plugins;
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
    CalloutFailure, PluginFailure, PluginFailureOutcome, PluginFailureReport, PrometheusMetricSink,
    WasmMetric, WasmMetricKind, WasmMetricRecorder, WasmMetricSink,
};
pub use plugins::{WasmChainHandle, WasmPlugins};
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
/// Whether a failure fails the request depends on the plugin's [FailPolicy]. The default
/// `fail_to_proxy` of `ProxyHttp` responds with 503 for this type.
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
/// [WasmConf::chain_plugins]. A plugin whose file cannot be compiled, or whose guest does not
/// start, is reported with this type as well. A file that cannot be read is a `ReadError`.
pub const ERR_INVALID_CONF: ErrorType = ErrorType::new("WasmInvalidConf");

pub(crate) fn invalid_conf(detail: impl Into<String>) -> Box<Error> {
    Error::explain(ERR_INVALID_CONF, detail.into())
}

pub(crate) fn plugin_unavailable(name: &str, what: &str) -> Box<Error> {
    Error::explain(ERR_PLUGIN_FAILED, format!("wasm plugin {name}: {what}"))
}
