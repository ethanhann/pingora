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

//! The metrics that plugins define, and where your proxy sends them.

mod prometheus_sink;
mod log_sink;

pub use prometheus_sink::PrometheusMetricSink;
pub(crate) use log_sink::LogCrateSink;

/// A receiver of the metrics that plugins define, and of a report for each failed callout.
///
/// A plugin can read its own metrics whatever sink you pass, because the runtime keeps their
/// values. Pass a sink in [WasmServices::metric_sink](crate::WasmServices::metric_sink) to publish the
/// metrics, for example [PrometheusMetricSink].
pub trait WasmMetricSink: Send + Sync {
    /// Return the recorder of a metric that a plugin defined for the first time.
    ///
    /// The runtime calls this once for each VM id and name, and sends each later change of the
    /// metric to the recorder. By default it returns `None`, and the metric is not published.
    fn register_metric(&self, _metric: &WasmMetric) -> Option<Box<dyn WasmMetricRecorder>> {
        None
    }

    /// Receive a callout of the plugin `plugin_name` that failed, with the reason in `failure`. By default it
    /// does nothing.
    fn callout_failed(&self, _plugin_name: &str, _failure: CalloutFailure) {}
}

/// The receiver of the changes that plugins make to one metric.
pub trait WasmMetricRecorder: Send + Sync {
    /// Add `delta` to a counter or a gauge. A counter receives only positive deltas.
    fn add(&self, _delta: i64) {}

    /// Record a value of a histogram.
    fn record(&self, _value: u64) {}
}

/// A metric that a plugin defined.
///
/// The VM id and the name come from the plugin as bytes. Bytes that are not valid UTF-8 become
/// the replacement character.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WasmMetric {
    /// The VM id of the plugin. Plugins with the same VM id share their metrics.
    pub vm_id: String,
    /// The name that the plugin passed to `proxy_define_metric`.
    pub name: String,
    /// The type of the metric.
    pub kind: WasmMetricKind,
}

/// The type of a [WasmMetric].
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WasmMetricKind {
    /// A value that only grows.
    Counter,
    /// A value that grows and shrinks.
    Gauge,
    /// A distribution of recorded values.
    Histogram,
}

/// Why a callout of a plugin failed.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CalloutFailure {
    /// The callout, or a read or a write of its connection, reached its timeout before the
    /// response header.
    Timeout,
    /// The [CalloutUpstreams](crate::CalloutUpstreams) returned no peer for the upstream name.
    NoPeer,
    /// The runtime was already sending
    /// [max_callouts_in_flight](crate::WasmServices::max_callouts_in_flight) callouts.
    Overflow,
    /// The connection or the TLS handshake reached its timeout.
    ConnectTimeout,
    /// The connection failed for another reason.
    ConnectFailed,
    /// The peer sent a response that is not valid HTTP.
    ProtocolError,
    /// The connection closed before the response header.
    ConnectionClosed,
    /// The callout failed after the response header.
    FailedAfterHeader,
    /// The response body is over
    /// [callout_response_limit](crate::WasmPluginConf::callout_response_limit).
    ResponseTooLarge,
    /// The task that sends the callout panicked, or no tokio runtime was running.
    TaskFailed,
}

impl std::fmt::Display for CalloutFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl CalloutFailure {
    /// Return the name of the failure in snake case, for example `connect_timeout`.
    pub fn as_str(&self) -> &'static str {
        match self {
            CalloutFailure::Timeout => "timeout",
            CalloutFailure::NoPeer => "no_peer",
            CalloutFailure::Overflow => "overflow",
            CalloutFailure::ConnectTimeout => "connect_timeout",
            CalloutFailure::ConnectFailed => "connect_failed",
            CalloutFailure::ProtocolError => "protocol_error",
            CalloutFailure::ConnectionClosed => "connection_closed",
            CalloutFailure::FailedAfterHeader => "failed_after_header",
            CalloutFailure::ResponseTooLarge => "response_too_large",
            CalloutFailure::TaskFailed => "task_failed",
        }
    }
}

/// The default sink, which publishes no metric and counts no failed callout.
pub(crate) struct NoMetricSink;

impl WasmMetricSink for NoMetricSink {}
