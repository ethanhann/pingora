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

//! Plugin metrics and failure reporting

mod log_sink;
mod plugin_failure;
mod prometheus_recorders;
mod prometheus_sink;

pub(crate) use log_sink::LogCrateSink;
pub use plugin_failure::{PluginFailure, PluginFailureOutcome, PluginFailureReport};
pub use prometheus_sink::PrometheusMetricSink;

/// A sink for the metrics plugins define, and for reports of failed callouts, plugin failures,
/// and replaced guests.
///
/// Implement this to publish plugin metrics to your own metrics system, or use
/// [PrometheusMetricSink]. Set your sink in
/// [WasmServices::metric_sink](crate::WasmServices::metric_sink). The runtime keeps the metric
/// values itself, so plugins can read their metrics back whichever sink is set, including the
/// default one, which publishes nothing.
///
/// When you replace a runtime to reload plugins, pass the same sink to the new runtime. The new
/// runtime registers its metrics from scratch, so a shared sink will see
/// [register_metric](Self::register_metric) again for a VM id and name it already knows. When the
/// old runtime is dropped, it subtracts what its gauges added through their recorders, which
/// leaves a gauge on a shared sink at the sum over the runtimes still alive.
pub trait WasmMetricSink: Send + Sync {
    /// Return a recorder for a metric a plugin has defined.
    ///
    /// This is called the first time a runtime sees `proxy_define_metric` for a given VM id and
    /// name. Every later change to the metric is passed to the recorder you return. Return `None`
    /// to leave the metric unpublished, which is what the default implementation does.
    fn register_metric(&self, _metric: &WasmMetric) -> Option<Box<dyn WasmMetricRecorder>> {
        None
    }

    /// Report a failed callout made by the plugin `plugin_name`.
    ///
    /// By default it does nothing.
    fn callout_failed(&self, _plugin_name: &str, _failure: CalloutFailure) {}

    /// Report a plugin failure.
    ///
    /// This is called when a plugin fails, under both fail policies. `report` has the outcome,
    /// either a failed request or a skipped plugin. Only the first failure of a plugin on a
    /// request is reported. By default it does nothing.
    fn plugin_failed(&self, _report: &PluginFailureReport<'_>) {}

    /// Report that a guest of the plugin `plugin_name` was replaced after a failure.
    ///
    /// This is called each time a new guest is started in a slot that had lost its guest. By
    /// default it does nothing.
    fn guest_replaced(&self, _plugin_name: &str) {}
}

/// A recorder for the changes plugins make to one metric.
///
/// You return one from [WasmMetricSink::register_metric]. A counter or gauge only calls
/// [add](Self::add) and a histogram only calls [record](Self::record). Both do nothing by default.
pub trait WasmMetricRecorder: Send + Sync {
    /// Add `delta` to a counter or gauge.
    ///
    /// A counter only gets positive deltas. A gauge gets negative ones as well.
    fn add(&self, _delta: i64) {}

    /// Record one value in a histogram.
    fn record(&self, _value: u64) {}
}

/// A metric defined by a plugin.
///
/// The VM id and the name are raw bytes on the plugin side. They are converted lossily here, so
/// any invalid UTF-8 becomes `U+FFFD`.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WasmMetric {
    /// The VM id of the plugin that defined the metric. Plugins with the same VM id share metrics.
    pub vm_id: String,
    /// The name the plugin passed to `proxy_define_metric`.
    pub name: String,
    /// The kind of metric.
    pub kind: WasmMetricKind,
}

/// The kind of a [WasmMetric].
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WasmMetricKind {
    /// A value that only increases.
    Counter,
    /// A gauge that plugins can raise, lower, or set.
    Gauge,
    /// A distribution of recorded values.
    Histogram,
}

/// The reason a plugin's callout failed.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CalloutFailure {
    /// The callout timed out before its response header was received, either as a whole or on a
    /// single read or write.
    Timeout,
    /// [CalloutUpstreams](crate::CalloutUpstreams) did not return a peer for the upstream.
    NoPeer,
    /// The runtime already had
    /// [max_callouts_in_flight](crate::WasmServices::max_callouts_in_flight) callouts in flight.
    Overflow,
    /// Connecting to the peer, or the TLS handshake with it, timed out.
    ConnectTimeout,
    /// Connecting to the peer failed for any other reason.
    ConnectFailed,
    /// The peer's response was not valid HTTP.
    ProtocolError,
    /// The connection was closed, or failed in some other way, before the response header was
    /// received.
    ConnectionClosed,
    /// The callout failed or timed out after the response header was received.
    FailedAfterHeader,
    /// The response body exceeded
    /// [callout_response_limit](crate::WasmPluginConf::callout_response_limit).
    ResponseTooLarge,
    /// The task sending the callout panicked, or there was no tokio runtime to spawn it on.
    TaskFailed,
}

impl std::fmt::Display for CalloutFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl CalloutFailure {
    /// Return the failure as a snake_case string, e.g. `connect_timeout`.
    ///
    /// `Display` writes the same string.
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

pub(crate) struct NoMetricSink;

impl WasmMetricSink for NoMetricSink {}
