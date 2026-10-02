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

//! Prometheus metric sink

use super::prometheus_recorders;
use super::{
    CalloutFailure, PluginFailureReport, WasmMetric, WasmMetricKind, WasmMetricRecorder,
    WasmMetricSink,
};
use log::warn;
use parking_lot::Mutex;
use prometheus::core::Collector;
use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

const CALLOUT_FAILURES_NAME: &str = "wasm_callout_failures_total";
const CALLOUT_FAILURES_HELP: &str = "Total number of failed wasm plugin callouts";
const PLUGIN_FAILURES_NAME: &str = "wasm_plugin_failures_total";
const PLUGIN_FAILURES_HELP: &str = "Total number of wasm plugin failures";
const GUESTS_REPLACED_NAME: &str = "wasm_guests_replaced_total";
const GUESTS_REPLACED_HELP: &str = "Total number of wasm plugin guests replaced after a failure";
const PLUGIN_METRIC_HELP: &str = "Metric defined by a wasm plugin";
const VM_ID_LABEL: &str = "vm_id";
const PLUGIN_LABEL: &str = "plugin";
const FAILURE_LABEL: &str = "failure";
const OUTCOME_LABEL: &str = "outcome";
// The ABI gives a plugin no way to choose buckets, so every histogram gets this fixed set
const HISTOGRAM_BUCKETS: [f64; 19] = [
    0.5, 1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0, 30000.0,
    60000.0, 300000.0, 600000.0, 1800000.0, 3600000.0,
];

/// A [WasmMetricSink] that publishes plugin metrics to a Prometheus registry.
///
/// Every plugin metric has a `vm_id` label. Characters Prometheus does not allow in a metric
/// name are replaced with `_`, so a plugin's `waf.tx.total` is published as `waf_tx_total`.
///
/// The sink has three counters of its own. Failed callouts are counted in
/// `wasm_callout_failures_total`, labeled with `plugin` and `failure`. Plugin failures are
/// counted in `wasm_plugin_failures_total`, labeled with `plugin`, `failure`, and `outcome`.
/// Guests replaced after a failure are counted in `wasm_guests_replaced_total`, labeled with
/// `plugin`.
///
/// A metric is left unpublished, with one warning logged for its name, if its name is empty or
/// rejected by Prometheus, if the registry already holds a collector under that name, if another
/// plugin metric already maps to the same Prometheus name, or if it was first defined with a
/// different kind. The plugin can still use such a metric as usual.
///
/// Create one sink and pass it to every runtime you build, including the ones you build to
/// reload plugins. A registry accepts each metric name only once, so a second sink on the same
/// registry cannot be created. With a shared sink, a reloaded plugin keeps reporting into the
/// existing series. Counters and histograms continue from where they were, and a gauge drops by
/// whatever the old runtime had added once that runtime is dropped.
///
/// ```no_run
/// use pingora_wasm::{PrometheusMetricSink, WasmServices};
/// use std::sync::Arc;
///
/// let registry = pingora_wasm::prometheus::default_registry().clone();
/// let mut services = WasmServices::default();
/// services.metric_sink = Arc::new(PrometheusMetricSink::new(registry).unwrap());
/// ```
pub struct PrometheusMetricSink {
    registry: Registry,
    callout_failures: IntCounterVec,
    plugin_failures: IntCounterVec,
    guests_replaced: IntCounterVec,
    families: Mutex<HashMap<String, Family>>,
    skipped_names: Mutex<HashSet<String>>,
}

/// The vector registered under one Prometheus name, and the plugin metric name that claimed it.
struct Family {
    name_in_plugin: String,
    vector: FamilyVector,
}

#[derive(Clone)]
enum FamilyVector {
    Counter(IntCounterVec),
    Gauge(IntGaugeVec),
    Histogram(HistogramVec),
}

impl FamilyVector {
    fn new(name: &str, kind: WasmMetricKind) -> prometheus::Result<Self> {
        let labels = [VM_ID_LABEL];
        Ok(match kind {
            WasmMetricKind::Counter => FamilyVector::Counter(IntCounterVec::new(
                Opts::new(name, PLUGIN_METRIC_HELP),
                &labels,
            )?),
            WasmMetricKind::Gauge => FamilyVector::Gauge(IntGaugeVec::new(
                Opts::new(name, PLUGIN_METRIC_HELP),
                &labels,
            )?),
            WasmMetricKind::Histogram => {
                let opts = HistogramOpts::new(name, PLUGIN_METRIC_HELP)
                    .buckets(HISTOGRAM_BUCKETS.to_vec());
                FamilyVector::Histogram(HistogramVec::new(opts, &labels)?)
            }
        })
    }

    fn kind(&self) -> WasmMetricKind {
        match self {
            FamilyVector::Counter(_) => WasmMetricKind::Counter,
            FamilyVector::Gauge(_) => WasmMetricKind::Gauge,
            FamilyVector::Histogram(_) => WasmMetricKind::Histogram,
        }
    }

    fn collector(&self) -> Box<dyn Collector> {
        match self {
            FamilyVector::Counter(vector) => Box::new(vector.clone()),
            FamilyVector::Gauge(vector) => Box::new(vector.clone()),
            FamilyVector::Histogram(vector) => Box::new(vector.clone()),
        }
    }

    fn recorder(&self, vm_id: &str) -> Box<dyn WasmMetricRecorder> {
        match self {
            FamilyVector::Counter(vector) => Box::new(prometheus_recorders::CounterRecorder(
                vector.with_label_values(&[vm_id]),
            )),
            FamilyVector::Gauge(vector) => Box::new(prometheus_recorders::GaugeRecorder(
                vector.with_label_values(&[vm_id]),
            )),
            FamilyVector::Histogram(vector) => Box::new(prometheus_recorders::HistogramRecorder(
                vector.with_label_values(&[vm_id]),
            )),
        }
    }
}

impl std::fmt::Debug for PrometheusMetricSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrometheusMetricSink")
            .field("families", &self.families.lock().len())
            .finish_non_exhaustive()
    }
}

impl PrometheusMetricSink {
    /// Create a sink that registers metrics in `registry`.
    ///
    /// # Errors
    ///
    /// Returns an error if `wasm_callout_failures_total`, `wasm_plugin_failures_total`, or
    /// `wasm_guests_replaced_total` is already registered in `registry`, e.g. by another sink
    /// using the same registry.
    pub fn new(registry: Registry) -> prometheus::Result<Self> {
        let callout_failures = IntCounterVec::new(
            Opts::new(CALLOUT_FAILURES_NAME, CALLOUT_FAILURES_HELP),
            &[PLUGIN_LABEL, FAILURE_LABEL],
        )?;
        let plugin_failures = IntCounterVec::new(
            Opts::new(PLUGIN_FAILURES_NAME, PLUGIN_FAILURES_HELP),
            &[PLUGIN_LABEL, FAILURE_LABEL, OUTCOME_LABEL],
        )?;
        let guests_replaced = IntCounterVec::new(
            Opts::new(GUESTS_REPLACED_NAME, GUESTS_REPLACED_HELP),
            &[PLUGIN_LABEL],
        )?;
        registry.register(Box::new(callout_failures.clone()))?;
        registry.register(Box::new(plugin_failures.clone()))?;
        registry.register(Box::new(guests_replaced.clone()))?;
        Ok(PrometheusMetricSink {
            registry,
            callout_failures,
            plugin_failures,
            guests_replaced,
            families: Mutex::new(HashMap::new()),
            skipped_names: Mutex::new(HashSet::new()),
        })
    }

    /// Warn, once per metric name, that a plugin metric is not being published.
    ///
    /// `prometheus_name` is the name the metric would have been published under, or `None` if
    /// its name in the plugin is empty. Always returns `None` so the caller can return it from
    /// `register_metric`.
    fn skip_metric(
        &self,
        metric: &WasmMetric,
        prometheus_name: Option<&str>,
        reason: &str,
    ) -> Option<Box<dyn WasmMetricRecorder>> {
        if self.skipped_names.lock().insert(metric.name.clone()) {
            let (name, vm_id) = (&metric.name, &metric.vm_id);
            let published_as = match prometheus_name {
                Some(prometheus_name) => format!(" as {prometheus_name}"),
                None => String::new(),
            };
            warn!("wasm plugin metric {name} of VM {vm_id} not published to Prometheus{published_as}: {reason}, further occurrences are not logged");
        }
        None
    }
}

impl WasmMetricSink for PrometheusMetricSink {
    fn register_metric(&self, metric: &WasmMetric) -> Option<Box<dyn WasmMetricRecorder>> {
        let Some(prometheus_name) = prometheus_name(&metric.name) else {
            return self.skip_metric(metric, None, "name is empty");
        };
        let mut families = self.families.lock();
        let family = match families.entry(prometheus_name.clone()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let vector = match FamilyVector::new(entry.key(), metric.kind) {
                    Ok(vector) => vector,
                    Err(e) => {
                        let reason = format!("invalid name: {e}");
                        return self.skip_metric(metric, Some(&prometheus_name), &reason);
                    }
                };
                if let Err(e) = self.registry.register(vector.collector()) {
                    let reason = format!("registration failed: {e}");
                    return self.skip_metric(metric, Some(&prometheus_name), &reason);
                }
                entry.insert(Family {
                    name_in_plugin: metric.name.clone(),
                    vector,
                })
            }
        };
        if family.name_in_plugin != metric.name {
            let reason = "Prometheus name already taken by another plugin metric";
            return self.skip_metric(metric, Some(&prometheus_name), reason);
        }
        if family.vector.kind() != metric.kind {
            let reason = "already defined with a different kind";
            return self.skip_metric(metric, Some(&prometheus_name), reason);
        }
        Some(family.vector.recorder(&metric.vm_id))
    }

    fn callout_failed(&self, plugin_name: &str, failure: CalloutFailure) {
        self.callout_failures
            .with_label_values(&[plugin_name, failure.as_str()])
            .inc();
    }

    fn plugin_failed(&self, report: &PluginFailureReport<'_>) {
        let labels = [
            report.plugin_name,
            report.failure.as_str(),
            report.outcome.as_str(),
        ];
        self.plugin_failures.with_label_values(&labels).inc();
    }

    fn guest_replaced(&self, plugin_name: &str) {
        self.guests_replaced.with_label_values(&[plugin_name]).inc();
    }
}

/// Convert a plugin metric name into a valid Prometheus name.
///
/// Characters Prometheus does not allow are replaced with `_`, and a name starting with a digit
/// is prefixed with `_`. Returns `None` for an empty name.
fn prometheus_name(name: &str) -> Option<String> {
    if name.is_empty() {
        return None;
    }
    let mut prometheus_name = String::with_capacity(name.len() + 1);
    if name.starts_with(|c: char| c.is_ascii_digit()) {
        prometheus_name.push('_');
    }
    prometheus_name.extend(name.chars().map(|c| {
        if c.is_ascii_alphanumeric() || c == '_' || c == ':' {
            c
        } else {
            '_'
        }
    }));
    Some(prometheus_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FailureOutcome, PluginFailure};
    use prometheus::{Encoder, TextEncoder};

    fn metric(vm_id: &str, name: &str, kind: WasmMetricKind) -> WasmMetric {
        WasmMetric {
            vm_id: vm_id.to_string(),
            name: name.to_string(),
            kind,
        }
    }

    fn metrics_text(registry: &Registry) -> String {
        let mut buffer = Vec::new();
        TextEncoder::new()
            .encode(&registry.gather(), &mut buffer)
            .unwrap();
        String::from_utf8(buffer).unwrap()
    }

    #[test]
    fn plugin_metric_names_become_valid_prometheus_names() {
        let cases = [
            ("waf_filter.tx.total", Some("waf_filter_tx_total")),
            (
                "istio_reporter=.=destination;.;requests_total",
                Some("istio_reporter___destination___requests_total"),
            ),
            ("5xx_count", Some("_5xx_count")),
            ("", None),
        ];

        for (name, expected) in cases {
            assert_eq!(prometheus_name(name).as_deref(), expected, "{name}");
        }
    }

    #[test]
    fn vm_ids_share_family_with_vm_id_label() {
        let registry = Registry::new();
        let sink = PrometheusMetricSink::new(registry.clone()).unwrap();
        let first = sink.register_metric(&metric("a", "requests", WasmMetricKind::Counter));
        let second = sink.register_metric(&metric("b", "requests", WasmMetricKind::Counter));

        first.unwrap().add(2);
        second.unwrap().add(3);

        let text = metrics_text(&registry);
        assert!(text.contains("requests{vm_id=\"a\"} 2"), "{text}");
        assert!(text.contains("requests{vm_id=\"b\"} 3"), "{text}");
    }

    #[test]
    fn conflicting_kind_or_prometheus_name_is_not_published() {
        let sink = PrometheusMetricSink::new(Registry::new()).unwrap();
        sink.register_metric(&metric("a", "a.b", WasmMetricKind::Counter));

        let other_type = sink.register_metric(&metric("b", "a.b", WasmMetricKind::Gauge));
        let same_prometheus_name =
            sink.register_metric(&metric("a", "a_b", WasmMetricKind::Counter));

        assert!(other_type.is_none());
        assert!(same_prometheus_name.is_none());
    }

    #[test]
    fn second_sink_on_same_registry_fails() {
        let registry = Registry::new();
        let _first = PrometheusMetricSink::new(registry.clone()).unwrap();

        let second = PrometheusMetricSink::new(registry);

        assert!(second.is_err());
    }

    #[test]
    fn plugin_failure_and_replaced_guest_are_counted_by_label() {
        let registry = Registry::new();
        let sink = PrometheusMetricSink::new(registry.clone()).unwrap();
        let report = PluginFailureReport {
            plugin_name: "stats",
            failure: PluginFailure::WaitLimit,
            outcome: FailureOutcome::Skipped,
            callback: Some("proxy_on_request_headers"),
        };

        sink.plugin_failed(&report);
        sink.plugin_failed(&report);
        sink.guest_replaced("stats");

        let text = metrics_text(&registry);
        let failures = "wasm_plugin_failures_total\
            {failure=\"wait_limit\",outcome=\"skipped\",plugin=\"stats\"} 2";
        assert!(text.contains(failures), "{text}");
        let replaced = "wasm_guests_replaced_total{plugin=\"stats\"} 1";
        assert!(text.contains(replaced), "{text}");
    }

    #[test]
    fn failed_callout_is_counted_by_plugin_and_failure() {
        let registry = Registry::new();
        let sink = PrometheusMetricSink::new(registry.clone()).unwrap();

        sink.callout_failed("authz", CalloutFailure::ConnectTimeout);

        let text = metrics_text(&registry);
        let line = "wasm_callout_failures_total{failure=\"connect_timeout\",plugin=\"authz\"} 1";
        assert!(text.contains(line), "{text}");
    }
}
