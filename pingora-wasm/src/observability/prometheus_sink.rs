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

use super::{CalloutFailure, WasmMetric, WasmMetricKind, WasmMetricRecorder, WasmMetricSink};
use log::warn;
use parking_lot::Mutex;
use prometheus::core::Collector;
use prometheus::{
    Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    Registry,
};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

const CALLOUT_FAILURES_NAME: &str = "wasm_callout_failures_total";
const CALLOUT_FAILURES_HELP: &str = "Total number of failed wasm plugin callouts";
const PLUGIN_METRIC_HELP: &str = "Metric defined by a wasm plugin";
const VM_ID_LABEL: &str = "vm_id";
const PLUGIN_LABEL: &str = "plugin";
const FAILURE_LABEL: &str = "failure";
// The ABI gives a plugin no way to choose buckets, so every histogram gets this fixed set
const HISTOGRAM_BUCKETS: [f64; 19] = [
    0.5, 1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0, 30000.0,
    60000.0, 300000.0, 600000.0, 1800000.0, 3600000.0,
];

/// A [WasmMetricSink] that publishes plugin metrics to a Prometheus registry.
///
/// Every plugin metric has a `vm_id` label. Characters Prometheus does not allow in a metric
/// name are replaced with `_`, so a plugin's `waf.tx.total` is published as `waf_tx_total`.
/// Failed callouts are counted in `wasm_callout_failures_total`, labeled with `plugin` and
/// `failure`.
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
            FamilyVector::Counter(vector) => {
                Box::new(CounterRecorder(vector.with_label_values(&[vm_id])))
            }
            FamilyVector::Gauge(vector) => {
                Box::new(GaugeRecorder(vector.with_label_values(&[vm_id])))
            }
            FamilyVector::Histogram(vector) => {
                Box::new(HistogramRecorder(vector.with_label_values(&[vm_id])))
            }
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
    /// Returns an error if `wasm_callout_failures_total` is already registered in `registry`,
    /// e.g. by another sink using the same registry.
    pub fn new(registry: Registry) -> prometheus::Result<Self> {
        let callout_failures = IntCounterVec::new(
            Opts::new(CALLOUT_FAILURES_NAME, CALLOUT_FAILURES_HELP),
            &[PLUGIN_LABEL, FAILURE_LABEL],
        )?;
        registry.register(Box::new(callout_failures.clone()))?;
        Ok(PrometheusMetricSink {
            registry,
            callout_failures,
            families: Mutex::new(HashMap::new()),
            skipped_names: Mutex::new(HashSet::new()),
        })
    }

    /// Warn, once per metric name, that a plugin metric is not being published.
    ///
    /// Always returns `None` so the caller can return it from `register_metric`.
    fn skip_metric(&self, name: &str, reason: &str) -> Option<Box<dyn WasmMetricRecorder>> {
        if self.skipped_names.lock().insert(name.to_string()) {
            warn!("wasm plugin metric {name} not published to Prometheus: {reason}");
        }
        None
    }
}

impl WasmMetricSink for PrometheusMetricSink {
    fn register_metric(&self, metric: &WasmMetric) -> Option<Box<dyn WasmMetricRecorder>> {
        let Some(prometheus_name) = prometheus_name(&metric.name) else {
            return self.skip_metric(&metric.name, "name is empty");
        };
        let mut families = self.families.lock();
        let family = match families.entry(prometheus_name) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let vector = match FamilyVector::new(entry.key(), metric.kind) {
                    Ok(vector) => vector,
                    Err(e) => {
                        let reason = format!("invalid name: {e}");
                        return self.skip_metric(&metric.name, &reason);
                    }
                };
                if let Err(e) = self.registry.register(vector.collector()) {
                    let reason = format!("registration failed: {e}");
                    return self.skip_metric(&metric.name, &reason);
                }
                entry.insert(Family {
                    name_in_plugin: metric.name.clone(),
                    vector,
                })
            }
        };
        if family.name_in_plugin != metric.name {
            return self.skip_metric(
                &metric.name,
                "Prometheus name already taken by another plugin metric",
            );
        }
        if family.vector.kind() != metric.kind {
            return self.skip_metric(&metric.name, "already defined with a different kind");
        }
        Some(family.vector.recorder(&metric.vm_id))
    }

    fn callout_failed(&self, plugin_name: &str, failure: CalloutFailure) {
        self.callout_failures
            .with_label_values(&[plugin_name, failure.as_str()])
            .inc();
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

struct CounterRecorder(IntCounter);

impl WasmMetricRecorder for CounterRecorder {
    fn add(&self, delta: i64) {
        if let Ok(delta) = u64::try_from(delta) {
            self.0.inc_by(delta);
        }
    }
}

struct GaugeRecorder(IntGauge);

impl WasmMetricRecorder for GaugeRecorder {
    fn add(&self, delta: i64) {
        self.0.add(delta);
    }
}

struct HistogramRecorder(Histogram);

impl WasmMetricRecorder for HistogramRecorder {
    fn record(&self, value: u64) {
        // Prometheus histograms take f64, which is exact for integers up to 2^53
        #[allow(clippy::cast_precision_loss)]
        self.0.observe(value as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn failed_callout_is_counted_by_plugin_and_failure() {
        let registry = Registry::new();
        let sink = PrometheusMetricSink::new(registry.clone()).unwrap();

        sink.callout_failed("authz", CalloutFailure::ConnectTimeout);

        let text = metrics_text(&registry);
        let line = "wasm_callout_failures_total{failure=\"connect_timeout\",plugin=\"authz\"} 1";
        assert!(text.contains(line), "{text}");
    }
}
