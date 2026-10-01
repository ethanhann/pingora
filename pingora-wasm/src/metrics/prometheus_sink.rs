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

const CALLOUT_FAILURES: &str = "wasm_callout_failures_total";
const CALLOUT_FAILURES_HELP: &str = "Callouts of wasm plugins that failed";
const PLUGIN_METRIC_HELP: &str = "A metric of a wasm plugin";
const VM_ID_LABEL: &str = "vm_id";
const PLUGIN_LABEL: &str = "plugin";
const FAILURE_LABEL: &str = "failure";
// The default buckets of Envoy, so a plugin written for Envoy gets the same distribution
const HISTOGRAM_BUCKETS: [f64; 19] = [
    0.5, 1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0, 30000.0,
    60000.0, 300000.0, 600000.0, 1800000.0, 3600000.0,
];

/// A [WasmMetricSink] that registers the metrics of plugins in a Prometheus registry.
///
/// Each plugin metric gets the label `vm_id`. A character that Prometheus does not permit in a
/// name becomes `_`, so the name `waf.tx.total` of a plugin is `waf_tx_total` in Prometheus.
/// Failed callouts are counted in `wasm_callout_failures_total`, with the labels `plugin` and
/// `failure`.
///
/// Create one sink and pass it to each runtime you build, also when you reload plugins, because
/// a registry accepts each name once.
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

/// The Prometheus vector of one cleaned name, and the plugin metric name it was created for.
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
    /// An error when `registry` already has `wasm_callout_failures_total`, for example from a
    /// second sink on the same registry.
    pub fn new(registry: Registry) -> prometheus::Result<Self> {
        let callout_failures = IntCounterVec::new(
            Opts::new(CALLOUT_FAILURES, CALLOUT_FAILURES_HELP),
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

    /// Log once that the metric `name` of a plugin is not published, and why.
    fn skip_metric(&self, name: &str, reason: &str) -> Option<Box<dyn WasmMetricRecorder>> {
        if self.skipped_names.lock().insert(name.to_string()) {
            warn!("the wasm plugin metric {name} is not published in Prometheus, because {reason}");
        }
        None
    }
}

impl WasmMetricSink for PrometheusMetricSink {
    fn metric_defined(&self, metric: &WasmMetric) -> Option<Box<dyn WasmMetricRecorder>> {
        let Some(clean_name) = prometheus_name(&metric.name) else {
            return self.skip_metric(&metric.name, "its name is empty");
        };
        let mut families = self.families.lock();
        let family = match families.entry(clean_name) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let vector = match FamilyVector::new(entry.key(), metric.kind) {
                    Ok(vector) => vector,
                    Err(e) => return self.skip_metric(&metric.name, &e.to_string()),
                };
                if let Err(e) = self.registry.register(vector.collector()) {
                    return self.skip_metric(&metric.name, &e.to_string());
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
                "another plugin metric maps to the same Prometheus name",
            );
        }
        if family.vector.kind() != metric.kind {
            return self.skip_metric(&metric.name, "a plugin defined it with another type");
        }
        Some(family.vector.recorder(&metric.vm_id))
    }

    fn callout_failed(&self, plugin: &str, failure: CalloutFailure) {
        self.callout_failures
            .with_label_values(&[plugin, failure.as_str()])
            .inc();
    }
}

/// Return `name` with each character that Prometheus does not permit replaced by `_`, and with
/// `_` before a leading digit. Return `None` for an empty name.
fn prometheus_name(name: &str) -> Option<String> {
    if name.is_empty() {
        return None;
    }
    let mut clean = String::with_capacity(name.len() + 1);
    if name.starts_with(|c: char| c.is_ascii_digit()) {
        clean.push('_');
    }
    clean.extend(name.chars().map(|c| {
        if c.is_ascii_alphanumeric() || c == '_' || c == ':' {
            c
        } else {
            '_'
        }
    }));
    Some(clean)
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
        // A histogram of Prometheus holds f64 values, which lose precision only above 2^53
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

    fn output(registry: &Registry) -> String {
        let mut buffer = Vec::new();
        TextEncoder::new()
            .encode(&registry.gather(), &mut buffer)
            .unwrap();
        String::from_utf8(buffer).unwrap()
    }

    #[test]
    fn the_names_of_plugins_become_valid_prometheus_names() {
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
    fn two_vm_ids_on_one_sink_share_a_family_with_the_vm_id_as_label() {
        let registry = Registry::new();
        let sink = PrometheusMetricSink::new(registry.clone()).unwrap();
        let first = sink.metric_defined(&metric("a", "requests", WasmMetricKind::Counter));
        let second = sink.metric_defined(&metric("b", "requests", WasmMetricKind::Counter));

        first.unwrap().add(2);
        second.unwrap().add(3);

        let text = output(&registry);
        assert!(text.contains("requests{vm_id=\"a\"} 2"), "{text}");
        assert!(text.contains("requests{vm_id=\"b\"} 3"), "{text}");
    }

    #[test]
    fn a_name_with_another_type_or_the_same_prometheus_name_is_not_published() {
        let sink = PrometheusMetricSink::new(Registry::new()).unwrap();
        sink.metric_defined(&metric("a", "a.b", WasmMetricKind::Counter));

        let other_type = sink.metric_defined(&metric("b", "a.b", WasmMetricKind::Gauge));
        let same_clean_name = sink.metric_defined(&metric("a", "a_b", WasmMetricKind::Counter));

        assert!(other_type.is_none());
        assert!(same_clean_name.is_none());
    }

    #[test]
    fn a_second_sink_on_one_registry_is_an_error() {
        let registry = Registry::new();
        let _first = PrometheusMetricSink::new(registry.clone()).unwrap();

        let second = PrometheusMetricSink::new(registry);

        assert!(second.is_err());
    }

    #[test]
    fn a_failed_callout_counts_with_its_plugin_and_failure() {
        let registry = Registry::new();
        let sink = PrometheusMetricSink::new(registry.clone()).unwrap();

        sink.callout_failed("authz", CalloutFailure::ConnectTimeout);

        let text = output(&registry);
        let line = "wasm_callout_failures_total{failure=\"connect_timeout\",plugin=\"authz\"} 1";
        assert!(text.contains(line), "{text}");
    }
}
