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

//! Prometheus recorders

use super::WasmMetricRecorder;
use prometheus::{Histogram, IntCounter, IntGauge};

pub(super) struct CounterRecorder(pub(super) IntCounter);

impl WasmMetricRecorder for CounterRecorder {
    fn add(&self, delta: i64) {
        if let Ok(delta) = u64::try_from(delta) {
            self.0.inc_by(delta);
        }
    }
}

pub(super) struct GaugeRecorder(pub(super) IntGauge);

impl WasmMetricRecorder for GaugeRecorder {
    fn add(&self, delta: i64) {
        self.0.add(delta);
    }
}

pub(super) struct HistogramRecorder(pub(super) Histogram);

impl WasmMetricRecorder for HistogramRecorder {
    fn record(&self, value: u64) {
        // Prometheus histograms take f64, which is exact for integers up to 2^53
        #[allow(clippy::cast_precision_loss)]
        self.0.observe(value as f64);
    }
}
