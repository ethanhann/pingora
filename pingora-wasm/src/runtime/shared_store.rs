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

//! The store of shared data, queues, and metrics that all plugins of a runtime use.

use crate::metrics::{WasmMetric, WasmMetricKind, WasmMetricRecorder, WasmMetricSink};
use parking_lot::Mutex;
use proxy_wasm_host::abi::v0_2_1::types::{MetricType, Status};
use proxy_wasm_host::abi::v0_2_1::{
    InMemoryStore, InMemoryStoreLimits, Invocation, MetricId, QueueEnqueued, QueueId,
    SharedServices, SharedValue,
};
use std::collections::HashMap;
use std::sync::Arc;

/// The shared services of a runtime.
///
/// Shared data and queues stay in an [InMemoryStore], and the metrics live here instead.
/// `proxy_record_metric` adds to a counter, as in Envoy, where the store would replace the
/// value, and each change also goes to the metric sink.
pub(crate) struct SharedStore {
    store: InMemoryStore,
    metrics: Mutex<Metrics>,
    metric_limit: usize,
    sink: Arc<dyn WasmMetricSink>,
}

#[derive(Default)]
struct Metrics {
    ids: HashMap<(Vec<u8>, Vec<u8>), MetricId>,
    entries: HashMap<MetricId, MetricEntry>,
    last_id: u32,
}

struct MetricEntry {
    kind: MetricType,
    value: u64,
    recorder: Option<Arc<dyn WasmMetricRecorder>>,
}

impl SharedStore {
    /// Create a store with `limits`, which calls `enqueue_observer` for each queue item and
    /// sends the metrics to `sink`.
    pub(crate) fn new(
        limits: InMemoryStoreLimits,
        enqueue_observer: Arc<dyn Fn(QueueEnqueued<'_>) + Send + Sync>,
        sink: Arc<dyn WasmMetricSink>,
    ) -> Self {
        SharedStore {
            metric_limit: limits.metrics(),
            store: InMemoryStore::new()
                .with_limits(limits)
                .with_enqueue_observer(enqueue_observer),
            metrics: Mutex::new(Metrics::default()),
            sink,
        }
    }

    /// Change the value of `metric` with `change` under the lock, and send what `change`
    /// returns to the recorder of the metric after the lock is released.
    fn change_metric(
        &self,
        metric: MetricId,
        change: impl FnOnce(&mut MetricEntry) -> Result<RecorderCall, Status>,
    ) -> Result<(), Status> {
        let (call, recorder) = {
            let mut metrics = self.metrics.lock();
            let entry = metrics.entries.get_mut(&metric).ok_or(Status::NotFound)?;
            (change(entry)?, entry.recorder.clone())
        };
        if let Some(recorder) = recorder {
            match call {
                RecorderCall::Add(0) => {}
                RecorderCall::Add(delta) => recorder.add(delta),
                RecorderCall::Record(value) => recorder.record(value),
            }
        }
        Ok(())
    }
}

// A recorder can be shared with a runtime that replaces this one, so a gauge takes back its
// value, and the gauge of a sink is the sum of the runtimes that are alive, as in Envoy
impl Drop for SharedStore {
    fn drop(&mut self) {
        let metrics = self.metrics.get_mut();
        for entry in metrics.entries.values() {
            if let (MetricType::Gauge, Some(recorder)) = (entry.kind, &entry.recorder) {
                recorder.add(saturating_delta(entry.value, 0));
            }
        }
    }
}

enum RecorderCall {
    Add(i64),
    Record(u64),
}

fn saturating_delta(from: u64, to: u64) -> i64 {
    let delta = i128::from(to) - i128::from(from);
    i64::try_from(delta).unwrap_or(if delta < 0 { i64::MIN } else { i64::MAX })
}

fn metric_kind(kind: MetricType) -> WasmMetricKind {
    match kind {
        MetricType::Counter => WasmMetricKind::Counter,
        MetricType::Gauge => WasmMetricKind::Gauge,
        MetricType::Histogram => WasmMetricKind::Histogram,
    }
}

impl SharedServices for SharedStore {
    fn get_shared_data(
        &self,
        call: Invocation,
        vm_id: &[u8],
        key: &[u8],
    ) -> Result<SharedValue, Status> {
        self.store.get_shared_data(call, vm_id, key)
    }

    fn set_shared_data(
        &self,
        call: Invocation,
        vm_id: &[u8],
        key: &[u8],
        value: &[u8],
        cas: Option<u32>,
    ) -> Result<(), Status> {
        self.store.set_shared_data(call, vm_id, key, value, cas)
    }

    fn register_shared_queue(
        &self,
        call: Invocation,
        vm_id: &[u8],
        name: &[u8],
    ) -> Result<QueueId, Status> {
        self.store.register_shared_queue(call, vm_id, name)
    }

    fn resolve_shared_queue(
        &self,
        call: Invocation,
        vm_id: &[u8],
        name: &[u8],
    ) -> Result<QueueId, Status> {
        self.store.resolve_shared_queue(call, vm_id, name)
    }

    fn enqueue_shared_queue(
        &self,
        call: Invocation,
        queue: QueueId,
        value: &[u8],
    ) -> Result<(), Status> {
        self.store.enqueue_shared_queue(call, queue, value)
    }

    fn dequeue_shared_queue(&self, call: Invocation, queue: QueueId) -> Result<Vec<u8>, Status> {
        self.store.dequeue_shared_queue(call, queue)
    }

    fn define_metric(
        &self,
        _call: Invocation,
        vm_id: &[u8],
        kind: MetricType,
        name: &[u8],
    ) -> Result<MetricId, Status> {
        let mut metrics = self.metrics.lock();
        let key = (vm_id.to_vec(), name.to_vec());
        if let Some(id) = metrics.ids.get(&key).copied() {
            let entry = metrics.entries.get(&id).ok_or(Status::InternalFailure)?;
            return if entry.kind == kind {
                Ok(id)
            } else {
                Err(Status::BadArgument)
            };
        }
        if metrics.ids.len() >= self.metric_limit {
            return Err(Status::InternalFailure);
        }
        let next = metrics
            .last_id
            .checked_add(1)
            .ok_or(Status::InternalFailure)?;
        let id = MetricId::try_from(next).map_err(|_| Status::InternalFailure)?;
        metrics.last_id = next;
        let metric = WasmMetric {
            vm_id: String::from_utf8_lossy(vm_id).into_owned(),
            name: String::from_utf8_lossy(name).into_owned(),
            kind: metric_kind(kind),
        };
        let recorder = self.sink.metric_defined(&metric).map(Arc::from);
        metrics.ids.insert(key, id);
        metrics.entries.insert(
            id,
            MetricEntry {
                kind,
                value: 0,
                recorder,
            },
        );
        Ok(id)
    }

    fn record_metric(&self, _call: Invocation, metric: MetricId, value: u64) -> Result<(), Status> {
        self.change_metric(metric, |entry| match entry.kind {
            MetricType::Counter => {
                let before = entry.value;
                entry.value = entry.value.saturating_add(value);
                Ok(RecorderCall::Add(saturating_delta(before, entry.value)))
            }
            MetricType::Gauge => {
                let before = entry.value;
                entry.value = value;
                Ok(RecorderCall::Add(saturating_delta(before, value)))
            }
            MetricType::Histogram => Ok(RecorderCall::Record(value)),
        })
    }

    fn increment_metric(
        &self,
        _call: Invocation,
        metric: MetricId,
        delta: i64,
    ) -> Result<(), Status> {
        self.change_metric(metric, |entry| {
            let before = entry.value;
            entry.value = match entry.kind {
                MetricType::Counter if delta <= 0 => return Err(Status::BadArgument),
                MetricType::Counter => entry.value.saturating_add(delta.unsigned_abs()),
                MetricType::Gauge => entry
                    .value
                    .checked_add_signed(delta)
                    .ok_or(Status::BadArgument)?,
                MetricType::Histogram => return Err(Status::BadArgument),
            };
            Ok(RecorderCall::Add(saturating_delta(before, entry.value)))
        })
    }

    fn get_metric(&self, _call: Invocation, metric: MetricId) -> Result<u64, Status> {
        let metrics = self.metrics.lock();
        let entry = metrics.entries.get(&metric).ok_or(Status::NotFound)?;
        match entry.kind {
            MetricType::Histogram => Err(Status::BadArgument),
            _ => Ok(entry.value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proxy_wasm_host::abi::v0_2_1::{ContextId, GuestId};

    /// A sink that records each metric definition and each change.
    #[derive(Default)]
    struct RecordingSink {
        events: Arc<Mutex<Vec<String>>>,
    }

    struct RecordingRecorder {
        name: String,
        events: Arc<Mutex<Vec<String>>>,
    }

    impl WasmMetricSink for RecordingSink {
        fn metric_defined(&self, metric: &WasmMetric) -> Option<Box<dyn WasmMetricRecorder>> {
            let name = format!("{}/{}", metric.vm_id, metric.name);
            self.events.lock().push(format!("define {name}"));
            Some(Box::new(RecordingRecorder {
                name,
                events: self.events.clone(),
            }))
        }
    }

    impl WasmMetricRecorder for RecordingRecorder {
        fn add(&self, delta: i64) {
            self.events
                .lock()
                .push(format!("add {} {delta}", self.name));
        }

        fn record(&self, value: u64) {
            self.events
                .lock()
                .push(format!("record {} {value}", self.name));
        }
    }

    fn call() -> Invocation {
        Invocation::new(GuestId::next(), ContextId::try_from(1).unwrap())
    }

    fn store() -> (SharedStore, Arc<Mutex<Vec<String>>>) {
        let sink = RecordingSink::default();
        let events = sink.events.clone();
        (
            SharedStore::new(
                InMemoryStoreLimits::default(),
                Arc::new(|_| {}),
                Arc::new(sink),
            ),
            events,
        )
    }

    #[test]
    fn a_recorded_counter_adds_and_a_gauge_sends_its_delta() {
        let cases = [
            (MetricType::Counter, 7, ["add vm/m 2", "add vm/m 5"]),
            (MetricType::Gauge, 5, ["add vm/m 2", "add vm/m 3"]),
        ];

        for (kind, expected, sent) in cases {
            let (store, events) = store();
            let metric = store.define_metric(call(), b"vm", kind, b"m").unwrap();
            store.record_metric(call(), metric, 2).unwrap();

            store.record_metric(call(), metric, 5).unwrap();

            assert_eq!(store.get_metric(call(), metric), Ok(expected), "{kind:?}");
            assert_eq!(events.lock()[1..], sent, "{kind:?}");
        }
    }

    #[test]
    fn a_histogram_records_values_and_has_no_value_to_read() {
        let (store, events) = store();
        let histogram = store
            .define_metric(call(), b"vm", MetricType::Histogram, b"latency")
            .unwrap();

        let recorded = store.record_metric(call(), histogram, 12);
        let incremented = store.increment_metric(call(), histogram, 1);

        assert_eq!(recorded, Ok(()));
        assert_eq!(incremented, Err(Status::BadArgument));
        assert_eq!(
            store.get_metric(call(), histogram),
            Err(Status::BadArgument)
        );
        assert_eq!(events.lock().last().unwrap(), "record vm/latency 12");
    }

    #[test]
    fn plugins_with_one_vm_id_share_a_metric_that_is_defined_once() {
        let (store, events) = store();
        let first = store.define_metric(call(), b"vm", MetricType::Counter, b"requests");
        let second = store.define_metric(call(), b"vm", MetricType::Counter, b"requests");
        let other_vm = store.define_metric(call(), b"other", MetricType::Counter, b"requests");
        let other_kind = store.define_metric(call(), b"vm", MetricType::Gauge, b"requests");

        assert_eq!(first, second);
        assert_ne!(first, other_vm);
        assert_eq!(other_kind, Err(Status::BadArgument));
        assert_eq!(events.lock().len(), 2);
    }

    #[test]
    fn a_counter_refuses_an_increment_that_is_not_positive() {
        let (store, _events) = store();
        let counter = store
            .define_metric(call(), b"vm", MetricType::Counter, b"c")
            .unwrap();

        let refused = [-1, 0].map(|delta| store.increment_metric(call(), counter, delta));

        assert_eq!(
            refused,
            [Err(Status::BadArgument), Err(Status::BadArgument)]
        );
    }

    #[test]
    fn a_gauge_delta_over_the_range_of_i64_saturates() {
        let (store, events) = store();
        let gauge = store
            .define_metric(call(), b"vm", MetricType::Gauge, b"g")
            .unwrap();

        store.record_metric(call(), gauge, u64::MAX).unwrap();

        assert_eq!(
            *events.lock().last().unwrap(),
            format!("add vm/g {}", i64::MAX)
        );
    }

    #[test]
    fn a_dropped_store_takes_back_the_value_of_each_gauge() {
        let (store, events) = store();
        let gauge = store
            .define_metric(call(), b"vm", MetricType::Gauge, b"open")
            .unwrap();
        store.record_metric(call(), gauge, 5).unwrap();

        drop(store);

        assert_eq!(events.lock().last().unwrap(), "add vm/open -5");
    }
}
