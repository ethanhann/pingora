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

//! Root callback loop state

use super::ending::EndState;
use super::queue_registrations::QueueRegistrations;
use super::root_callouts::{ArrivedDelivery, RootCallbackCallouts};
use super::tick_schedule::TickSchedule;
use super::work::{Work, WorkOutcome};
use crate::callout::grpc::status::CANCELLED;
use crate::callout::{
    AcceptedCallout, CalloutDelivery, GrpcCalloutEvent, HttpCalloutResult, PendingResult,
};
use crate::runtime::pool::events::{GuestAddress, RootCallbackEvent};
use crate::runtime::RuntimeInner;
use proxy_wasm_host::abi::v0_2_1::{CalloutKind, ContextId};
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedReceiver;

const BUSY_SLOT_RETRY_DELAY: Duration = Duration::from_millis(1);

#[derive(Default)]
pub(super) struct RootCallbackLoop {
    pub(super) ticks: TickSchedule,
    pub(super) queues: QueueRegistrations,
    pub(super) callouts: RootCallbackCallouts,
    /// Callouts to start on the next run, once the thread's tokio runtime is entered.
    callouts_to_start: Vec<(GuestAddress, ContextId, AcceptedCallout)>,
    ready_work: VecDeque<Work>,
    retries: Vec<(Instant, Work)>,
    pub(super) ending: Option<EndState>,
}

impl RootCallbackLoop {
    pub(super) async fn wait_for_work(
        &mut self,
        events: &mut UnboundedReceiver<RootCallbackEvent>,
    ) -> bool {
        let due = self.next_due();
        tokio::select! {
            event = events.recv() => match event {
                Some(event) => self.accept_event(event),
                // The channel is closed, so the `WasmRuntime` was dropped
                None => return false,
            },
            Some(arrived) = self.callouts.next_arrived() => {
                self.ready_work.push_back(Work::DeliverCallout(arrived));
            }
            _ = sleep_until_due(due), if due.is_some() => {}
        }
        // A guest call that both registers a queue and enqueues to it sends the item event ahead
        // of the registration. Drain everything already queued before running any work, so the
        // registration is recorded before the item is delivered.
        while let Ok(event) = events.try_recv() {
            self.accept_event(event);
        }
        true
    }

    fn next_due(&self) -> Option<Instant> {
        let retry = self.retries.iter().map(|(due, _)| *due);
        let recheck = self.ending.as_ref().and_then(EndState::recheck_at);
        retry.chain(self.ticks.next_due()).chain(recheck).min()
    }

    fn accept_event(&mut self, event: RootCallbackEvent) {
        let is_tick_or_queue = matches!(
            event,
            RootCallbackEvent::TicksOrQueuesChanged { .. } | RootCallbackEvent::QueueItem(_)
        );
        if is_tick_or_queue && self.ending.is_some() {
            return;
        }
        match event {
            RootCallbackEvent::End(progress) => {
                self.ticks = TickSchedule::default();
                self.queues = QueueRegistrations::default();
                let is_tick_or_queue =
                    |work: &Work| matches!(work, Work::Tick(_) | Work::QueueItem(_));
                self.ready_work.retain(|work| !is_tick_or_queue(work));
                self.retries.retain(|(_, work)| !is_tick_or_queue(work));
                self.ending.get_or_insert_with(|| EndState::new(progress));
            }
            RootCallbackEvent::TicksOrQueuesChanged {
                address,
                root,
                changes,
            } => {
                if let Some(period) = changes.tick_periods.get(&root) {
                    self.ticks.set_period(address, *period, Instant::now());
                    if period.is_none() {
                        let is_tick_of_address =
                            |work: &Work| matches!(work, Work::Tick(a) if *a == address);
                        self.retries.retain(|(_, work)| !is_tick_of_address(work));
                    }
                }
                for registration in changes.queues {
                    let queue = registration.queue;
                    let pending_item_count =
                        self.queues
                            .register(queue, &registration.name, address, registration.root);
                    let items = (0..pending_item_count).map(|_| Work::QueueItem(queue));
                    self.ready_work.extend(items);
                }
            }
            RootCallbackEvent::QueueItem(queue) => {
                self.ready_work.push_back(Work::QueueItem(queue));
            }
            RootCallbackEvent::CalloutsToStart {
                address,
                context,
                callouts,
            } => {
                let callouts = callouts.into_iter().map(|c| (address, context, c));
                self.callouts_to_start.extend(callouts);
            }
            RootCallbackEvent::OpenCalloutsToFail {
                address,
                context,
                callouts,
            } => {
                let failures = callouts.into_iter().map(|(id, kind)| {
                    let delivery = match kind {
                        CalloutKind::HttpCall => CalloutDelivery::Http(HttpCalloutResult::Failed),
                        _ => CalloutDelivery::Grpc(GrpcCalloutEvent::close(
                            CANCELLED,
                            "request ended",
                        )),
                    };
                    Work::DeliverCallout(ArrivedDelivery {
                        address,
                        context,
                        id,
                        delivery,
                    })
                });
                self.ready_work.extend(failures);
            }
            RootCallbackEvent::StreamsToAdopt {
                address,
                context,
                streams,
            } => {
                for (id, stream) in streams {
                    // Outside of a request every event reaches the plugin
                    stream.handle().set_keeps_events(true);
                    let pending = PendingResult::Grpc(stream);
                    self.callouts.add(address, context, id, pending);
                }
            }
            RootCallbackEvent::HeldContextDone {
                address,
                context,
                needs_on_log,
            } => self.ready_work.push_back(Work::EndHeldContext {
                address,
                context,
                needs_on_log,
            }),
        }
    }

    pub(super) fn retries_are_empty(&self) -> bool {
        self.retries.is_empty()
    }

    pub(super) fn retry_later(&mut self, work: Work) {
        self.retries
            .push((Instant::now() + BUSY_SLOT_RETRY_DELAY, work));
    }

    pub(super) fn run_due_work(&mut self, runtime: &RuntimeInner) {
        for (address, context, callout) in self.callouts_to_start.drain(..) {
            self.callouts.start(runtime, address, context, callout);
        }
        let now = Instant::now();
        let (due, waiting) = self.retries.drain(..).partition(|(at, _)| *at <= now);
        self.retries = waiting;
        // Retried work runs before work that arrived after it
        for (_, work) in due.into_iter().rev() {
            // A tick that was waiting for its slot is stale if the guest has set a new period
            // since, as the schedule already holds that slot's next tick
            if let Work::Tick(address) = &work {
                if self.ticks.has_next_tick(address.slot) {
                    continue;
                }
            }
            self.ready_work.push_front(work);
        }
        let ticks = self.ticks.take_due(now);
        self.ready_work.extend(ticks.into_iter().map(Work::Tick));
        while let Some(work) = self.ready_work.pop_front() {
            // The events of a gRPC stream must reach the plugin in order, so a delivery waits
            // behind an earlier one to the same guest that found the slot busy
            if self.waits_behind_retried_delivery(&work) {
                self.retry_later(work);
                continue;
            }
            if let WorkOutcome::SlotBusy = self.run_work(runtime, &work) {
                self.retry_later(work);
            }
        }
    }

    fn waits_behind_retried_delivery(&self, work: &Work) -> bool {
        let Work::DeliverCallout(arrived) = work else {
            return false;
        };
        self.retries.iter().any(|(_, retried)| {
            matches!(retried, Work::DeliverCallout(earlier) if earlier.address == arrived.address)
        })
    }
}

async fn sleep_until_due(due: Option<Instant>) {
    if let Some(due) = due {
        tokio::time::sleep_until(due.into()).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::root_callbacks::root_callouts::ArrivedDelivery;
    use crate::runtime::pool::events::SlotIndex;
    use crate::test_support::callouts::{authz_services, FixedSender};
    use crate::test_support::{plugin, wat_guest, RecordedGuestLogs, Wat};
    use crate::{WasmRuntime, WasmServices};
    use bytes::Bytes;
    use proxy_wasm_host::abi::v0_2_1::{CalloutId, Changes, GuestId};
    use std::sync::Arc;
    use tokio::sync::Notify;

    #[test]
    fn zero_period_drops_tick_waiting_for_slot() {
        let mut callback_loop = RootCallbackLoop::default();
        let address = GuestAddress {
            slot: SlotIndex {
                pool_index: 0,
                slot_index: 0,
            },
            guest: GuestId::next(),
        };
        let root = ContextId::try_from(1).unwrap();
        callback_loop
            .retries
            .push((Instant::now(), Work::Tick(address)));
        let mut changes = Changes::default();
        changes.tick_periods.insert(root, None);

        callback_loop.accept_event(RootCallbackEvent::TicksOrQueuesChanged {
            address,
            root,
            changes,
        });

        assert!(callback_loop.retries.is_empty());
    }

    type Delivery = Box<dyn Fn(&'static [u8]) -> Work>;

    /// Return a runtime whose root opened a gRPC stream, its logs, and a builder of message
    /// deliveries for that stream.
    fn runtime_with_open_stream(label: &str) -> (WasmRuntime, Arc<RecordedGuestLogs>, Delivery) {
        let wat = Wat {
            configure: "(call $open_grpc_stream) (i32.const 1)",
            grpc_receive: Some("(call $log_grpc_message (local.get 2))"),
            ..Wat::default()
        };
        let logs = Arc::new(RecordedGuestLogs::default());
        let services = WasmServices {
            log_sink: logs.clone(),
            ..authz_services()
        };
        let plugins = vec![plugin(label, wat_guest(label, wat), 1)];
        let sender = FixedSender::grpc_released_by(Arc::new(Notify::new()), Vec::new());
        let runtime = WasmRuntime::new_with_callout_sender(plugins, services, sender).unwrap();
        let (address, root) = {
            let guard = runtime.inner.pools[0].lock_slot(0);
            let loaded = guard.as_ref().unwrap();
            (loaded.address(), loaded.root)
        };
        let delivery = move |message: &'static [u8]| {
            Work::DeliverCallout(ArrivedDelivery {
                address,
                context: root,
                id: CalloutId::try_from(1).unwrap(),
                delivery: CalloutDelivery::Grpc(GrpcCalloutEvent::Message(Bytes::from_static(
                    message,
                ))),
            })
        };
        (runtime, logs, Box::new(delivery))
    }

    #[test]
    fn delivery_waits_behind_retried_delivery_to_same_guest() {
        let (runtime, logs, delivery) = runtime_with_open_stream("ordered");
        let mut callback_loop = RootCallbackLoop::default();
        let later = Instant::now() + Duration::from_secs(3600);
        callback_loop.retries.push((later, delivery(b"first")));
        callback_loop.ready_work.push_back(delivery(b"second"));

        callback_loop.run_due_work(&runtime.inner);

        assert!(logs.0.lock().is_empty());
        assert_eq!(callback_loop.retries.len(), 2);
    }

    #[test]
    fn due_retry_runs_before_later_delivery_to_same_guest() {
        let (runtime, logs, delivery) = runtime_with_open_stream("due-retry");
        let mut callback_loop = RootCallbackLoop::default();
        callback_loop
            .retries
            .push((Instant::now(), delivery(b"first")));
        callback_loop.ready_work.push_back(delivery(b"second"));

        callback_loop.run_due_work(&runtime.inner);

        assert_eq!(
            logs.0.lock()[..],
            ["first".to_string(), "second".to_string()]
        );
    }
}
