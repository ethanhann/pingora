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

use super::queue_registrations::QueueRegistrations;
use super::root_callouts::{FinishedCallout, RootCallbackCallouts};
use super::tick_schedule::TickSchedule;
use super::work::{Work, WorkOutcome};
use crate::callout::{AcceptedCallout, CalloutResult};
use crate::runtime::pool::events::{GuestAddress, RootCallbackEvent};
use crate::runtime::RuntimeInner;
use proxy_wasm_host::abi::v0_2_1::ContextId;
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
            Some(finished) = self.callouts.next_finished() => {
                self.ready_work.push_back(Work::DeliverCalloutResult(finished));
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
        let retry = self.retries.iter().map(|(due, _)| *due).min();
        match (retry, self.ticks.next_due()) {
            (Some(retry), Some(tick)) => Some(retry.min(tick)),
            (retry, tick) => retry.or(tick),
        }
    }

    fn accept_event(&mut self, event: RootCallbackEvent) {
        match event {
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
                let failures = callouts.into_iter().map(|id| {
                    Work::DeliverCalloutResult(FinishedCallout {
                        address,
                        context,
                        id,
                        result: CalloutResult::Failed,
                    })
                });
                self.ready_work.extend(failures);
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
        for (_, work) in due {
            // A tick that was waiting for its slot is stale if the guest has set a new period
            // since, as the schedule already holds that slot's next tick
            if let Work::Tick(address) = &work {
                if self.ticks.has_next_tick(address.slot) {
                    continue;
                }
            }
            self.ready_work.push_back(work);
        }
        let ticks = self.ticks.take_due(now);
        self.ready_work.extend(ticks.into_iter().map(Work::Tick));
        while let Some(work) = self.ready_work.pop_front() {
            if let WorkOutcome::SlotBusy = self.run_work(runtime, &work) {
                self.retry_later(work);
            }
        }
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
    use crate::runtime::pool::events::SlotIndex;
    use proxy_wasm_host::abi::v0_2_1::{Changes, GuestId};

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
}
