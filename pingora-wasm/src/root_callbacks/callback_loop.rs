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

//! The state of the root callback thread: what it waits for, and what is due.

use super::queue_registrations::QueueRegistrations;
use super::root_callouts::{FinishedCallout, RootCallouts};
use super::tick_schedule::TickSchedule;
use super::work::{Work, WorkOutcome};
use crate::callout::{AcceptedCallout, CalloutResult};
use crate::runtime::pool::events::{GuestAddress, RootCallbackEvent};
use crate::runtime::RuntimeInner;
use proxy_wasm_host::abi::v0_2_1::ContextId;
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedReceiver;

/// How long the thread waits before it tries again a slot that a request holds.
const BUSY_SLOT_RETRY: Duration = Duration::from_millis(1);

/// The state of the loop on the root callback thread.
#[derive(Default)]
pub(super) struct RootCallbackLoop {
    pub(super) ticks: TickSchedule,
    pub(super) queues: QueueRegistrations,
    pub(super) callouts: RootCallouts,
    /// Callouts that the next run starts, on the tokio runtime of the thread.
    callouts_to_start: Vec<(GuestAddress, ContextId, AcceptedCallout)>,
    ready_work: VecDeque<Work>,
    retries: Vec<(Instant, Work)>,
}

impl RootCallbackLoop {
    /// Wait until an event arrives, a callout ends, or a tick or a retry is due.
    ///
    /// Return `false` when the channel closed, because the runtime of the plugins dropped.
    pub(super) async fn wait_for_work(
        &mut self,
        events: &mut UnboundedReceiver<RootCallbackEvent>,
    ) -> bool {
        let due = self.next_due();
        tokio::select! {
            event = events.recv() => match event {
                Some(event) => self.accept(event),
                None => return false,
            },
            Some(finished) = self.callouts.next_finished() => {
                self.ready_work.push_back(Work::Delivery(finished));
            }
            _ = sleep_until_due(due), if due.is_some() => {}
        }
        // A guest call that registers a queue and puts an item on it sends the item first, so
        // the thread takes every waiting event before it runs work
        while let Ok(event) = events.try_recv() {
            self.accept(event);
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

    fn accept(&mut self, event: RootCallbackEvent) {
        match event {
            RootCallbackEvent::GuestChanged {
                address,
                root,
                changes,
            } => {
                if let Some(period) = changes.tick_periods.get(&root) {
                    self.ticks.set_period(address, *period, Instant::now());
                }
                for registration in changes.queues {
                    let queue = registration.queue;
                    let pending = self.queues.register(queue, address, registration.root);
                    let items = (0..pending).map(|_| Work::QueueItem(queue));
                    self.ready_work.extend(items);
                }
            }
            RootCallbackEvent::QueueItem(queue) => {
                self.ready_work.push_back(Work::QueueItem(queue));
            }
            RootCallbackEvent::CalloutsWithNoRequest {
                address,
                context,
                callouts,
            } => {
                let callouts = callouts.into_iter().map(|c| (address, context, c));
                self.callouts_to_start.extend(callouts);
            }
            RootCallbackEvent::CalloutsWithNoResult {
                address,
                context,
                callouts,
            } => {
                let failures = callouts.into_iter().map(|id| {
                    Work::Delivery(FinishedCallout {
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
                log_owed,
            } => self.ready_work.push_back(Work::EndHeldContext {
                address,
                context,
                log_owed,
            }),
        }
    }

    /// Start the callouts that arrived, and run each piece of work that is due.
    ///
    /// It runs outside the async context of the thread, inside the tokio runtime of the thread
    /// so that callouts start there.
    pub(super) fn run_due_work(&mut self, runtime: &RuntimeInner) {
        for (address, context, callout) in self.callouts_to_start.drain(..) {
            self.callouts.start(runtime, address, context, callout);
        }
        let now = Instant::now();
        let (due, waiting) = self.retries.drain(..).partition(|(at, _)| *at <= now);
        self.retries = waiting;
        for (_, work) in due {
            // A tick that waited for its slot is dropped when the guest set a new period since
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
                self.retries.push((Instant::now() + BUSY_SLOT_RETRY, work));
            }
        }
    }
}

async fn sleep_until_due(due: Option<Instant>) {
    if let Some(due) = due {
        tokio::time::sleep_until(due.into()).await;
    }
}
