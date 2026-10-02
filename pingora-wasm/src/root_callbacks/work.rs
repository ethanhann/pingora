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

//! Root callback work

use super::callback_loop::RootCallbackLoop;
use super::root_callouts::FinishedCallout;
use crate::observability::{PluginFailure, PluginFailureOutcome, PluginFailureReport};
use crate::root_callbacks::RootStream;
use crate::runtime::pool::events::GuestAddress;
use crate::runtime::pool::SlotLockAttempt;
use crate::runtime::RuntimeInner;
use log::{debug, warn};
use proxy_wasm_host::abi::v0_2_1::{CallScope, ContextId, GuestError, QueueId};
use std::time::Instant;

pub(super) enum Work {
    Tick(GuestAddress),
    QueueItem(QueueId),
    DeliverCalloutResult(FinishedCallout),
    EndHeldContext {
        address: GuestAddress,
        context: ContextId,
        needs_on_log: bool,
    },
}

pub(super) enum WorkOutcome {
    Done,
    SlotBusy,
}

enum GuestCallOutcome<R> {
    Ran(R),
    Failed,
    SlotBusy,
    /// The slot now holds a different guest, or none.
    GuestGone,
}

impl<R> GuestCallOutcome<R> {
    fn work_outcome(&self) -> WorkOutcome {
        match self {
            GuestCallOutcome::SlotBusy => WorkOutcome::SlotBusy,
            _ => WorkOutcome::Done,
        }
    }
}

#[derive(Clone, Copy)]
enum GuestCallContext {
    RootOfGuest,
    Given(ContextId),
}

impl RootCallbackLoop {
    pub(super) fn run_work(&mut self, runtime: &RuntimeInner, work: &Work) -> WorkOutcome {
        match work {
            Work::Tick(address) => self.run_tick(runtime, *address),
            Work::QueueItem(queue) => self.run_queue_item(runtime, *queue),
            Work::DeliverCalloutResult(finished) => self.deliver_callout_result(runtime, finished),
            Work::EndHeldContext {
                address,
                context,
                needs_on_log,
            } => self.end_held_context(runtime, *address, *context, *needs_on_log),
        }
    }

    fn run_tick(&mut self, runtime: &RuntimeInner, address: GuestAddress) -> WorkOutcome {
        let context = GuestCallContext::RootOfGuest;
        let tick = self.call_guest(runtime, address, context, "proxy_on_tick", |scope, root| {
            scope.on_tick(root)?;
            Ok(scope.guest().tick_period(root))
        });
        // The period starts when the tick has finished, so ticks of a slot never overlap and a
        // tick that ran late is not made up for
        if let GuestCallOutcome::Ran(period) = tick {
            self.ticks.set_period(address, period, Instant::now());
        }
        tick.work_outcome()
    }

    fn run_queue_item(&mut self, runtime: &RuntimeInner, queue: QueueId) -> WorkOutcome {
        loop {
            let Some(registrant) = self.queues.last_registrant(queue) else {
                let name = self.queues.name(queue);
                debug!("wasm queue {name} ({queue}): item kept pending, no live registrant");
                self.queues.add_pending_item(queue);
                return WorkOutcome::Done;
            };
            let context = GuestCallContext::Given(registrant.root);
            let callback_name = "proxy_on_queue_ready";
            let ready = self.call_guest(runtime, registrant.address, context, callback_name, {
                |scope, root| scope.on_queue_ready(root, queue)
            });
            match ready {
                GuestCallOutcome::GuestGone => self.queues.remove(queue, registrant),
                ready => return ready.work_outcome(),
            }
        }
    }

    fn deliver_callout_result(
        &mut self,
        runtime: &RuntimeInner,
        finished: &FinishedCallout,
    ) -> WorkOutcome {
        let context = GuestCallContext::Given(finished.context);
        let callback_name = "proxy_on_http_call_response";
        let delivery = self.call_guest(runtime, finished.address, context, callback_name, {
            |scope, context| {
                // The callout is no longer open once `proxy_on_delete` has ended its context
                if scope.guest().open_callout(finished.id).is_none() {
                    return Ok(());
                }
                let response = finished.result.as_http_call_response();
                scope.on_http_call_response(context, finished.id, response)
            }
        });
        delivery.work_outcome()
    }

    fn end_held_context(
        &mut self,
        runtime: &RuntimeInner,
        address: GuestAddress,
        context: ContextId,
        needs_on_log: bool,
    ) -> WorkOutcome {
        let call_context = GuestCallContext::Given(context);
        if needs_on_log {
            let logged = self.call_guest(runtime, address, call_context, "proxy_on_log", {
                |scope, context| scope.on_log(context)
            });
            match logged {
                GuestCallOutcome::SlotBusy => return WorkOutcome::SlotBusy,
                GuestCallOutcome::GuestGone => return WorkOutcome::Done,
                // A failing `proxy_on_log` does not skip `proxy_on_delete`
                GuestCallOutcome::Ran(()) | GuestCallOutcome::Failed => {}
            }
        }
        let deleted = self.call_guest(runtime, address, call_context, "proxy_on_delete", {
            |scope, context| scope.on_delete(context)
        });
        // Only `proxy_on_delete` is retried, so `proxy_on_log` never runs twice
        if let GuestCallOutcome::SlotBusy = deleted {
            self.retry_later(Work::EndHeldContext {
                address,
                context,
                needs_on_log: false,
            });
        }
        WorkOutcome::Done
    }

    fn call_guest<R>(
        &mut self,
        runtime: &RuntimeInner,
        address: GuestAddress,
        context: GuestCallContext,
        callback_name: &'static str,
        body: impl FnOnce(&mut CallScope<'_, RootStream>, ContextId) -> Result<R, GuestError>,
    ) -> GuestCallOutcome<R> {
        let pool = &runtime.pools[address.slot.pool_index];
        let mut guard = match pool.try_lock_guest(address.slot.slot_index, address.guest) {
            SlotLockAttempt::LockedGuest(guard) => guard,
            SlotLockAttempt::Busy => return GuestCallOutcome::SlotBusy,
            SlotLockAttempt::GuestGone => return GuestCallOutcome::GuestGone,
        };
        let Some(loaded) = guard.as_mut() else {
            return GuestCallOutcome::GuestGone;
        };
        let context = match context {
            GuestCallContext::RootOfGuest => loaded.root,
            GuestCallContext::Given(context) => context,
        };
        let (result, callouts) = loaded.run_root_callback(context, |scope| body(scope, context));
        match result {
            Ok(value) => {
                for callout in callouts {
                    self.callouts.start(runtime, address, context, callout);
                }
                GuestCallOutcome::Ran(value)
            }
            Err(e) => {
                warn!("wasm plugin {}: {callback_name} failed: {e}", pool.name);
                pool.replace_if_unusable(address.slot.slot_index, guard, &e);
                // Outside of a request there is nothing for a fail policy to act on, so the
                // outcome is always `Failed`
                runtime.metric_sink.plugin_failed(&PluginFailureReport {
                    plugin_name: &pool.name,
                    failure: PluginFailure::GuestError,
                    outcome: PluginFailureOutcome::Failed,
                    callback: Some(callback_name),
                });
                GuestCallOutcome::Failed
            }
        }
    }
}
