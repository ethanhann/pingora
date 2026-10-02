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
//!
//! The kinds of work the root callback loop runs, and the guest call each one makes.

use super::callback_loop::RootCallbackLoop;
use super::root_callouts::FinishedCallout;
use crate::root_callbacks::RootStream;
use crate::runtime::pool::events::GuestAddress;
use crate::runtime::pool::SlotLockAttempt;
use crate::runtime::RuntimeInner;
use log::{debug, warn};
use proxy_wasm_host::abi::v0_2_1::{CallScope, ContextId, GuestError, QueueId};
use std::time::Instant;

/// One item of work for the root callback thread.
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

/// The outcome of running one [Work] item.
pub(super) enum WorkOutcome {
    Done,
    /// The slot is locked by a request, so the work has to be retried later.
    SlotBusy,
}

/// The outcome of a guest call made from the root callback thread.
enum GuestCallOutcome<R> {
    Ran(R),
    /// The call failed. The guest has been replaced if the error left it unusable.
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

/// Which context a guest call from the root callback thread targets.
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
        if let GuestCallOutcome::Ran(period) = tick {
            self.ticks.set_period(address, period, Instant::now());
        }
        tick.work_outcome()
    }

    fn run_queue_item(&mut self, runtime: &RuntimeInner, queue: QueueId) -> WorkOutcome {
        loop {
            let Some(registrant) = self.queues.last_registrant(queue) else {
                debug!("wasm queue {queue}: item kept pending, no live registrant");
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

    /// Deliver a callout result to the guest through `proxy_on_http_call_response`.
    ///
    /// The result is silently dropped if the callout is no longer open, which is the case once
    /// `proxy_on_delete` has ended its context.
    fn deliver_callout_result(
        &mut self,
        runtime: &RuntimeInner,
        finished: &FinishedCallout,
    ) -> WorkOutcome {
        let context = GuestCallContext::Given(finished.context);
        let callback_name = "proxy_on_http_call_response";
        let delivery = self.call_guest(runtime, finished.address, context, callback_name, {
            |scope, context| {
                if scope.guest().open_callout(finished.id).is_none() {
                    return Ok(());
                }
                let response = finished.result.as_http_call_response();
                scope.on_http_call_response(context, finished.id, response)
            }
        });
        delivery.work_outcome()
    }

    /// End a context the guest kept after its request, once the guest is done with it.
    ///
    /// Runs `proxy_on_log` first if `needs_on_log` is set, then `proxy_on_delete`. A failing
    /// `proxy_on_log` does not skip `proxy_on_delete`, although the latter does nothing if the
    /// failure cost the guest its slot. Callouts the context still had open are closed with it,
    /// so their results are dropped when they arrive.
    ///
    /// If the slot is busy by the time `proxy_on_delete` is due, only that call is retried, so
    /// `proxy_on_log` never runs twice.
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
                GuestCallOutcome::Ran(()) | GuestCallOutcome::Failed => {}
            }
        }
        let deleted = self.call_guest(runtime, address, call_context, "proxy_on_delete", {
            |scope, context| scope.on_delete(context)
        });
        if let GuestCallOutcome::SlotBusy = deleted {
            self.retry_later(Work::EndHeldContext {
                address,
                context,
                needs_on_log: false,
            });
        }
        WorkOutcome::Done
    }

    /// Run `body` against the guest at `address` outside of a request.
    ///
    /// `body` is passed the resolved context id. The slot is locked without blocking, and nothing
    /// runs if it is busy or no longer holds this guest. Callouts the guest made during a
    /// successful call are started afterwards.
    ///
    /// A failed call is logged as a warning with `callback_name`, unless the failure removed the
    /// guest from its slot, which the pool logs on its own.
    fn call_guest<R>(
        &mut self,
        runtime: &RuntimeInner,
        address: GuestAddress,
        context: GuestCallContext,
        callback_name: &str,
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
                if !pool.replace_if_unusable(address.slot.slot_index, guard, &e) {
                    warn!("wasm plugin {}: {callback_name} failed: {e}", pool.name);
                }
                GuestCallOutcome::Failed
            }
        }
    }
}
