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

//! A started guest in its slot, and what it reports to the root callback thread after each
//! call.

use super::events::{RootCallbackEvent, RootCallbackLink};
use crate::callout::{AcceptedCallout, GuestCalloutService};
use crate::stream::RootStream;
use proxy_wasm_host::abi::v0_2_1::{CallScope, ContextId, ContextState, Guest};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// A stream context that the guest holds after its request ended, because its
/// `proxy_on_done` returned `false`.
struct HeldContext {
    context: ContextId,
    /// Whether the context ended in `logging`, which means that it still owes `proxy_on_log`.
    needs_on_log: bool,
}

/// A started guest, the root context of its plugin, and its callout service.
pub(crate) struct Loaded {
    pub(crate) guest: Guest,
    pub(crate) root: ContextId,
    pub(crate) callout_service: Arc<GuestCalloutService>,
    root_callback_link: RootCallbackLink,
    held_contexts: Vec<HeldContext>,
    /// The number of held contexts of the slot, which `held_contexts` of the runtime reads
    /// with no lock.
    held_context_count: Arc<AtomicUsize>,
}

impl Loaded {
    pub(crate) fn new(
        guest: Guest,
        root: ContextId,
        callout_service: Arc<GuestCalloutService>,
        root_callback_link: RootCallbackLink,
        held_context_count: Arc<AtomicUsize>,
    ) -> Self {
        Loaded {
            guest,
            root,
            callout_service,
            root_callback_link,
            held_contexts: Vec::new(),
            held_context_count,
        }
    }

    /// Report to the root callback thread what the last guest call changed.
    ///
    /// The report has the tick period and the queues of the root, and each held context that the
    /// guest finished with `proxy_done`.
    pub(crate) fn report_to_root_callbacks(&mut self) {
        let changes = self.guest.take_changes();
        if !changes.is_empty() {
            self.root_callback_link
                .send(RootCallbackEvent::TicksOrQueuesChanged {
                    address: self.root_callback_link.address,
                    root: self.root,
                    changes,
                });
        }
        let mut index = 0;
        while index < self.held_contexts.len() {
            let context = self.held_contexts[index].context;
            if self.guest.context_state(context) != Some(ContextState::Done) {
                index += 1;
                continue;
            }
            let done_context = self.held_contexts.swap_remove(index);
            self.held_context_count.fetch_sub(1, Ordering::Relaxed);
            self.root_callback_link
                .send(RootCallbackEvent::HeldContextDone {
                    address: self.root_callback_link.address,
                    context: done_context.context,
                    needs_on_log: done_context.needs_on_log,
                });
        }
    }

    /// Record that the guest holds `context` after its request ended.
    ///
    /// `callouts` are the callouts that the context sent while it ended, and the root callback
    /// thread delivers their results. Every other callout that the context still has open gets
    /// a failure, because the request that sent it no longer reads its result.
    pub(crate) fn hold_context(
        &mut self,
        context: ContextId,
        needs_on_log: bool,
        callouts: Vec<AcceptedCallout>,
    ) {
        let open_callouts_to_fail = self
            .guest
            .open_callouts()
            .into_iter()
            .filter(|open| open.caller == context)
            .map(|open| open.callout)
            .filter(|id| callouts.iter().all(|callout| callout.id != *id))
            .collect::<Vec<_>>();
        self.held_contexts.push(HeldContext {
            context,
            needs_on_log,
        });
        self.held_context_count.fetch_add(1, Ordering::Relaxed);
        self.send_callouts_to_root_callbacks(context, callouts);
        if !open_callouts_to_fail.is_empty() {
            self.root_callback_link
                .send(RootCallbackEvent::OpenCalloutsToFail {
                    address: self.root_callback_link.address,
                    context,
                    callouts: open_callouts_to_fail,
                });
        }
    }

    /// Send the callouts that no request waits for to the root callback thread.
    ///
    /// The thread delivers their results to `context`.
    pub(crate) fn send_callouts_to_root_callbacks(
        &self,
        context: ContextId,
        callouts: Vec<AcceptedCallout>,
    ) {
        if !callouts.is_empty() {
            self.root_callback_link
                .send(RootCallbackEvent::CalloutsToStart {
                    address: self.root_callback_link.address,
                    context,
                    callouts,
                });
        }
    }

    /// Run `body` for `context` with no request, under the root stream state.
    ///
    /// Return the result with the callouts that the guest sent from `context`, after the changes
    /// of the call are reported to the root callback thread.
    pub(crate) fn run_root_callback<R>(
        &mut self,
        context: ContextId,
        body: impl FnOnce(&mut CallScope<'_, RootStream>) -> R,
    ) -> (R, Vec<AcceptedCallout>) {
        let service = self.callout_service.clone();
        let stream = RootStream::new(self.root_callback_link.plugin.clone());
        let guest = &mut self.guest;
        let result_and_callouts = service.record_callouts(context, || {
            let mut scope = guest.enter(stream);
            let result = body(&mut scope);
            let _root_stream = scope.finish();
            result
        });
        self.report_to_root_callbacks();
        result_and_callouts
    }
}
