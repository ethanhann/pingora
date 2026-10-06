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

//! Loaded guest

use super::events::{GuestAddress, RootCallbackEvent, RootCallbackLink};
use crate::callout::{AcceptedCallout, GrpcPending, GuestCalloutService};
use crate::root_callbacks::RootStream;
use proxy_wasm_host::abi::v0_2_1::{CallScope, CalloutId, ContextId, ContextState, Guest};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// A stream context whose guest returned `false` from `proxy_on_done` to keep it after its
/// request ended.
struct HeldContext {
    context: ContextId,
    /// Whether the context ended in `logging` and therefore still needs `proxy_on_log`.
    needs_on_log: bool,
}

/// A started guest with its plugin's root context and its callout service.
pub(crate) struct Loaded {
    pub(crate) plugin_name: Arc<str>,
    pub(crate) guest: Guest,
    pub(crate) root: ContextId,
    pub(crate) callout_service: Arc<GuestCalloutService>,
    root_callback_link: RootCallbackLink,
    held_contexts: Vec<HeldContext>,
    /// The slot's count of held contexts, kept in an atomic so that
    /// `WasmRuntime::held_contexts` can read it without locking the slot.
    held_context_count: Arc<AtomicUsize>,
}

impl Loaded {
    pub(crate) fn new(
        plugin_name: Arc<str>,
        guest: Guest,
        root: ContextId,
        callout_service: Arc<GuestCalloutService>,
        root_callback_link: RootCallbackLink,
        held_context_count: Arc<AtomicUsize>,
    ) -> Self {
        Loaded {
            plugin_name,
            guest,
            root,
            callout_service,
            root_callback_link,
            held_contexts: Vec::new(),
            held_context_count,
        }
    }

    pub(crate) fn address(&self) -> GuestAddress {
        self.root_callback_link.address
    }

    /// Report the effects of the last guest call to the root callback thread.
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

    /// Record that the guest is keeping `context` after its request ended.
    ///
    /// `callouts` are the ones the context sent while it was ending, and `streams` are its open
    /// gRPC streams, which move with it to the root callback thread.
    pub(crate) fn hold_context(
        &mut self,
        context: ContextId,
        needs_on_log: bool,
        callouts: Vec<AcceptedCallout>,
        streams: Vec<(CalloutId, GrpcPending)>,
    ) {
        // Any other callout the context still has open is failed, since the request that would
        // have received its result is gone
        let kept = |id: CalloutId| {
            callouts.iter().any(|callout| callout.id == id)
                || streams.iter().any(|(stream, _)| *stream == id)
        };
        let open_callouts_to_fail = self
            .guest
            .open_callouts()
            .into_iter()
            .filter(|open| open.caller == context && !kept(open.callout))
            .map(|open| (open.callout, open.kind))
            .collect::<Vec<_>>();
        self.held_contexts.push(HeldContext {
            context,
            needs_on_log,
        });
        self.held_context_count.fetch_add(1, Ordering::Relaxed);
        self.send_callouts_to_root_callbacks(context, callouts);
        if !streams.is_empty() {
            self.root_callback_link
                .send(RootCallbackEvent::StreamsToAdopt {
                    address: self.root_callback_link.address,
                    context,
                    streams,
                });
        }
        if !open_callouts_to_fail.is_empty() {
            self.root_callback_link
                .send(RootCallbackEvent::OpenCalloutsToFail {
                    address: self.root_callback_link.address,
                    context,
                    callouts: open_callouts_to_fail,
                });
        }
    }

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

    /// Run `body` for `context` outside of a request, under the root stream state.
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
