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

//! Root callback events
//!
//! Events sent to the root callback thread about the effects of guest calls, and the types that
//! identify the guest an event is about.

use crate::callout::AcceptedCallout;
use crate::root_callbacks::RootCallbackPluginState;
use proxy_wasm_host::abi::v0_2_1::{CalloutId, Changes, ContextId, GuestId, QueueId};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;

/// A slot, identified by its pool and its index within that pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SlotIndex {
    pub(crate) pool_index: usize,
    pub(crate) slot_index: usize,
}

/// A guest together with the slot it runs in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct GuestAddress {
    pub(crate) slot: SlotIndex,
    pub(crate) guest: GuestId,
}

/// An event sent to the root callback thread.
pub(crate) enum RootCallbackEvent {
    /// A guest call changed the root context's tick period or registered a queue.
    TicksOrQueuesChanged {
        address: GuestAddress,
        root: ContextId,
        changes: Changes,
    },
    /// An item was enqueued on a shared queue.
    QueueItem(QueueId),
    /// Callouts to start whose results are delivered to `context` outside of a request. They
    /// were sent by a root context, or by a context the guest kept after its request ended.
    CalloutsToStart {
        address: GuestAddress,
        context: ContextId,
        callouts: Vec<AcceptedCallout>,
    },
    /// Callouts a held context sent during its request, whose results will no longer be
    /// delivered. The context gets a failure for each of them.
    OpenCalloutsToFail {
        address: GuestAddress,
        context: ContextId,
        callouts: Vec<CalloutId>,
    },
    /// The guest called `proxy_done` for a held context.
    HeldContextDone {
        address: GuestAddress,
        context: ContextId,
        needs_on_log: bool,
    },
}

pub(crate) type RootCallbackSender = UnboundedSender<RootCallbackEvent>;

/// A guest's address and plugin state, with the sender for its root callback events.
pub(crate) struct RootCallbackLink {
    pub(crate) address: GuestAddress,
    pub(crate) plugin: Arc<RootCallbackPluginState>,
    sender: RootCallbackSender,
}

impl RootCallbackLink {
    pub(crate) fn new(
        address: GuestAddress,
        plugin: Arc<RootCallbackPluginState>,
        sender: RootCallbackSender,
    ) -> Self {
        RootCallbackLink {
            address,
            plugin,
            sender,
        }
    }

    pub(crate) fn send(&self, event: RootCallbackEvent) {
        // The receiver is only dropped when the runtime is dropped, and then the event is not
        // needed
        let _ = self.sender.send(event);
    }
}
