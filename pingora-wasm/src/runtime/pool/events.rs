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

//! What a guest tells the root callback thread, and where the guest is.

use crate::callout::AcceptedCallout;
use crate::root_callbacks::RootCallbackPluginState;
use proxy_wasm_host::abi::v0_2_1::{CalloutId, Changes, ContextId, GuestId, QueueId};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;

/// One slot of one pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SlotIndex {
    pub(crate) pool_index: usize,
    pub(crate) slot_index: usize,
}

/// One guest, in the slot where it runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct GuestAddress {
    pub(crate) slot: SlotIndex,
    pub(crate) guest: GuestId,
}

/// An event that tells the root callback thread about a guest call on another thread.
pub(crate) enum RootCallbackEvent {
    /// A guest call changed the tick period or registered a queue.
    TicksOrQueuesChanged {
        address: GuestAddress,
        root: ContextId,
        changes: Changes,
    },
    /// A queue got an item.
    QueueItem(QueueId),
    /// Callouts whose results go to `context` with no request. These are the callouts of a root,
    /// and the callouts of a context that the guest held after its request.
    CalloutsToStart {
        address: GuestAddress,
        context: ContextId,
        callouts: Vec<AcceptedCallout>,
    },
    /// Callouts that a held context sent during its request, whose results no task delivers.
    /// The context receives a failure for each one.
    OpenCalloutsToFail {
        address: GuestAddress,
        context: ContextId,
        callouts: Vec<CalloutId>,
    },
    /// The guest called `proxy_done` for a context that it held.
    HeldContextDone {
        address: GuestAddress,
        context: ContextId,
        needs_on_log: bool,
    },
}

pub(crate) type RootCallbackSender = UnboundedSender<RootCallbackEvent>;

/// The address and plugin of a guest, and the sender to the root callback thread.
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
        // The receiver is gone only when the runtime drops, and then nothing waits for events
        let _ = self.sender.send(event);
    }
}
