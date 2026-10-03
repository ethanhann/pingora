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

use crate::callout::AcceptedCallout;
use crate::root_callbacks::{EndProgress, RootCallbackPluginState};
use proxy_wasm_host::abi::v0_2_1::{CalloutId, Changes, ContextId, GuestId, QueueId};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SlotIndex {
    pub(crate) pool_index: usize,
    pub(crate) slot_index: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct GuestAddress {
    pub(crate) slot: SlotIndex,
    pub(crate) guest: GuestId,
}

pub(crate) enum RootCallbackEvent {
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
    /// The runtime has no request left. The thread ends the plugins and sends its progress on
    /// the channel.
    End(watch::Sender<EndProgress>),
}

pub(crate) type RootCallbackSender = UnboundedSender<RootCallbackEvent>;

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
