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

//! Callouts made outside of a request

use crate::callout::{
    AcceptedCallout, CalloutDelivery, GrpcCalloutHandle, GrpcPending, HttpCalloutResult,
    PendingResult,
};
use crate::runtime::pool::events::GuestAddress;
use crate::runtime::RuntimeInner;
use futures::stream::{self, BoxStream, SelectAll, StreamExt};
use proxy_wasm_host::abi::v0_2_1::{CalloutId, ContextId};
use std::sync::Arc;

pub(super) struct ArrivedDelivery {
    pub(super) address: GuestAddress,
    pub(super) context: ContextId,
    pub(super) id: CalloutId,
    pub(super) delivery: CalloutDelivery,
}

/// In-flight callouts made from a root context or from a context kept after its request ended.
#[derive(Default)]
pub(super) struct RootCallbackCallouts {
    deliveries: SelectAll<BoxStream<'static, ArrivedDelivery>>,
    /// The open gRPC streams, which end with the context that opened them.
    streams: Vec<(GuestAddress, ContextId, Arc<GrpcCalloutHandle>)>,
}

impl RootCallbackCallouts {
    pub(super) fn start(
        &mut self,
        runtime: &RuntimeInner,
        address: GuestAddress,
        context: ContextId,
        callout: AcceptedCallout,
    ) {
        let id = callout.id;
        // Without the thread's tokio runtime entered, the callout is dropped and no result is
        // delivered for it
        if let Some(pending) = runtime.callout_launcher.spawn_for_root_callback(callout) {
            self.add(address, context, id, pending);
        }
    }

    pub(super) fn add(
        &mut self,
        address: GuestAddress,
        context: ContextId,
        id: CalloutId,
        pending: PendingResult,
    ) {
        let arrived = move |delivery| ArrivedDelivery {
            address,
            context,
            id,
            delivery,
        };
        let deliveries = match pending {
            PendingResult::Known(result) => {
                stream::once(async move { CalloutDelivery::Http(result) }).boxed()
            }
            PendingResult::FromTask(task) => stream::once(async move {
                CalloutDelivery::Http(task.await.unwrap_or(HttpCalloutResult::Failed))
            })
            .boxed(),
            PendingResult::Grpc(grpc) => {
                if grpc.is_stream() {
                    self.streams.retain(|(.., handle)| !handle.is_ended());
                    self.streams.push((address, context, grpc.handle()));
                }
                grpc_events(grpc)
            }
        };
        self.deliveries.push(deliveries.map(arrived).boxed());
    }

    /// Cancel the open streams of `context`, or of every context of the guest when `context` is
    /// `None`.
    pub(super) fn cancel_streams_of(&mut self, address: GuestAddress, context: Option<ContextId>) {
        self.streams.retain(|(a, c, handle)| {
            let ended = *a == address && context.is_none_or(|context| *c == context);
            if ended {
                handle.cancel();
            }
            !ended && !handle.is_ended()
        });
    }

    pub(super) async fn next_arrived(&mut self) -> Option<ArrivedDelivery> {
        // Never resolve while no callout is in flight, so that this can be a `select!` branch
        if self.deliveries.is_empty() {
            return std::future::pending().await;
        }
        self.deliveries.next().await
    }
}

fn grpc_events(grpc: GrpcPending) -> BoxStream<'static, CalloutDelivery> {
    stream::unfold(Some(grpc), |grpc| async move {
        let mut grpc = grpc?;
        let event = grpc.next_event().await;
        let next = (!event.ends_callout(grpc.is_stream())).then_some(grpc);
        Some((CalloutDelivery::Grpc(event), next))
    })
    .boxed()
}
