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


//! The callouts that no request waits for.

use crate::callout::{AcceptedCallout, CalloutResult, PendingResult};
use crate::runtime::pool::events::GuestAddress;
use crate::runtime::RuntimeInner;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use futures::FutureExt;
use proxy_wasm_host::abi::v0_2_1::{CalloutId, ContextId};
use std::collections::HashSet;

/// The result of a callout that no request waits for.
pub(super) struct FinishedCallout {
    pub(super) address: GuestAddress,
    pub(super) context: ContextId,
    pub(super) id: CalloutId,
    pub(super) result: CalloutResult,
}

/// The callouts in flight whose results go to a root, or to a context that the guest held.
#[derive(Default)]
pub(super) struct RootCallouts {
    results: FuturesUnordered<BoxFuture<'static, FinishedCallout>>,
    in_flight: HashSet<(GuestAddress, CalloutId)>,
    /// The callouts in flight whose context ended, so that their results go nowhere.
    ended: HashSet<(GuestAddress, CalloutId)>,
}

impl RootCallouts {
    /// Start `callout` on the connector of the root callback thread.
    pub(super) fn start(
        &mut self,
        runtime: &RuntimeInner,
        address: GuestAddress,
        context: ContextId,
        callout: AcceptedCallout,
    ) {
        let id = callout.id;
        let Some(pending) = runtime.callout_launcher.spawn_root(callout) else {
            return;
        };
        self.in_flight.insert((address, id));
        self.results.push(
            async move {
                let result = match pending {
                    PendingResult::Known(result) => result,
                    PendingResult::FromTask(task) => task.await.unwrap_or(CalloutResult::Failed),
                };
                FinishedCallout {
                    address,
                    context,
                    id,
                    result,
                }
            }
            .boxed(),
        );
    }

    /// Wait for the next result. Never returns when no callout is in flight.
    pub(super) async fn next_finished(&mut self) -> Option<FinishedCallout> {
        if self.results.is_empty() {
            return std::future::pending().await;
        }
        let finished = self.results.next().await?;
        self.in_flight.remove(&(finished.address, finished.id));
        Some(finished)
    }

    /// Record that `proxy_on_delete` ended the callouts `ids` of a context of the guest at
    /// `address`. Only the callouts that are in flight here are recorded, because a callout of
    /// a request sends its result nowhere.
    pub(super) fn end(&mut self, address: GuestAddress, ids: &[CalloutId]) {
        let in_flight = ids.iter().filter(|id| self.in_flight.contains(&(address, **id)));
        let ended: Vec<_> = in_flight.map(|id| (address, *id)).collect();
        self.ended.extend(ended);
    }

    /// Return whether the context of the callout `id` ended, and forget the callout.
    pub(super) fn was_ended(&mut self, address: GuestAddress, id: CalloutId) -> bool {
        self.ended.remove(&(address, id))
    }
}
