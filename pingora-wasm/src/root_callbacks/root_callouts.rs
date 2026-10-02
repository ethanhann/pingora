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

use crate::callout::{AcceptedCallout, CalloutResult, PendingResult};
use crate::runtime::pool::events::GuestAddress;
use crate::runtime::RuntimeInner;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use futures::FutureExt;
use proxy_wasm_host::abi::v0_2_1::{CalloutId, ContextId};

/// A callout result to be delivered by the root callback thread.
pub(super) struct FinishedCallout {
    pub(super) address: GuestAddress,
    pub(super) context: ContextId,
    pub(super) id: CalloutId,
    pub(super) result: CalloutResult,
}

/// In-flight callouts made from a root context or from a context kept after its request ended.
#[derive(Default)]
pub(super) struct RootCallbackCallouts {
    results: FuturesUnordered<BoxFuture<'static, FinishedCallout>>,
}

impl RootCallbackCallouts {
    /// Start `callout` with the root callback thread's callout sender.
    ///
    /// Must be called with the thread's tokio runtime entered. Without one the callout is dropped
    /// and no result is delivered for it.
    pub(super) fn start(
        &mut self,
        runtime: &RuntimeInner,
        address: GuestAddress,
        context: ContextId,
        callout: AcceptedCallout,
    ) {
        let id = callout.id;
        let Some(pending) = runtime.callout_launcher.spawn_for_root_callback(callout) else {
            return;
        };
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

    /// Wait for the next callout to finish.
    ///
    /// Never resolves while no callout is in flight, so it can be used in a `select!` branch.
    pub(super) async fn next_finished(&mut self) -> Option<FinishedCallout> {
        if self.results.is_empty() {
            return std::future::pending().await;
        }
        self.results.next().await
    }
}
