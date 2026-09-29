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

//! The callouts of one request.

use super::{AcceptedCallout, CalloutResult};
use proxy_wasm_host::abi::v0_2_1::CalloutId;
use std::future::{poll_fn, Future};
use std::mem;
use std::pin::Pin;
use std::task::Poll;
use tokio::task::JoinHandle;

/// The result of a callout that the plugin did not receive yet.
#[derive(Debug)]
pub(crate) enum PendingResult {
    /// The task that sends the callout returns the result.
    FromTask(JoinHandle<CalloutResult>),
    Known(CalloutResult),
}

struct PendingCallout {
    position: usize,
    id: CalloutId,
    result: PendingResult,
}

/// The callouts of the plugins of one request.
///
/// A callout that a plugin sends during a guest call stays in `accepted` until the phase starts
/// its task. When the plugin is paused, the started callout moves to `pending`, where the phase
/// waits for its result. For a request with no callout, both lists stay empty and allocate
/// nothing.
#[derive(Default)]
pub(crate) struct RequestCallouts {
    accepted: Vec<AcceptedCallout>,
    pending: Vec<PendingCallout>,
    /// Whether a phase is waiting for a callout. It stays `true` when the future of the phase
    /// is dropped during the wait.
    pub(crate) in_callout_wait: bool,
}

impl RequestCallouts {
    /// Replace the callouts of the last guest call with those of a new one.
    pub(crate) fn set_accepted(&mut self, accepted: Vec<AcceptedCallout>) {
        self.accepted = accepted;
    }

    /// Remove and return the callouts of the last guest call.
    pub(crate) fn take_accepted(&mut self) -> Vec<AcceptedCallout> {
        mem::take(&mut self.accepted)
    }

    /// Add a started callout of the plugin at `position`, so that the phase can wait for its
    /// result.
    pub(crate) fn add_pending(&mut self, position: usize, id: CalloutId, result: PendingResult) {
        self.pending.push(PendingCallout {
            position,
            id,
            result,
        });
    }

    pub(crate) fn has_pending(&self, position: usize) -> bool {
        self.pending.iter().any(|p| p.position == position)
    }

    /// Stop waiting for the callouts of the plugin at `position`. Their tasks continue.
    pub(crate) fn forget_pending(&mut self, position: usize) {
        self.pending.retain(|p| p.position != position);
    }

    /// Forget every callout of the request. The tasks that are running continue.
    pub(crate) fn clear(&mut self) {
        self.accepted.clear();
        self.pending.clear();
    }

    /// Wait for the next result of a pending callout of the plugin at `position`.
    ///
    /// Return `None` when the plugin has no pending callout. A task that panicked or was
    /// cancelled yields [CalloutResult::Failed].
    pub(crate) async fn next_result(
        &mut self,
        position: usize,
    ) -> Option<(CalloutId, CalloutResult)> {
        if !self.has_pending(position) {
            return None;
        }
        let (index, result) = poll_fn(|cx| {
            for (index, pending) in self.pending.iter_mut().enumerate() {
                if pending.position != position {
                    continue;
                }
                let ready = match &mut pending.result {
                    PendingResult::Known(result) => Poll::Ready(result.clone()),
                    PendingResult::FromTask(task) => Pin::new(task)
                        .poll(cx)
                        .map(|joined| joined.unwrap_or(CalloutResult::Failed)),
                };
                if let Poll::Ready(result) = ready {
                    return Poll::Ready((index, result));
                }
            }
            Poll::Pending
        })
        .await;
        let callout = self.pending.remove(index);
        Some((callout.id, result))
    }
}
