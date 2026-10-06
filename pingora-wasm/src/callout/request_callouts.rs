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

//! Per-request callouts

use super::{
    AcceptedCallout, CalloutDelivery, GrpcCalloutEvent, GrpcCalloutHandle, HttpCalloutResult,
};
use crate::callout::grpc::status::INTERNAL;
use proxy_wasm_host::abi::v0_2_1::CalloutId;
use std::future::{poll_fn, Future};
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinHandle;

#[derive(Debug)]
pub(crate) enum PendingResult {
    FromTask(JoinHandle<HttpCalloutResult>),
    /// The result was decided without sending the callout.
    Known(HttpCalloutResult),
    Grpc(GrpcPending),
}

/// The events of a gRPC callout, as its task sends them.
///
/// Dropping a stream cancels it. A call runs until its response or its timeout, even after the
/// request ends.
#[derive(Debug)]
pub(crate) struct GrpcPending {
    events: UnboundedReceiver<GrpcCalloutEvent>,
    /// A close found while dropping the events the plugin does not receive, which ends the
    /// callout and so is kept.
    held_close: Option<GrpcCalloutEvent>,
    handle: Arc<GrpcCalloutHandle>,
    stream: bool,
}

impl GrpcPending {
    pub(crate) fn new(
        events: UnboundedReceiver<GrpcCalloutEvent>,
        handle: Arc<GrpcCalloutHandle>,
        stream: bool,
    ) -> Self {
        GrpcPending {
            events,
            held_close: None,
            handle,
            stream,
        }
    }

    pub(crate) fn is_stream(&self) -> bool {
        self.stream
    }

    pub(crate) fn handle(&self) -> Arc<GrpcCalloutHandle> {
        self.handle.clone()
    }

    pub(crate) async fn next_event(&mut self) -> GrpcCalloutEvent {
        poll_fn(|cx| self.poll_event(cx)).await
    }

    fn poll_event(&mut self, cx: &mut std::task::Context<'_>) -> Poll<GrpcCalloutEvent> {
        if let Some(close) = self.held_close.take() {
            return Poll::Ready(close);
        }
        self.events.poll_recv(cx).map(|event| {
            event.unwrap_or_else(|| GrpcCalloutEvent::close(INTERNAL, "callout task ended"))
        })
    }

    fn drop_queued_events(&mut self) {
        while let Ok(event) = self.events.try_recv() {
            if matches!(event, GrpcCalloutEvent::Close(_)) {
                self.held_close = Some(event);
            }
        }
    }
}

impl Drop for GrpcPending {
    fn drop(&mut self) {
        if self.stream {
            self.handle.cancel();
        }
    }
}

struct PendingCallout {
    position: usize,
    id: CalloutId,
    result: PendingResult,
    /// Whether the current wait of the plugin lasts until this callout ends.
    covered: bool,
}

impl PendingCallout {
    fn stream_mut(&mut self) -> Option<&mut GrpcPending> {
        match &mut self.result {
            PendingResult::Grpc(grpc) if grpc.stream => Some(grpc),
            _ => None,
        }
    }

    fn is_stream(&self) -> bool {
        matches!(&self.result, PendingResult::Grpc(grpc) if grpc.stream)
    }
}

/// Callouts made by the plugins of one request.
///
/// Callouts made during a guest call are kept in `accepted` until the filter starts their tasks.
/// If the plugin is paused at that point, the started callouts move to `pending`, where the
/// filter waits for their results. A gRPC stream moves to `pending` even if the plugin is not
/// paused, and stays there until it closes or its context ends.
#[derive(Default)]
pub(crate) struct RequestCallouts {
    accepted: Vec<AcceptedCallout>,
    pending: Vec<PendingCallout>,
    /// The chain position of the plugin a filter is waiting on for a callout result.
    pub(crate) waiting_position: Option<usize>,
}

impl RequestCallouts {
    pub(crate) fn set_accepted(&mut self, accepted: Vec<AcceptedCallout>) {
        self.accepted = accepted;
    }

    pub(crate) fn take_accepted(&mut self) -> Vec<AcceptedCallout> {
        mem::take(&mut self.accepted)
    }

    pub(crate) fn add_pending(&mut self, position: usize, id: CalloutId, result: PendingResult) {
        self.pending.push(PendingCallout {
            position,
            id,
            result,
            covered: false,
        });
    }

    fn at(&mut self, position: usize) -> impl Iterator<Item = &mut PendingCallout> {
        self.pending
            .iter_mut()
            .filter(move |p| p.position == position)
    }

    /// Mark the callouts that a wait of the plugin at `position` lasts for, and return whether
    /// there is one. A call always counts, and a stream counts once the plugin opened, sent on,
    /// or closed it since the last mark.
    pub(crate) fn cover_for_wait(&mut self, position: usize) -> bool {
        let mut covers_any = false;
        for pending in self.at(position) {
            pending.covered |= match pending.stream_mut() {
                Some(stream) => stream.handle.take_plugin_activity(),
                None => true,
            };
            covers_any |= pending.covered;
        }
        covers_any
    }

    /// Keep the events of the streams of `position` for its plugin, which has paused.
    pub(crate) fn keep_stream_events(&mut self, position: usize) {
        for pending in self.at(position) {
            if let Some(stream) = pending.stream_mut() {
                stream.handle.set_keeps_events(true);
            }
        }
    }

    /// Drop the events of the streams of `position` from now on, and those already queued,
    /// because its plugin has continued.
    pub(crate) fn drop_stream_events(&mut self, position: usize) {
        for pending in self.at(position) {
            pending.covered = false;
            if let Some(stream) = pending.stream_mut() {
                stream.handle.set_keeps_events(false);
                stream.handle.take_plugin_activity();
                stream.drop_queued_events();
            }
        }
    }

    /// Forget the results the plugin at `position` no longer waits for. Its streams stay open.
    pub(crate) fn forget_pending(&mut self, position: usize) {
        self.pending
            .retain(|p| p.position != position || p.is_stream());
        for stream in self.at(position) {
            stream.covered = false;
        }
    }

    pub(crate) fn take_streams(&mut self, position: usize) -> Vec<(CalloutId, GrpcPending)> {
        let (taken, kept) = mem::take(&mut self.pending)
            .into_iter()
            .partition(|p| p.position == position && p.is_stream());
        self.pending = kept;
        taken
            .into_iter()
            .filter_map(|p| match p.result {
                PendingResult::Grpc(grpc) => Some((p.id, grpc)),
                _ => None,
            })
            .collect()
    }

    /// Cancel the streams of `position`, whose context has ended.
    pub(crate) fn end_streams(&mut self, position: usize) {
        drop(self.take_streams(position));
    }

    /// Forget every callout except the streams, which end with the context of their plugin.
    /// Running HTTP callouts and gRPC calls are not cancelled.
    pub(crate) fn clear(&mut self) {
        self.accepted.clear();
        self.pending.retain(PendingCallout::is_stream);
    }

    /// Wait for the next result or event at `position`, or return `None` once no callout that
    /// the wait covers is left.
    pub(crate) async fn next_result(
        &mut self,
        position: usize,
    ) -> Option<(CalloutId, CalloutDelivery)> {
        self.pending.retain(|p| match &p.result {
            PendingResult::Grpc(grpc) => !grpc.handle.is_cancelled(),
            _ => true,
        });
        if !self
            .pending
            .iter()
            .any(|p| p.position == position && p.covered)
        {
            return None;
        }
        let (index, delivery, ends) = poll_fn(|cx| {
            for (index, pending) in self.pending.iter_mut().enumerate() {
                if pending.position != position {
                    continue;
                }
                let ready = match &mut pending.result {
                    PendingResult::Known(result) => {
                        Poll::Ready((CalloutDelivery::Http(result.clone()), true))
                    }
                    // A task that panicked or was cancelled yields `Failed`
                    PendingResult::FromTask(task) => Pin::new(task).poll(cx).map(|joined| {
                        let result = joined.unwrap_or(HttpCalloutResult::Failed);
                        (CalloutDelivery::Http(result), true)
                    }),
                    PendingResult::Grpc(grpc) => grpc.poll_event(cx).map(|event| {
                        let ends = event.ends_callout(grpc.stream);
                        (CalloutDelivery::Grpc(event), ends)
                    }),
                };
                if let Poll::Ready((delivery, ends)) = ready {
                    return Poll::Ready((index, delivery, ends));
                }
            }
            Poll::Pending
        })
        .await;
        let id = self.pending[index].id;
        if ends {
            self.pending.remove(index);
        }
        Some((id, delivery))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::callout::grpc::{GrpcCommand, GrpcEventSender};
    use bytes::Bytes;
    use futures::FutureExt;

    fn message(text: &'static str) -> GrpcCalloutEvent {
        GrpcCalloutEvent::Message(Bytes::from_static(text.as_bytes()))
    }

    /// Return callouts with one stream at position 0, and the sender of its events.
    fn one_stream() -> (RequestCallouts, Arc<GrpcCalloutHandle>, GrpcEventSender) {
        let (handle, _commands) = GrpcCalloutHandle::new();
        let (events, received) = GrpcEventSender::new(handle.clone());
        let mut callouts = RequestCallouts::default();
        let stream = GrpcPending::new(received, handle.clone(), true);
        callouts.add_pending(
            0,
            CalloutId::try_from(1).unwrap(),
            PendingResult::Grpc(stream),
        );
        (callouts, handle, events)
    }

    fn next_event(callouts: &mut RequestCallouts) -> Option<GrpcCalloutEvent> {
        match callouts.next_result(0).now_or_never().flatten() {
            Some((_, CalloutDelivery::Grpc(event))) => Some(event),
            _ => None,
        }
    }

    #[test]
    fn stream_events_reach_plugin_only_while_it_waits() {
        let close = GrpcCalloutEvent::close(0, "");
        type Step = fn(&mut RequestCallouts, &GrpcCalloutHandle, &GrpcEventSender);
        let cases: [(&str, Step, Option<GrpcCalloutEvent>); 3] = [
            (
                "message after the plugin continued",
                |callouts, _, events| {
                    events.send(message("early"));
                    callouts.drop_stream_events(0);
                    events.send(message("late"));
                },
                None,
            ),
            (
                "close after the plugin continued",
                |callouts, handle, events| {
                    callouts.drop_stream_events(0);
                    events.send(GrpcCalloutEvent::close(0, ""));
                    handle.queue_command(GrpcCommand::Close);
                },
                Some(close),
            ),
            (
                "answer before the wait starts",
                |callouts, handle, events| {
                    callouts.drop_stream_events(0);
                    handle.queue_command(GrpcCommand::Close);
                    events.send(message("answer"));
                },
                Some(message("answer")),
            ),
        ];
        for (case, step, want) in cases {
            let (mut callouts, handle, events) = one_stream();
            step(&mut callouts, &handle, &events);
            callouts.keep_stream_events(0);
            callouts.cover_for_wait(0);

            let got = next_event(&mut callouts);

            assert_eq!(got, want, "{case}");
        }
    }
}
