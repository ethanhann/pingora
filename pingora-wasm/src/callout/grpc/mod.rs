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

//! gRPC callouts from plugins

mod client;
pub(crate) mod frame;
mod service_config;
pub(crate) mod status;

pub(crate) use service_config::upstream_name;

use super::headers::RejectedCalloutHeader;
use super::result::OwnedHeaderPairs;
use bytes::Bytes;
use http::header::{HeaderName, CONTENT_TYPE, HOST, TE};
use http::Method;
use pingora_http::RequestHeader;
use proxy_wasm_host::abi::v0_2_1::{Callback, GrpcStatus, HeaderPairs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::task::AbortHandle;

pub(crate) const APPLICATION_GRPC: &str = "application/grpc";
const GRPC_TIMEOUT: &str = "grpc-timeout";
const BINARY_SUFFIX: &str = "-bin";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GrpcCalloutEvent {
    InitialMetadata(OwnedHeaderPairs),
    Message(Bytes),
    TrailingMetadata(OwnedHeaderPairs),
    Close(GrpcStatus),
}

impl GrpcCalloutEvent {
    pub(crate) fn close(code: u32, message: &str) -> Self {
        GrpcCalloutEvent::Close(GrpcStatus::new(code, message))
    }

    pub(crate) fn ends_callout(&self, is_stream: bool) -> bool {
        match self {
            GrpcCalloutEvent::Close(_) => true,
            GrpcCalloutEvent::Message(_) => !is_stream,
            _ => false,
        }
    }

    pub(crate) fn callback(&self) -> Callback {
        match self {
            GrpcCalloutEvent::InitialMetadata(_) => Callback::GrpcReceiveInitialMetadata,
            GrpcCalloutEvent::Message(_) => Callback::GrpcReceive,
            GrpcCalloutEvent::TrailingMetadata(_) => Callback::GrpcReceiveTrailingMetadata,
            GrpcCalloutEvent::Close(_) => Callback::GrpcClose,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GrpcCommand {
    Send { message: Bytes, end_of_stream: bool },
    Close,
}

/// The plugin's side of a gRPC callout, shared by the guest's callout service and the context
/// that waits for its events.
#[derive(Debug)]
pub(crate) struct GrpcCalloutHandle {
    commands: UnboundedSender<GrpcCommand>,
    task: OnceLock<AbortHandle>,
    cancelled: AtomicBool,
    /// Set when the plugin opens, sends on, or closes the stream, and cleared when a wait checks
    /// it or the plugin continues.
    plugin_activity: AtomicBool,
    /// Whether the task queues the messages and the metadata it receives. The plugin of a
    /// request receives them only while it is paused, so they are dropped while it is not.
    keeps_events: AtomicBool,
}

impl GrpcCalloutHandle {
    pub(crate) fn new() -> (Arc<Self>, UnboundedReceiver<GrpcCommand>) {
        let (commands, receiver) = mpsc::unbounded_channel();
        let handle = GrpcCalloutHandle {
            commands,
            task: OnceLock::new(),
            cancelled: AtomicBool::new(false),
            plugin_activity: AtomicBool::new(true),
            keeps_events: AtomicBool::new(true),
        };
        (Arc::new(handle), receiver)
    }

    /// Queue `command` for the task, which may not have started yet, and return `false` once the
    /// task has ended.
    pub(crate) fn queue_command(&self, command: GrpcCommand) -> bool {
        self.plugin_activity.store(true, Ordering::Relaxed);
        // The response to this command may arrive before the plugin pauses
        self.set_keeps_events(true);
        self.commands.send(command).is_ok()
    }

    pub(crate) fn set_task(&self, task: AbortHandle) {
        let _ = self.task.set(task);
        // A cancel between the spawn and the store above found no task to abort
        if self.cancelled.load(Ordering::SeqCst) {
            self.abort_task();
        }
    }

    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        self.abort_task();
    }

    fn abort_task(&self) {
        if let Some(task) = self.task.get() {
            task.abort();
        }
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    pub(crate) fn is_ended(&self) -> bool {
        self.commands.is_closed()
    }

    pub(crate) fn take_plugin_activity(&self) -> bool {
        self.plugin_activity.swap(false, Ordering::Relaxed)
    }

    pub(crate) fn set_keeps_events(&self, keeps: bool) {
        self.keeps_events.store(keeps, Ordering::SeqCst);
    }
}

/// Event sender for a gRPC callout that drops messages and metadata while its plugin does not
/// receive them.
#[derive(Clone)]
pub(crate) struct GrpcEventSender {
    events: UnboundedSender<GrpcCalloutEvent>,
    handle: Arc<GrpcCalloutHandle>,
}

impl GrpcEventSender {
    pub(crate) fn new(
        handle: Arc<GrpcCalloutHandle>,
    ) -> (Self, UnboundedReceiver<GrpcCalloutEvent>) {
        let (events, receiver) = mpsc::unbounded_channel();
        (GrpcEventSender { events, handle }, receiver)
    }

    pub(crate) fn send(&self, event: GrpcCalloutEvent) {
        let close = matches!(event, GrpcCalloutEvent::Close(_));
        if close || self.handle.keeps_events.load(Ordering::SeqCst) {
            let _ = self.events.send(event);
        }
    }
}

pub(crate) struct AcceptedGrpc {
    pub(crate) stream: bool,
    pub(crate) commands: UnboundedReceiver<GrpcCommand>,
    pub(crate) handle: Arc<GrpcCalloutHandle>,
}

pub(crate) fn request_header(
    upstream: &str,
    service: &[u8],
    method: &[u8],
    metadata: &HeaderPairs<'_>,
    timeout: Option<Duration>,
) -> Result<RequestHeader, RejectedCalloutHeader> {
    let invalid_path = || RejectedCalloutHeader::InvalidPseudo(":path");
    let mut path = Vec::with_capacity(service.len() + method.len() + 2);
    for part in [service, method] {
        path.push(b'/');
        path.extend_from_slice(part);
    }
    let mut request = RequestHeader::build(Method::POST, &path, Some(metadata.len() + 4))
        .map_err(|_| invalid_path())?;
    let invalid_host = || RejectedCalloutHeader::InvalidRegular(HOST.to_string());
    request
        .insert_header(HOST, upstream)
        .map_err(|_| invalid_host())?;
    for (key, value) in metadata {
        let invalid = || RejectedCalloutHeader::InvalidRegular(String::from_utf8_lossy(key).into());
        let name = HeaderName::from_bytes(key).map_err(|_| invalid())?;
        if [HOST, TE, CONTENT_TYPE].contains(&name) || name == GRPC_TIMEOUT {
            continue;
        }
        let appended = match name.as_str().ends_with(BINARY_SUFFIX) {
            true => request.append_header(name, status::base64(value)),
            false => request.append_header(name, value.as_ref()),
        };
        appended.map_err(|_| invalid())?;
    }
    let reserved = [
        (TE.as_str(), "trailers"),
        (CONTENT_TYPE.as_str(), APPLICATION_GRPC),
    ];
    for (name, value) in reserved {
        let _ = request.insert_header(name, value);
    }
    if let Some(timeout) = timeout {
        let _ = request.insert_header(GRPC_TIMEOUT, status::timeout_header(timeout));
    }
    Ok(request)
}

pub(crate) fn is_grpc_content_type(content_type: &[u8]) -> bool {
    let grpc = APPLICATION_GRPC.as_bytes();
    content_type == grpc || content_type.starts_with(&[grpc, b"+"].concat())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::callout::headers::tests::pairs;

    #[test]
    fn request_header_sets_grpc_headers() {
        let metadata = pairs(&[
            ("x-trace", "abc"),
            ("trace-bin", "\x01\x02"),
            ("content-type", "text/plain"),
            ("host", "elsewhere"),
        ]);

        let header = request_header(
            "authz",
            b"example.Authz",
            b"Check",
            &metadata,
            Some(Duration::from_secs(1)),
        )
        .unwrap();

        assert_eq!(header.method, Method::POST);
        assert_eq!(header.raw_path(), b"/example.Authz/Check");
        let get = |name: &str| header.headers[name].to_str().unwrap();
        let want = [
            ("host", "authz"),
            ("te", "trailers"),
            ("content-type", "application/grpc"),
            ("grpc-timeout", "1000m"),
            ("x-trace", "abc"),
            ("trace-bin", "AQI="),
        ];
        assert_eq!(
            want.map(|(name, _)| get(name)),
            want.map(|(_, value)| value)
        );
        assert_eq!(header.headers.len(), want.len());
    }

    #[test]
    fn grpc_content_type_allows_plus_suffix_only() {
        let cases: [(&[u8], bool); 4] = [
            (b"application/grpc", true),
            (b"application/grpc+proto", true),
            (b"application/grpc-web", false),
            (b"application/json", false),
        ];

        let got = cases.map(|(content_type, _)| is_grpc_content_type(content_type));

        assert_eq!(got, cases.map(|(_, grpc)| grpc));
    }
}
