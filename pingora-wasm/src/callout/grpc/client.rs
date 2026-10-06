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

//! gRPC callout client

use super::frame::{self, FrameError, MessageReader};
use super::status::{self, INTERNAL, OK, RESOURCE_EXHAUSTED, UNAVAILABLE, UNKNOWN};
use super::{GrpcCalloutEvent, GrpcCommand, GrpcEventSender};
use crate::callout::result::{connect_failure, session_failure, OwnedHeaderPairs};
use crate::callout::{AcceptedCallout, CalloutTarget, ConnectorSender};
use crate::observability::CalloutFailure;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::FutureExt;
use h2::SendStream;
use http::{HeaderMap, StatusCode};
use log::debug;
use pingora_core::protocols::http::client::HttpSession;
use pingora_core::protocols::http::v2::client::Http2Session;
use pingora_core::protocols::http::v2::write_body;
use pingora_core::upstreams::peer::{HttpPeer, Peer};
use pingora_error::{Error, ErrorType};
use pingora_timeout::timeout;
use proxy_wasm_host::abi::v0_2_1::GrpcStatus;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedReceiver;

/// The failure kind, the close the plugin gets, and the cause for the log.
type Failure = (CalloutFailure, GrpcCalloutEvent, String);

type Written = (SendStream<Bytes>, pingora_error::Result<()>, bool);

enum Writer {
    Idle(SendStream<Bytes>),
    Writing(BoxFuture<'static, Written>),
    Closed,
}

/// The writer of the plugin's messages for a stream, which writes while the response is read.
struct Outbox {
    writer: Writer,
    write_timeout: Option<Duration>,
}

impl Outbox {
    fn takes_command(&self, commands: &UnboundedReceiver<GrpcCommand>) -> bool {
        matches!(self.writer, Writer::Idle(_)) && !commands.is_closed()
    }

    fn is_writing(&self) -> bool {
        matches!(self.writer, Writer::Writing(_))
    }

    fn start_write(&mut self, command: Option<GrpcCommand>) {
        let (data, end) = match command {
            Some(GrpcCommand::Send {
                message,
                end_of_stream,
            }) => (frame::frame(&message), end_of_stream),
            Some(GrpcCommand::Close) => (Bytes::new(), true),
            None => return,
        };
        let Writer::Idle(mut writer) = std::mem::replace(&mut self.writer, Writer::Closed) else {
            return;
        };
        let write_timeout = self.write_timeout;
        let write = async move {
            let written = write_body(&mut writer, data, end, write_timeout).await;
            (writer, written, end)
        };
        self.writer = Writer::Writing(write.boxed());
    }

    async fn finish_write(&mut self) {
        let Writer::Writing(write) = &mut self.writer else {
            return std::future::pending().await;
        };
        let (writer, written, end) = write.await;
        self.writer = match written {
            Ok(()) if !end => Writer::Idle(writer),
            Ok(()) => Writer::Closed,
            // The server may have ended the call already, so its status is still read
            Err(e) => {
                debug!("gRPC stream write failed, the response is still read: {e}");
                Writer::Closed
            }
        };
    }
}

impl ConnectorSender {
    pub(crate) async fn send_grpc_callout(
        &self,
        callout: AcceptedCallout,
        commands: UnboundedReceiver<GrpcCommand>,
        stream: bool,
        events: GrpcEventSender,
    ) {
        let plugin = &callout.plugin_conf.plugin_name;
        let exchange = self.exchange(&callout, commands, stream, &events);
        let outcome = match stream {
            true => exchange.await,
            false => match timeout(callout.timeout, exchange).await {
                Ok(outcome) => outcome,
                Err(_) => Err((
                    CalloutFailure::Timeout,
                    GrpcCalloutEvent::close(status::DEADLINE_EXCEEDED, "deadline exceeded"),
                    format!("timed out after {:?}", callout.timeout),
                )),
            },
        };
        if let Err((failure, close, cause)) = outcome {
            debug!(
                "wasm plugin {plugin}: gRPC callout to upstream {} failed: {failure}, {cause}",
                callout.upstream
            );
            self.metric_sink.callout_failed(plugin, failure);
            events.send(close);
        }
    }

    async fn exchange(
        &self,
        callout: &AcceptedCallout,
        mut commands: UnboundedReceiver<GrpcCommand>,
        stream: bool,
        events: &GrpcEventSender,
    ) -> Result<(), Failure> {
        let (mut session, peer) = self.open_grpc(callout, stream).await?;
        let writer = stream.then(|| session.take_request_body_writer()).flatten();
        let mut outbox = Outbox {
            writer: writer.map_or(Writer::Closed, Writer::Idle),
            write_timeout: session.write_timeout,
        };
        loop {
            tokio::select! {
                read = session.read_response_header() => {
                    read.map_err(|e| failure_before_header(&e))?;
                    break;
                }
                command = commands.recv(), if outbox.takes_command(&commands) => {
                    outbox.start_write(command);
                }
                () = outbox.finish_write(), if outbox.is_writing() => {}
            }
        }
        let Some(header) = session.response_header().cloned() else {
            return Err(invalid_response("no response header"));
        };
        let header_status = status::from_headers(&header.headers);
        if session.response_finished() {
            events.send(GrpcCalloutEvent::Close(header_only_status(
                header.status,
                header_status,
            )?));
            return Ok(());
        }
        if header.status != StatusCode::OK {
            if let Some(status) = header_status {
                events.send(GrpcCalloutEvent::Close(status));
                return Ok(());
            }
            return Err(not_grpc_response(header.status));
        }
        if stream {
            let metadata = owned_pairs(&header.headers);
            events.send(GrpcCalloutEvent::InitialMetadata(metadata));
        }
        let limit = callout.plugin_conf.response_limit;
        let mut reader = MessageReader::default();
        let mut response_message = None;
        loop {
            tokio::select! {
                chunk = session.read_response_body() => {
                    let Some(chunk) = chunk.map_err(|e| failure_after_header(&e))? else {
                        break;
                    };
                    reader.push(&chunk);
                    while let Some(message) = reader.next_message(limit).map_err(frame_failure)? {
                        match stream {
                            true => events.send(GrpcCalloutEvent::Message(message)),
                            false => response_message = response_message.or(Some(message)),
                        }
                    }
                }
                command = commands.recv(), if outbox.takes_command(&commands) => {
                    outbox.start_write(command);
                }
                () = outbox.finish_write(), if outbox.is_writing() => {}
            }
        }
        if reader.has_partial_message() {
            return Err(invalid_response("response ended inside a message"));
        }
        let trailers = session
            .read_trailers()
            .await
            .map_err(|e| failure_after_header(&e))?;
        let Some(status) = trailers
            .as_ref()
            .and_then(status::from_headers)
            .or(header_status)
        else {
            return Err(missing_status());
        };
        match (stream, status.code, response_message) {
            (true, ..) => {
                if let Some(trailers) = &trailers {
                    events.send(GrpcCalloutEvent::TrailingMetadata(owned_pairs(trailers)));
                }
                events.send(GrpcCalloutEvent::Close(status));
            }
            (false, OK, Some(message)) => events.send(GrpcCalloutEvent::Message(message)),
            (false, OK, None) => return Err(invalid_response("call ended with no message")),
            (false, ..) => events.send(GrpcCalloutEvent::Close(status)),
        }
        drop(outbox);
        let idle_timeout = peer.idle_timeout();
        self.connector
            .release_http_session(HttpSession::H2(session), &*peer, idle_timeout)
            .await;
        Ok(())
    }

    async fn open_grpc(
        &self,
        callout: &AcceptedCallout,
        stream: bool,
    ) -> Result<(Http2Session, Box<HttpPeer>), Failure> {
        let plugin = &callout.plugin_conf.plugin_name;
        let target = CalloutTarget::new(plugin, &callout.upstream, &callout.request);
        let mut peer = self.upstreams.callout_peer(&target).await.map_err(|e| {
            let close = GrpcCalloutEvent::close(UNAVAILABLE, "no healthy upstream");
            (CalloutFailure::NoPeer, close, e.to_string())
        })?;
        peer.options.set_http_version(2, 2);
        let mut may_retry = true;
        loop {
            let (session, reused) = self.connector.get_http_session(&*peer).await.map_err(|e| {
                let close = GrpcCalloutEvent::close(UNAVAILABLE, "upstream connect error");
                (connect_failure(&e), close, e.to_string())
            })?;
            let HttpSession::H2(mut session) = session else {
                let close = GrpcCalloutEvent::close(UNAVAILABLE, "upstream did not use HTTP/2");
                let cause = "the connector returned an HTTP/1 session".to_string();
                return Err((CalloutFailure::ProtocolError, close, cause));
            };
            session.write_timeout = peer.options.write_timeout;
            // A stream can be idle for as long as the plugin and the server want
            session.read_timeout = (!stream).then_some(peer.options.read_timeout).flatten();
            let written = match session.write_request_header(callout.request.clone(), false) {
                Ok(()) if !stream => {
                    let body = callout.body.clone();
                    session.write_request_body(body, true).await
                }
                written => written,
            };
            match written {
                Ok(()) => return Ok((session, peer)),
                // The peer may have closed a pooled connection while it sat idle
                Err(_) if reused && may_retry => may_retry = false,
                Err(e) => return Err(failure_before_header(&e)),
            }
        }
    }
}

/// Return the status of a response that ended on its header, which must have a `grpc-status`.
fn header_only_status(
    http_status: StatusCode,
    grpc_status: Option<GrpcStatus>,
) -> Result<GrpcStatus, Failure> {
    match grpc_status {
        Some(status) => Ok(status),
        None if http_status == StatusCode::OK => Err(missing_status()),
        None => Err(not_grpc_response(http_status)),
    }
}

fn is_timeout(e: &Error) -> bool {
    matches!(
        e.etype(),
        ErrorType::ReadTimedout | ErrorType::WriteTimedout
    )
}

fn failure_before_header(e: &Error) -> Failure {
    let message = match is_timeout(e) {
        true => "upstream request timeout",
        false => "upstream connect error or disconnect",
    };
    let close = GrpcCalloutEvent::close(UNAVAILABLE, message);
    (session_failure(e), close, e.to_string())
}

fn failure_after_header(e: &Error) -> Failure {
    let (failure, message) = match is_timeout(e) {
        true => (CalloutFailure::Timeout, "upstream request timeout"),
        false => (
            CalloutFailure::FailedAfterHeader,
            "upstream reset or disconnect",
        ),
    };
    let close = GrpcCalloutEvent::close(INTERNAL, message);
    (failure, close, e.to_string())
}

fn frame_failure(e: FrameError) -> Failure {
    let (failure, close) = match e {
        FrameError::Compressed => (
            CalloutFailure::FailedAfterHeader,
            GrpcCalloutEvent::close(INTERNAL, "compressed message"),
        ),
        FrameError::TooLarge => (
            CalloutFailure::ResponseTooLarge,
            GrpcCalloutEvent::close(RESOURCE_EXHAUSTED, "message over callout_response_limit"),
        ),
    };
    (failure, close, format!("{e:?}"))
}

fn not_grpc_response(http_status: StatusCode) -> Failure {
    let code = status::from_http_status(http_status.as_u16());
    let message = format!("HTTP status {}", http_status.as_str());
    let close = GrpcCalloutEvent::close(code, &message);
    (CalloutFailure::NotGrpcResponse, close, message)
}

fn missing_status() -> Failure {
    invalid_response_with(UNKNOWN, "missing grpc-status")
}

fn invalid_response(message: &str) -> Failure {
    invalid_response_with(INTERNAL, message)
}

fn invalid_response_with(code: u32, message: &str) -> Failure {
    let close = GrpcCalloutEvent::close(code, message);
    (
        CalloutFailure::InvalidGrpcResponse,
        close,
        message.to_string(),
    )
}

fn owned_pairs(headers: &HeaderMap) -> OwnedHeaderPairs {
    headers
        .iter()
        .map(|(name, value)| (name.as_str().as_bytes().to_vec(), value.as_bytes().to_vec()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::callout::{PluginCalloutConf, StaticCalloutUpstreams};
    use crate::test_support::RecordedFailures;
    use futures::future::BoxFuture;
    use futures::FutureExt;
    use h2::server::SendResponse;
    use h2::{Reason, RecvStream};
    use http::{Request, Response};
    use pingora_core::connectors::http::Connector;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;

    type Handler = fn(Request<RecvStream>, SendResponse<Bytes>) -> BoxFuture<'static, ()>;

    const LIMIT: usize = 8;

    async fn h2_server<H>(handler: H) -> SocketAddr
    where
        H: Fn(Request<RecvStream>, SendResponse<Bytes>) -> BoxFuture<'static, ()>,
        H: Clone + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let handler = handler.clone();
                tokio::spawn(async move {
                    let mut connection = h2::server::handshake(stream).await.unwrap();
                    while let Some(Ok((request, respond))) = connection.accept().await {
                        tokio::spawn(handler(request, respond));
                    }
                });
            }
        });
        addr
    }

    fn header(status: u16, grpc_status: Option<&str>) -> Response<()> {
        let mut response = Response::builder().status(status);
        if let Some(code) = grpc_status {
            response = response
                .header("grpc-status", code)
                .header("grpc-message", "why");
        }
        response.body(()).unwrap()
    }

    async fn respond_with(
        mut respond: SendResponse<Bytes>,
        body: &'static [u8],
        grpc_status: Option<&'static str>,
    ) {
        let mut stream = respond.send_response(header(200, None), false).unwrap();
        stream.send_data(Bytes::from_static(body), false).unwrap();
        let mut trailers = HeaderMap::new();
        if let Some(code) = grpc_status {
            trailers.insert("grpc-status", code.parse().unwrap());
        }
        stream.send_trailers(trailers).unwrap();
    }

    fn sender(addr: SocketAddr, metric_sink: Arc<RecordedFailures>) -> ConnectorSender {
        let mut upstreams = StaticCalloutUpstreams::new();
        upstreams.insert("authz", HttpPeer::new(addr, false, String::new()));
        ConnectorSender {
            connector: Arc::new(Connector::new(None)),
            upstreams: Arc::new(upstreams),
            metric_sink,
        }
    }

    fn callout(upstream: &str, timeout: Duration) -> AcceptedCallout {
        let upstreams = Arc::new(StaticCalloutUpstreams::new());
        let conf = PluginCalloutConf::new("a", upstreams, timeout, timeout, LIMIT);
        let header = super::super::request_header(upstream, b"svc", b"Check", &vec![], None);
        AcceptedCallout {
            id: 1.try_into().unwrap(),
            plugin_conf: Arc::new(conf),
            upstream: upstream.to_string(),
            request: Box::new(header.unwrap()),
            body: frame::frame(b"ping"),
            timeout,
            callback: None,
            grpc: None,
        }
    }

    fn event_channel() -> (GrpcEventSender, mpsc::UnboundedReceiver<GrpcCalloutEvent>) {
        GrpcEventSender::new(super::super::GrpcCalloutHandle::new().0)
    }

    fn received(events: &mut mpsc::UnboundedReceiver<GrpcCalloutEvent>) -> Vec<GrpcCalloutEvent> {
        std::iter::from_fn(|| events.try_recv().ok()).collect()
    }

    #[tokio::test]
    async fn call_ends_with_status_of_each_outcome() {
        let close = |code, message: &str| GrpcCalloutEvent::close(code, message);
        let message = |body: &'static [u8]| GrpcCalloutEvent::Message(Bytes::from_static(body));
        let cases: [(&str, Handler, GrpcCalloutEvent, Option<CalloutFailure>); 12] = [
            (
                "message",
                |_, respond| respond_with(respond, b"\0\0\0\0\x04pong", Some("0")).boxed(),
                message(b"pong"),
                None,
            ),
            (
                "server error in header",
                |_, mut respond| {
                    drop(respond.send_response(header(200, Some("7")), true));
                    async {}.boxed()
                },
                close(7, "why"),
                None,
            ),
            (
                "error status in trailers",
                |_, respond| respond_with(respond, b"", Some("5")).boxed(),
                close(5, ""),
                None,
            ),
            (
                "http error",
                |_, mut respond| {
                    drop(respond.send_response(header(503, None), false));
                    async {}.boxed()
                },
                close(UNAVAILABLE, "HTTP status 503"),
                Some(CalloutFailure::NotGrpcResponse),
            ),
            (
                "header without grpc-status",
                |_, mut respond| {
                    drop(respond.send_response(header(200, None), true));
                    async {}.boxed()
                },
                close(UNKNOWN, "missing grpc-status"),
                Some(CalloutFailure::InvalidGrpcResponse),
            ),
            (
                "http error with grpc-status",
                |_, mut respond| {
                    drop(respond.send_response(header(503, Some("8")), false));
                    async {}.boxed()
                },
                close(8, "why"),
                None,
            ),
            (
                "no grpc-status",
                |_, respond| respond_with(respond, b"", None).boxed(),
                close(UNKNOWN, "missing grpc-status"),
                Some(CalloutFailure::InvalidGrpcResponse),
            ),
            (
                "ok with no message",
                |_, respond| respond_with(respond, b"", Some("0")).boxed(),
                close(INTERNAL, "call ended with no message"),
                Some(CalloutFailure::InvalidGrpcResponse),
            ),
            (
                "message over limit",
                |_, respond| respond_with(respond, b"\0\0\0\0\x09too large", Some("0")).boxed(),
                close(RESOURCE_EXHAUSTED, "message over callout_response_limit"),
                Some(CalloutFailure::ResponseTooLarge),
            ),
            (
                "compressed message",
                |_, respond| respond_with(respond, b"\x01\0\0\0\x04pong", Some("0")).boxed(),
                close(INTERNAL, "compressed message"),
                Some(CalloutFailure::FailedAfterHeader),
            ),
            (
                "reset after header",
                |_, mut respond| {
                    async move {
                        let mut stream = respond.send_response(header(200, None), false).unwrap();
                        // Reset once the client has read the header
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        stream.send_reset(Reason::INTERNAL_ERROR);
                    }
                    .boxed()
                },
                close(INTERNAL, "upstream reset or disconnect"),
                Some(CalloutFailure::FailedAfterHeader),
            ),
            (
                "no response",
                |_, respond| {
                    async move {
                        let _held = respond;
                        std::future::pending::<()>().await
                    }
                    .boxed()
                },
                close(status::DEADLINE_EXCEEDED, "deadline exceeded"),
                Some(CalloutFailure::Timeout),
            ),
        ];
        for (case, handler, want_event, want_failure) in cases {
            let metric_sink = Arc::new(RecordedFailures::default());
            let sender = sender(h2_server(handler).await, metric_sink.clone());
            let (events, mut receiver) = event_channel();
            let (_commands, commands_receiver) = mpsc::unbounded_channel();
            let callout = callout("authz", Duration::from_millis(500));

            sender
                .send_grpc_callout(callout, commands_receiver, false, events)
                .await;

            assert_eq!(received(&mut receiver), [want_event], "{case}");
            let want_failure: Vec<_> = want_failure.into_iter().collect();
            assert_eq!(metric_sink.callout_failures(), want_failure, "{case}");
        }
    }

    #[tokio::test]
    async fn call_without_peer_or_connection_is_unavailable() {
        let closed = TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap()
            .local_addr()
            .unwrap();
        let cases = [
            ("unknown", CalloutFailure::NoPeer, "no healthy upstream"),
            (
                "authz",
                CalloutFailure::ConnectFailed,
                "upstream connect error",
            ),
        ];
        for (upstream, want_failure, want_message) in cases {
            let metric_sink = Arc::new(RecordedFailures::default());
            let sender = sender(closed, metric_sink.clone());
            let (events, mut receiver) = event_channel();
            let (_commands, commands_receiver) = mpsc::unbounded_channel();
            let callout = callout(upstream, Duration::from_secs(1));

            sender
                .send_grpc_callout(callout, commands_receiver, false, events)
                .await;

            let want = GrpcCalloutEvent::close(UNAVAILABLE, want_message);
            assert_eq!(received(&mut receiver), [want], "{upstream}");
            assert_eq!(metric_sink.callout_failures(), [want_failure], "{upstream}");
        }
    }

    /// Echo each message of a stream and record what the server receives. Close with status 0
    /// after the client closes its side.
    fn echo_until_client_closes(
        request: Request<RecvStream>,
        mut respond: SendResponse<Bytes>,
        received: mpsc::UnboundedSender<String>,
    ) -> BoxFuture<'static, ()> {
        async move {
            let mut body = request.into_body();
            let mut stream = respond.send_response(header(200, None), false).unwrap();
            loop {
                match body.data().await {
                    Some(Ok(data)) if data.is_empty() => {}
                    Some(Ok(data)) => {
                        let message = String::from_utf8_lossy(&data[5..]).into_owned();
                        let _ = received.send(message);
                        stream.send_data(data, false).unwrap();
                    }
                    Some(Err(e)) => {
                        let reset = e.reason() == Some(Reason::CANCEL);
                        let _ = received.send(format!("reset={reset}"));
                        return;
                    }
                    None => break,
                }
            }
            let _ = received.send("client closed".to_string());
            let mut trailers = HeaderMap::new();
            trailers.insert("grpc-status", "0".parse().unwrap());
            stream.send_trailers(trailers).unwrap();
        }
        .boxed()
    }

    #[tokio::test]
    async fn closed_stream_reads_until_server_closes() {
        let first = Bytes::from_static(b"first");
        let send = |end_of_stream| GrpcCommand::Send {
            message: first.clone(),
            end_of_stream,
        };
        let cases = [
            ("close", vec![send(false), GrpcCommand::Close]),
            ("send with end of stream", vec![send(true)]),
        ];
        for (case, commands) in cases {
            let (seen, mut server_saw) = mpsc::unbounded_channel();
            let addr = h2_server(move |request, respond| {
                echo_until_client_closes(request, respond, seen.clone())
            })
            .await;
            let sender = sender(addr, Arc::default());
            let (events, mut receiver) = event_channel();
            let (queued, commands_receiver) = mpsc::unbounded_channel();
            for command in commands {
                queued.send(command).unwrap();
            }
            let callout = callout("authz", Duration::ZERO);

            sender
                .send_grpc_callout(callout, commands_receiver, true, events)
                .await;

            let trailers = vec![(b"grpc-status".to_vec(), b"0".to_vec())];
            let want = [
                GrpcCalloutEvent::InitialMetadata(Vec::new()),
                GrpcCalloutEvent::Message(first.clone()),
                GrpcCalloutEvent::TrailingMetadata(trailers),
                GrpcCalloutEvent::close(OK, ""),
            ];
            assert_eq!(received(&mut receiver), want, "{case}");
            assert_eq!(server_saw.recv().await.as_deref(), Some("first"), "{case}");
            let closed = server_saw.recv().await;
            assert_eq!(closed.as_deref(), Some("client closed"), "{case}");
        }
    }

    #[tokio::test]
    async fn cancelled_stream_resets_server_stream() {
        let (seen, mut server_saw) = mpsc::unbounded_channel();
        let addr = h2_server(move |request, respond| {
            echo_until_client_closes(request, respond, seen.clone())
        })
        .await;
        let sender = sender(addr, Arc::default());
        let (handle, _) = super::super::GrpcCalloutHandle::new();
        let (events, mut receiver) = GrpcEventSender::new(handle.clone());
        let (_commands, commands_receiver) = mpsc::unbounded_channel();
        let callout = callout("authz", Duration::ZERO);
        let task = tokio::spawn(async move {
            sender
                .send_grpc_callout(callout, commands_receiver, true, events)
                .await;
        });
        handle.set_task(task.abort_handle());
        let opened = receiver.recv().await;

        handle.cancel();

        assert!(matches!(opened, Some(GrpcCalloutEvent::InitialMetadata(_))));
        assert_eq!(server_saw.recv().await.as_deref(), Some("reset=true"));
    }
}
