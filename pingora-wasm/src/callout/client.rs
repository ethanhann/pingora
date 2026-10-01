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

//! The client that sends a callout to its peer.

use super::result::{connect_failure, session_failure, OwnedHeaderPairs, PSEUDO_STATUS};
use super::{AcceptedCallout, CalloutResult, CalloutTarget, CalloutUpstreams};
use crate::observability::{CalloutFailure, WasmMetricSink};
use crate::WasmServices;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use http::StatusCode;
use log::debug;
use pingora_core::connectors::http::Connector;
use pingora_core::protocols::http::client::HttpSession;
use pingora_core::upstreams::peer::{HttpPeer, Peer};
use pingora_error::{Error, ErrorType, Result};
use pingora_timeout::timeout;
use std::sync::Arc;
use std::time::Instant;

/// The interface that sends a callout and returns its result.
///
/// The runtime sends callouts through a [ConnectorSender]. Tests of the phases replace it, so
/// that they need no socket.
#[async_trait]
pub(crate) trait CalloutSender: Send + Sync {
    async fn send(&self, callout: AcceptedCallout) -> CalloutResult;
}

/// A sender that sends each callout through a Pingora connector.
///
/// It reports each failure to the metric sink.
pub(crate) struct ConnectorSender {
    pub(crate) connector: Arc<Connector>,
    pub(crate) upstreams: Arc<dyn CalloutUpstreams>,
    pub(crate) metric_sink: Arc<dyn WasmMetricSink>,
}

/// A session whose response header arrived.
struct ResponseInProgress {
    session: HttpSession,
    peer: Box<HttpPeer>,
    headers: OwnedHeaderPairs,
}

impl ConnectorSender {
    pub(crate) fn new(connector: Arc<Connector>, services: &WasmServices) -> Self {
        ConnectorSender {
            connector,
            upstreams: services.callout_upstreams.clone(),
            metric_sink: services.metric_sink.clone(),
        }
    }
}

#[async_trait]
impl CalloutSender for ConnectorSender {
    async fn send(&self, callout: AcceptedCallout) -> CalloutResult {
        let plugin_name = &callout.plugin_conf.plugin_name;
        let report_failure = |failure: CalloutFailure, result: CalloutResult| {
            self.metric_sink.callout_failed(plugin_name, failure);
            result
        };
        let deadline = Instant::now() + callout.timeout;
        let mut response = match timeout(callout.timeout, self.send_to_peer(&callout)).await {
            Ok(Ok(response)) => response,
            Ok(Err((failure, synthetic_response))) => {
                return report_failure(failure, synthetic_response)
            }
            Err(_) => {
                return report_failure(CalloutFailure::Timeout, CalloutResult::timeout_response())
            }
        };
        let remaining = deadline.saturating_duration_since(Instant::now());
        let limit = callout.plugin_conf.response_limit;
        let body_and_trailers = read_body_and_trailers(&mut response.session, limit);
        let (body, trailers) = match timeout(remaining, body_and_trailers).await {
            Ok(Ok(Some(body_and_trailers))) => body_and_trailers,
            Ok(Ok(None)) => {
                return report_failure(CalloutFailure::ResponseTooLarge, CalloutResult::Failed)
            }
            _ => return report_failure(CalloutFailure::FailedAfterHeader, CalloutResult::Failed),
        };
        let idle_timeout = response.peer.idle_timeout();
        self.connector
            .release_http_session(response.session, &*response.peer, idle_timeout)
            .await;
        CalloutResult::Response {
            headers: response.headers,
            body,
            trailers,
        }
    }
}

impl ConnectorSender {
    /// Send the request of `callout` to a peer of its upstream, and read the response header.
    ///
    /// Return the failure, and the response to give to the plugin, when no response header
    /// arrives.
    async fn send_to_peer(
        &self,
        callout: &AcceptedCallout,
    ) -> Result<ResponseInProgress, (CalloutFailure, CalloutResult)> {
        let plugin = &callout.plugin_conf.plugin_name;
        let upstream = &callout.upstream;
        let target = CalloutTarget::new(plugin, upstream, &callout.request);
        let peer = match self.upstreams.callout_peer(&target).await {
            Ok(peer) => peer,
            Err(e) => {
                debug!("wasm plugin {plugin} has no peer for the callout upstream {upstream}: {e}");
                let response = CalloutResult::no_healthy_upstream_response();
                return Err((CalloutFailure::NoPeer, response));
            }
        };
        let mut may_retry = true;
        loop {
            let (mut session, reused) = match self.connector.get_http_session(&*peer).await {
                Ok(connected) => connected,
                Err(e) => {
                    debug!("wasm plugin {plugin} cannot connect to the callout upstream {upstream}: {e}");
                    let response = CalloutResult::connect_failure_response(&e);
                    return Err((connect_failure(&e), response));
                }
            };
            session.set_write_timeout(peer.options.write_timeout);
            session.set_read_timeout(peer.options.read_timeout);
            let e = match write_request_and_read_response_header(&mut session, callout).await {
                Ok(headers) => {
                    return Ok(ResponseInProgress {
                        session,
                        peer,
                        headers,
                    })
                }
                Err(e) => e,
            };
            // A peer can close a pooled connection at any time, so try once more, as Pingora
            // does for a proxied request
            if reused && may_retry {
                may_retry = false;
                continue;
            }
            debug!("the callout of wasm plugin {plugin} to the upstream {upstream} failed: {e}");
            let response = CalloutResult::response_for_session_error(&e);
            return Err((session_failure(&e), response));
        }
    }
}

/// Write the request of `callout`, and read the response header as the pairs that the plugin
/// reads.
async fn write_request_and_read_response_header(
    session: &mut HttpSession,
    callout: &AcceptedCallout,
) -> Result<OwnedHeaderPairs> {
    session
        .write_request_header(callout.request.clone())
        .await?;
    if !callout.body.is_empty() {
        session
            .write_request_body(callout.body.clone(), true)
            .await?;
    }
    session.finish_request_body().await?;
    loop {
        session.read_response_header().await?;
        let Some(header) = session.response_header() else {
            return Error::e_explain(ErrorType::ReadError, "no callout response header");
        };
        // A second header read panics on an H2 session, so only an H1 session skips
        // informational responses
        let informational = header.status.is_informational()
            && header.status != StatusCode::SWITCHING_PROTOCOLS
            && matches!(session, HttpSession::H1(_));
        if informational {
            continue;
        }
        let status = header.status.as_str().as_bytes().to_vec();
        let mut pairs = Vec::with_capacity(header.headers.len() + 1);
        pairs.push((PSEUDO_STATUS.to_vec(), status));
        for (name, value) in &header.headers {
            pairs.push((name.as_str().as_bytes().to_vec(), value.as_bytes().to_vec()));
        }
        return Ok(pairs);
    }
}

/// Read the response body and the trailers. Return `None` for a body over `limit`.
async fn read_body_and_trailers(
    session: &mut HttpSession,
    limit: usize,
) -> Result<Option<(Bytes, OwnedHeaderPairs)>> {
    let mut body = BytesMut::new();
    while let Some(chunk) = session.read_response_body().await? {
        if body.len() + chunk.len() > limit {
            return Ok(None);
        }
        body.extend_from_slice(&chunk);
    }
    let HttpSession::H2(h2) = session else {
        return Ok(Some((body.freeze(), Vec::new())));
    };
    let trailers = h2.read_trailers().await?.unwrap_or_default();
    let pairs = trailers
        .iter()
        .map(|(name, value)| (name.as_str().as_bytes().to_vec(), value.as_bytes().to_vec()))
        .collect();
    Ok(Some((body.freeze(), pairs)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::callout::headers::tests::{pairs, post_to_authz};
    use crate::callout::{PluginCalloutConf, StaticCalloutUpstreams};
    use crate::observability::NoMetricSink;
    use parking_lot::Mutex;
    use std::net::SocketAddr;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    const RESPONSE_LIMIT: usize = 1024;
    const NO_TIMEOUT_EXPECTED: Duration = Duration::from_secs(5);
    const SHORT_TIMEOUT: Duration = Duration::from_millis(100);
    const OK_RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
    const RESET_BODY_PREFIX: &str =
        "upstream connect error or disconnect/reset before headers. reset reason: ";

    /// What an origin does after it wrote its response.
    #[derive(Clone, Copy)]
    enum AfterResponse {
        Close,
        KeepOpen,
    }

    /// Read one request with a body of `body_len` bytes, and return it.
    async fn read_request(stream: &mut TcpStream, body_len: usize) -> Vec<u8> {
        let mut request = Vec::new();
        let mut part = [0u8; 1024];
        loop {
            let head_end = request.windows(4).position(|w| w == b"\r\n\r\n");
            if head_end.is_some_and(|end| request.len() >= end + 4 + body_len) {
                return request;
            }
            match stream.read(&mut part).await {
                Ok(n) if n > 0 => request.extend_from_slice(&part[..n]),
                _ => return request,
            }
        }
    }

    /// Start an origin that reads one request and writes `response`.
    async fn start_h1_origin(response: &'static [u8], then: AfterResponse) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            read_request(&mut stream, 0).await;
            stream.write_all(response).await.unwrap();
            if let AfterResponse::KeepOpen = then {
                std::future::pending::<()>().await;
            }
        });
        addr
    }

    /// Return the address of a port that refuses connections.
    async fn closed_port() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap()
    }

    fn authz_peer(addr: SocketAddr) -> HttpPeer {
        HttpPeer::new(addr, false, String::new())
    }

    fn sender_to(peer: HttpPeer) -> ConnectorSender {
        let mut upstreams = StaticCalloutUpstreams::new();
        upstreams.insert("authz", peer);
        ConnectorSender {
            connector: Arc::new(Connector::new(None)),
            upstreams: Arc::new(upstreams),
            metric_sink: Arc::new(NoMetricSink),
        }
    }

    /// Build a POST callout to the upstream `authz` with `body`.
    fn post_callout(body: &'static str, timeout: Duration) -> AcceptedCallout {
        let upstreams = Arc::new(StaticCalloutUpstreams::new());
        let conf = PluginCalloutConf::new("a", upstreams, timeout, RESPONSE_LIMIT);
        let headers = pairs(&post_to_authz());
        let request = crate::callout::headers::callout_request_header("a", &headers, body.len());
        AcceptedCallout {
            id: 1.try_into().unwrap(),
            plugin_conf: Arc::new(conf),
            upstream: "authz".to_string(),
            request: Box::new(request.unwrap()),
            body: Bytes::from_static(body.as_bytes()),
            timeout,
        }
    }

    async fn send_to(addr: SocketAddr, timeout: Duration) -> CalloutResult {
        let callout = post_callout("", timeout);
        sender_to(authz_peer(addr)).send(callout).await
    }

    /// Return the status and the body of a response, and check that `:status` is its first
    /// header.
    fn status_and_body(result: &CalloutResult) -> (String, String) {
        let CalloutResult::Response { headers, body, .. } = result else {
            return ("failed".to_string(), String::new());
        };
        let (name, status) = &headers[0];
        assert_eq!(name, b":status");
        (
            String::from_utf8_lossy(status).into_owned(),
            String::from_utf8_lossy(body).into_owned(),
        )
    }

    #[tokio::test]
    async fn a_callout_returns_the_response_of_its_peer() {
        use AfterResponse::{Close, KeepOpen};
        let cases: [(&[u8], AfterResponse, &str, &str); 4] = [
            (OK_RESPONSE, KeepOpen, "200", "ok"),
            (
                b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n",
                KeepOpen,
                "403",
                "",
            ),
            (
                b"HTTP/1.1 103 Early Hints\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok",
                KeepOpen,
                "200",
                "ok",
            ),
            (
                b"HTTP/1.1 200 OK\r\n\r\nuntil the close",
                Close,
                "200",
                "until the close",
            ),
        ];

        for (response, then, status, body) in cases {
            let addr = start_h1_origin(response, then).await;

            let result = send_to(addr, NO_TIMEOUT_EXPECTED).await;

            let want = (status.to_string(), body.to_string());
            assert_eq!(status_and_body(&result), want);
        }
    }

    #[tokio::test]
    async fn a_callout_sends_its_body_with_its_length() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let received = Arc::new(Mutex::new(String::new()));
        let origin_received = received.clone();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_request(&mut stream, 4).await;
            *origin_received.lock() = String::from_utf8_lossy(&request).to_ascii_lowercase();
            stream.write_all(OK_RESPONSE).await.unwrap();
            std::future::pending::<()>().await;
        });
        let callout = post_callout("body", NO_TIMEOUT_EXPECTED);

        let result = sender_to(authz_peer(addr)).send(callout).await;

        assert_eq!(status_and_body(&result).0, "200");
        let received = received.lock();
        assert!(
            received.starts_with("post /check?dry=1 http/1.1\r\n"),
            "{received}"
        );
        assert!(received.contains("\r\nhost: authz.test\r\n"), "{received}");
        assert!(received.contains("\r\ncontent-length: 4\r\n"), "{received}");
        assert!(received.ends_with("\r\n\r\nbody"), "{received}");
    }

    #[tokio::test]
    async fn a_callout_with_no_response_header_returns_a_synthetic_response() {
        let reset = |reason: &str| format!("{RESET_BODY_PREFIX}{reason}");
        let refused = closed_port().await;
        let closes = start_h1_origin(b"", AfterResponse::Close).await;
        let silent = start_h1_origin(b"", AfterResponse::KeepOpen).await;
        let garbage = start_h1_origin(b"not http\r\n\r\n", AfterResponse::KeepOpen).await;
        let cases = [
            (
                refused,
                NO_TIMEOUT_EXPECTED,
                "503",
                reset("remote connection failure"),
            ),
            (
                closes,
                NO_TIMEOUT_EXPECTED,
                "503",
                reset("connection termination"),
            ),
            (
                silent,
                SHORT_TIMEOUT,
                "504",
                "upstream request timeout".to_string(),
            ),
            (garbage, NO_TIMEOUT_EXPECTED, "502", reset("protocol error")),
        ];

        for (addr, timeout, status, body) in cases {
            let result = send_to(addr, timeout).await;

            assert_eq!(status_and_body(&result), (status.to_string(), body));
            let CalloutResult::Response { headers, body, .. } = result else {
                panic!("no response");
            };
            let names: Vec<_> = headers.iter().map(|(name, _)| &name[..]).collect();
            assert_eq!(names, [&b":status"[..], b"content-length", b"content-type"]);
            assert_eq!(headers[1].1, body.len().to_string().into_bytes());
        }
    }

    #[tokio::test]
    async fn the_read_timeout_of_the_peer_gives_a_timeout_response() {
        let silent = start_h1_origin(b"", AfterResponse::KeepOpen).await;
        let mut peer = authz_peer(silent);
        peer.options.read_timeout = Some(SHORT_TIMEOUT);
        let callout = post_callout("", NO_TIMEOUT_EXPECTED);

        let result = sender_to(peer).send(callout).await;

        let want = ("504".to_string(), "upstream request timeout".to_string());
        assert_eq!(status_and_body(&result), want);
    }

    #[tokio::test]
    async fn a_callout_that_fails_after_its_response_header_has_no_response() {
        let header: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n";
        let over_the_limit = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: 2000\r\n\r\n{}",
            "x".repeat(2000)
        );
        let large: &[u8] = over_the_limit.leak().as_bytes();
        let one_second = Duration::from_secs(1);
        let cases = [
            (header, AfterResponse::KeepOpen, one_second),
            (header, AfterResponse::Close, NO_TIMEOUT_EXPECTED),
            (large, AfterResponse::KeepOpen, NO_TIMEOUT_EXPECTED),
        ];

        for (response, then, timeout) in cases {
            let addr = start_h1_origin(response, then).await;

            let result = send_to(addr, timeout).await;

            assert_eq!(result, CalloutResult::Failed);
        }
    }

    /// Start an origin that closes its first connection after one response and a second
    /// request, and responds on its second connection.
    async fn start_origin_that_closes_a_pooled_connection() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut pooled, _) = listener.accept().await.unwrap();
            read_request(&mut pooled, 0).await;
            pooled.write_all(OK_RESPONSE).await.unwrap();
            read_request(&mut pooled, 0).await;
            drop(pooled);
            let (mut new, _) = listener.accept().await.unwrap();
            read_request(&mut new, 0).await;
            new.write_all(OK_RESPONSE).await.unwrap();
            std::future::pending::<()>().await;
        });
        addr
    }

    #[tokio::test]
    async fn a_callout_on_a_pooled_connection_that_closed_uses_a_new_connection() {
        let addr = start_origin_that_closes_a_pooled_connection().await;
        let sender = sender_to(authz_peer(addr));
        let first = sender.send(post_callout("", NO_TIMEOUT_EXPECTED)).await;

        let second = sender.send(post_callout("", NO_TIMEOUT_EXPECTED)).await;

        let ok = ("200".to_string(), "ok".to_string());
        assert_eq!(status_and_body(&first), ok);
        assert_eq!(status_and_body(&second), ok);
    }

    struct NoHealthyPeer;

    #[async_trait]
    impl CalloutUpstreams for NoHealthyPeer {
        fn has_upstream(&self, _plugin_name: &str, _upstream_name: &str) -> bool {
            true
        }

        async fn callout_peer(&self, _target: &CalloutTarget<'_>) -> Result<Box<HttpPeer>> {
            Error::e_explain(ErrorType::ConnectNoRoute, "every backend is unhealthy")
        }
    }

    #[tokio::test]
    async fn a_callout_with_no_peer_returns_a_synthetic_response() {
        let sender = ConnectorSender {
            connector: Arc::new(Connector::new(None)),
            upstreams: Arc::new(NoHealthyPeer),
            metric_sink: Arc::new(NoMetricSink),
        };

        let result = sender.send(post_callout("", NO_TIMEOUT_EXPECTED)).await;

        let want = ("503".to_string(), "no healthy upstream".to_string());
        assert_eq!(status_and_body(&result), want);
    }

    /// Start an HTTP/2 origin that responds with a body and a trailer.
    async fn start_h2_origin() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut connection = h2::server::handshake(stream).await.unwrap();
            while let Some(Ok((_request, mut respond))) = connection.accept().await {
                let response = http::Response::builder().status(200).body(()).unwrap();
                let mut send = respond.send_response(response, false).unwrap();
                send.send_data(Bytes::from_static(b"ok"), false).unwrap();
                let mut trailers = http::HeaderMap::new();
                trailers.insert("x-checked", "yes".parse().unwrap());
                send.send_trailers(trailers).unwrap();
            }
        });
        addr
    }

    #[tokio::test]
    async fn a_callout_to_an_h2_peer_returns_its_trailers() {
        let mut peer = authz_peer(start_h2_origin().await);
        peer.options.set_http_version(2, 2);
        let callout = post_callout("", NO_TIMEOUT_EXPECTED);

        let result = sender_to(peer).send(callout).await;

        let CalloutResult::Response {
            headers,
            body,
            trailers,
        } = result
        else {
            panic!("no response");
        };
        assert_eq!(headers[0], (b":status".to_vec(), b"200".to_vec()));
        assert_eq!(&body[..], b"ok");
        assert_eq!(trailers, [(b"x-checked".to_vec(), b"yes".to_vec())]);
    }
}
