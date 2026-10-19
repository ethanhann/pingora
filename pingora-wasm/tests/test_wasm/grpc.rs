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

//! gRPC callout tests
//!
//! Covers a gRPC call from a request, the gRPC status of a plugin response, the gRPC callouts of
//! a root context, and foreign functions.

use super::get;
use crate::utils::{echo_origin, eventually, grpc_origin, guest_messages, init};
use bytes::Bytes;
use http::{HeaderMap, Request};
use tokio::net::TcpStream;

struct GrpcResponse {
    status: u16,
    headers: HeaderMap,
    /// Whether the response ended with its header block.
    ended_on_header: bool,
    trailers: Option<HeaderMap>,
}

/// Send a gRPC request to the proxy on `port` over plain text HTTP/2.
async fn grpc_request(port: u16, origin: u16) -> GrpcResponse {
    let stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let (mut client, connection) = h2::client::handshake(stream).await.unwrap();
    tokio::spawn(connection);
    let request = Request::post(format!("http://127.0.0.1:{port}/example.Service/Do"))
        .header("content-type", "application/grpc")
        .header("x-test-origin", origin.to_string())
        .body(())
        .unwrap();
    let (response, mut request_body) = client.send_request(request, false).unwrap();
    request_body
        .send_data(Bytes::from_static(b"\0\0\0\0\0"), true)
        .unwrap();
    let response = response.await.unwrap();
    let status = response.status().as_u16();
    let headers = response.headers().clone();
    let ended_on_header = response.body().is_end_stream();
    let mut body = response.into_body();
    while let Some(chunk) = body.data().await {
        let _ = body.flow_control().release_capacity(chunk.unwrap().len());
    }
    let trailers = body.trailers().await.unwrap();
    GrpcResponse {
        status,
        headers,
        ended_on_header,
        trailers,
    }
}

#[tokio::test]
async fn grpc_callout_decides_grpc_request() {
    init().await;
    let (origin, _) = echo_origin().await;
    let origin = origin.addr().port();
    grpc_origin().set_statuses(&[2, 3]);

    let (allowed, denied) = (
        grpc_request(6423, origin).await,
        grpc_request(6423, origin).await,
    );

    assert_eq!(allowed.status, 200);
    assert_eq!(allowed.headers["powered-by"], "proxy-wasm");
    assert!(!allowed.headers.contains_key("grpc-status"));
    assert_eq!(denied.status, 200);
    assert!(denied.ended_on_header);
    assert!(denied.trailers.is_none());
    assert_eq!(denied.headers["content-type"], "application/grpc");
    assert_eq!(denied.headers["grpc-status"], "10");
    assert_eq!(denied.headers["grpc-message"], "Aborted by Proxy-Wasm!");
}

#[tokio::test]
async fn root_grpc_callouts_and_foreign_function_reach_plugin() {
    init().await;
    let (origin, _) = echo_origin().await;
    let lines = || {
        let messages = guest_messages("exercise").into_iter();
        messages
            .map(|message| String::from_utf8(message).unwrap())
            .collect::<Vec<_>>()
    };
    let logged = |prefix: &str, suffix: &str| {
        let matches = |line: &String| line.starts_with(prefix) && line.ends_with(suffix);
        lines().iter().any(matches)
    };

    let response = get(6424, "/", origin.addr().port(), &[]).await;

    assert_eq!(response.status(), 200);
    let wanted = [
        ("foreign_call", "answer=echo ping"),
        ("grpc_response", "status=0 message= body=hello"),
        ("grpc_initial", "x-chat:open"),
        ("grpc_message", "body=first"),
        ("grpc_trailing", "pairs=grpc-status:0"),
        ("grpc_stream_close", "code=0 status=0 message="),
        ("grpc_cancel", ""),
    ];
    for (prefix, suffix) in wanted {
        let found = eventually(|| logged(prefix, suffix)).await;
        assert!(found, "{prefix} ... {suffix} not in {:?}", lines());
    }
    let received = grpc_origin().received();
    let says = received.iter().filter(|r| r.starts_with("Say")).count();
    assert_eq!(
        says, 1,
        "the call cancelled in its own tick is not sent: {received:?}"
    );
    for want in ["Say hello", "Chat first", "Chat closed"] {
        assert!(
            received.iter().any(|r| r == want),
            "{want} not in {received:?}"
        );
    }
}
