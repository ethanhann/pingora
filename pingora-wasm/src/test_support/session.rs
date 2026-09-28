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

//! Sessions for tests, and what their downstream receives.

use pingora_proxy::Session;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

/// Build a session that has read `request`, and return it with the client end of its
/// connection.
pub(crate) async fn session(request: &[u8]) -> (Session, DuplexStream) {
    let (mut client, server) = tokio::io::duplex(4096);
    client.write_all(request).await.unwrap();
    let mut session = Session::new_h1(Box::new(server));
    session.read_request().await.unwrap();
    (session, client)
}

pub(crate) const GET: &[u8] = b"GET /original HTTP/1.1\r\nHost: example.test\r\n\r\n";
pub(crate) const POST: &[u8] =
    b"POST /original HTTP/1.1\r\nHost: example.test\r\nContent-Length: 100\r\n\r\n";
pub(crate) const HEAD: &[u8] = b"HEAD /original HTTP/1.1\r\nHost: example.test\r\n\r\n";
pub(crate) const UPGRADE: &[u8] = b"GET /original HTTP/1.1\r\nHost: example.test\r\n\
Connection: upgrade\r\nUpgrade: websocket\r\n\r\n";

/// Write a response with the status 204 to the session, and return what the downstream received.
///
/// When the text starts with [MARKER_RESPONSE], nothing was written before the 204.
pub(crate) async fn read_downstream_after_marker(
    session: &mut Session,
    client: &mut DuplexStream,
) -> String {
    let marker = pingora_http::ResponseHeader::build(204, None).unwrap();
    session
        .write_response_header(Box::new(marker), true)
        .await
        .unwrap();
    read_downstream(client).await
}

pub(crate) const MARKER_RESPONSE: &str = "HTTP/1.1 204";

/// Return what the downstream received so far, as text.
pub(crate) async fn read_downstream(client: &mut DuplexStream) -> String {
    let mut all = Vec::new();
    let mut part = [0u8; 1024];
    while let Ok(Ok(n)) =
        tokio::time::timeout(Duration::from_millis(50), client.read(&mut part)).await
    {
        if n == 0 {
            break;
        }
        all.extend_from_slice(&part[..n]);
    }
    String::from_utf8_lossy(&all).into_owned()
}
