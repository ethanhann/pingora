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

//! Raw H1 client
//!
//! Requests are written straight to a TCP stream, so a test controls how the body is split into
//! chunks and when the connection closes.

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const CHUNK_PAUSE: Duration = Duration::from_millis(50);
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);
const END_OF_CHUNKS: &str = "0\r\n\r\n";

/// A response as read off the wire.
pub struct RawResponse {
    pub status: u16,
    pub head: String,
    pub body: String,
}

fn parse(response: &[u8]) -> RawResponse {
    let text = String::from_utf8_lossy(response);
    let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    let status = head.split(' ').nth(1).and_then(|code| code.parse().ok());
    RawResponse {
        status: status.unwrap_or_else(|| panic!("no status code in response {text:?}")),
        head: head.to_ascii_lowercase(),
        body: body.to_string(),
    }
}

/// Read until `done` accepts the bytes read so far, the peer closes, or `timeout` elapses.
async fn read_until(stream: &mut TcpStream, timeout: Duration, done: fn(&[u8]) -> bool) -> Vec<u8> {
    let mut response = Vec::new();
    let mut part = [0u8; 4096];
    let read = async {
        while !done(&response) {
            match stream.read(&mut part).await {
                Ok(n) if n > 0 => response.extend_from_slice(&part[..n]),
                _ => break,
            }
        }
    };
    let _ = tokio::time::timeout(timeout, read).await;
    response
}

fn never(_: &[u8]) -> bool {
    false
}

fn chunked_body_ended(response: &[u8]) -> bool {
    response.ends_with(END_OF_CHUNKS.as_bytes())
}

/// Send a request with a chunked body to the proxy on `port` and return the response.
///
/// Chunks are written one at a time with a short wait after each, so the proxy runs its body
/// filter once per chunk. Sending stops as soon as any response bytes arrive.
pub async fn send_chunked_request(
    port: u16,
    origin: u16,
    method: &str,
    headers: &[(&str, &str)],
    chunks: &[&str],
) -> RawResponse {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut head = format!(
        "{method} / HTTP/1.1\r\nHost: example.test\r\nConnection: close\r\n\
         Transfer-Encoding: chunked\r\nx-return-body: 1\r\nx-test-origin: {origin}\r\n"
    );
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    for chunk in chunks.iter().chain(&[""]) {
        let framed = format!("{:x}\r\n{chunk}\r\n", chunk.len());
        if stream.write_all(framed.as_bytes()).await.is_err() {
            break;
        }
        let _ = stream.flush().await;
        response = read_until(&mut stream, CHUNK_PAUSE, never).await;
        if !response.is_empty() {
            break;
        }
    }
    response.extend(read_until(&mut stream, RESPONSE_TIMEOUT, never).await);
    parse(&response)
}

/// Send a GET to the proxy on `port` and return the connection without reading the response.
pub async fn send_get_without_reading(port: u16, origin: u16) -> TcpStream {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let request =
        format!("GET / HTTP/1.1\r\nHost: example.test\r\nx-test-origin: {origin}\r\n\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();
    stream
}

/// Send one POST per body over a single connection and return the responses.
///
/// Each response is read up to the end of its chunked body, so the proxy must respond with
/// chunked encoding.
pub async fn post_on_one_connection(port: u16, origin: u16, bodies: &[&str]) -> Vec<RawResponse> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut responses = Vec::new();
    for body in bodies {
        let request = format!(
            "POST / HTTP/1.1\r\nHost: example.test\r\nContent-Length: {}\r\n\
             x-return-body: 1\r\nx-test-origin: {origin}\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(request.as_bytes()).await.unwrap();
        let response = read_until(&mut stream, RESPONSE_TIMEOUT, chunked_body_ended).await;
        responses.push(parse(&response));
    }
    responses
}

/// Decode a chunked body, stopping at the last chunk or at the first incomplete one.
pub fn decode_chunked_body(body: &str) -> String {
    let mut all = String::new();
    let mut rest = body;
    while let Some((size, after)) = rest.split_once("\r\n") {
        let size = usize::from_str_radix(size, 16).unwrap_or(0);
        if size == 0 || after.len() < size {
            break;
        }
        all.push_str(&after[..size]);
        rest = after[size..].trim_start_matches("\r\n");
    }
    all
}
