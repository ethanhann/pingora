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

//! gRPC origin

use bytes::{Bytes, BytesMut};
use h2::server::SendResponse;
use h2::RecvStream;
use http::{HeaderMap, Request, Response};
use once_cell::sync::Lazy;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;

static GRPC_RUNTIME: Lazy<Runtime> = Lazy::new(|| Runtime::new().unwrap());

/// A gRPC origin over plain text HTTP/2 that serves three methods:
///
/// - `grpcbin.GRPCBin/RandomError` ends each call with the next status of `set_statuses`, in
///   the response header.
/// - `exercise.Echo/Say` responds with the message it received.
/// - `exercise.Echo/Chat` sends the metadata `x-chat: open`, echoes each message, and ends with
///   status 0 when the client closes its side.
pub struct GrpcOrigin {
    addr: SocketAddr,
    statuses: Mutex<VecDeque<u32>>,
    /// The method and message of each message received, and `Chat closed` for a closed stream.
    received: Mutex<Vec<String>>,
}

impl GrpcOrigin {
    pub fn start() -> Arc<Self> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let origin = Arc::new(GrpcOrigin {
            addr: listener.local_addr().unwrap(),
            statuses: Mutex::new(VecDeque::new()),
            received: Mutex::new(Vec::new()),
        });
        let serving = origin.clone();
        GRPC_RUNTIME.spawn(async move {
            let listener = TcpListener::from_std(listener).unwrap();
            while let Ok((stream, _)) = listener.accept().await {
                let serving = serving.clone();
                tokio::spawn(async move {
                    let Ok(mut connection) = h2::server::handshake(stream).await else {
                        return;
                    };
                    while let Some(Ok((request, respond))) = connection.accept().await {
                        tokio::spawn(serving.clone().serve(request, respond));
                    }
                });
            }
        });
        origin
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn set_statuses(&self, statuses: &[u32]) {
        *self.statuses.lock().unwrap() = statuses.iter().copied().collect();
    }

    pub fn received(&self) -> Vec<String> {
        self.received.lock().unwrap().clone()
    }

    async fn serve(
        self: Arc<Self>,
        request: Request<RecvStream>,
        mut respond: SendResponse<Bytes>,
    ) {
        let method = request
            .uri()
            .path()
            .rsplit('/')
            .next()
            .unwrap_or_default()
            .to_string();
        let mut body = request.into_body();
        if method == "RandomError" {
            let code = self.statuses.lock().unwrap().pop_front().unwrap_or(2);
            let header = Response::builder()
                .header("content-type", "application/grpc")
                .header("grpc-status", code.to_string())
                .body(())
                .unwrap();
            let _ = respond.send_response(header, true);
            return;
        }
        let mut header = Response::builder().header("content-type", "application/grpc");
        if method == "Chat" {
            header = header.header("x-chat", "open");
        }
        let Ok(mut stream) = respond.send_response(header.body(()).unwrap(), false) else {
            return;
        };
        let mut buffer = BytesMut::new();
        while let Some(Ok(data)) = body.data().await {
            let _ = body.flow_control().release_capacity(data.len());
            buffer.extend_from_slice(&data);
            while buffer.len() >= 5 {
                let length = u32::from_be_bytes([buffer[1], buffer[2], buffer[3], buffer[4]]);
                let framed_length = 5 + length as usize;
                if buffer.len() < framed_length {
                    break;
                }
                let framed = buffer.split_to(framed_length).freeze();
                let message = String::from_utf8_lossy(&framed[5..]).into_owned();
                self.received
                    .lock()
                    .unwrap()
                    .push(format!("{method} {message}"));
                let _ = stream.send_data(framed, false);
            }
        }
        if method == "Chat" {
            self.received
                .lock()
                .unwrap()
                .push("Chat closed".to_string());
        }
        let mut trailers = HeaderMap::new();
        trailers.insert("grpc-status", "0".parse().unwrap());
        let _ = stream.send_trailers(trailers);
    }
}
