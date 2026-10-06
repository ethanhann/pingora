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

use crate::utils::{eventually, fixture, guest_lines, raise_open_file_limit, GuestMessageSink};
use async_trait::async_trait;
use pingora_core::server::Server;
use pingora_core::services::background::background_service;
use pingora_core::services::listening::Service;
use pingora_core::upstreams::peer::BasicPeer;
use pingora_error::{Error, ErrorType, Result};
use pingora_wasm::{
    WasmPluginConf, WasmPlugins, WasmRuntime, WasmServices, WasmTcpConnection, WasmTcpProxy,
    WasmTcpUpstream,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// The upstream of a test proxy, or `None` for one whose `upstream_peer` fails.
struct Origin(Option<u16>);

#[async_trait]
impl WasmTcpUpstream for Origin {
    async fn upstream_peer(&self, _connection: &WasmTcpConnection) -> Result<Box<BasicPeer>> {
        match self.0 {
            Some(port) => Ok(Box::new(BasicPeer::new(&format!("127.0.0.1:{port}")))),
            None => Error::e_explain(ErrorType::ConnectRefused, "no origin for this test"),
        }
    }
}

/// Start an origin that reads the first five bytes of each connection, replies with `world`, and
/// reports the bytes it read once the proxy closes that connection.
///
/// A connection that sends fewer bytes, such as the check that the proxy listens, is ignored.
async fn reply_world() -> (u16, tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (received, receiver) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let received = received.clone();
            tokio::spawn(async move {
                let mut bytes = vec![0; 5];
                if stream.read_exact(&mut bytes).await.is_err() {
                    return;
                }
                stream.write_all(b"world").await.unwrap();
                let mut rest = Vec::new();
                let _ = stream.read_to_end(&mut rest).await;
                let _ = received.send(bytes);
            });
        }
    });
    (port, receiver)
}

/// Start a server with a TCP proxy on `port` that runs `exercise-all` as the plugin `name`.
async fn start_tcp_proxy(port: u16, name: &'static str, origin: Origin) {
    raise_open_file_limit();
    let mut plugin = WasmPluginConf::new(name, fixture("exercise-all"));
    plugin.root_id = "tcp".to_string();
    let mut services = WasmServices::default();
    services.log_sink = Arc::new(GuestMessageSink);
    let runtime = WasmRuntime::new_with_services(vec![plugin], services).unwrap();
    let plugins = WasmPlugins::new(runtime, [("tcp", [name])]).unwrap();
    let plugins_service = background_service("wasm plugins", plugins);
    let chain = plugins_service.task().chain("tcp").unwrap();
    std::thread::spawn(move || {
        let mut server = Server::new(None).unwrap();
        server.bootstrap();
        let mut proxy = Service::new("tcp".to_string(), WasmTcpProxy::new(chain, origin));
        proxy.add_tcp(&format!("127.0.0.1:{port}"));
        server.add_service(proxy);
        server.add_service(plugins_service);
        server.run_forever();
    });
    let listening = || std::net::TcpStream::connect(("127.0.0.1", port)).is_ok();
    assert!(
        eventually(listening).await,
        "TCP proxy on {port} did not start"
    );
}

async fn read_to_end(client: &mut TcpStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(5), client.read_to_end(&mut bytes));
    read.await.unwrap().unwrap();
    bytes
}

fn plugin_lines(name: &str) -> Vec<String> {
    let prefix = format!("{name}: ");
    let lines = guest_lines().into_iter();
    lines
        .filter_map(|line| line.strip_prefix(&prefix).map(str::to_string))
        .collect()
}

#[tokio::test]
async fn tcp_guest_changes_downstream_and_upstream_data() {
    let (origin_port, mut origin) = reply_world().await;
    start_tcp_proxy(6425, "exercise-tcp", Origin(Some(origin_port))).await;
    let mut client = TcpStream::connect(("127.0.0.1", 6425)).await.unwrap();
    client.write_all(b"hello").await.unwrap();

    let at_client = read_to_end(&mut client).await;

    assert_eq!(at_client, b"WORLD");
    assert_eq!(origin.recv().await.unwrap(), b"HELLO");
    let closes = || {
        let lines = plugin_lines("exercise-tcp");
        let has = |line: &str| lines.iter().any(|l| l == line);
        has("upstream_close peer=Local") && has("downstream_close peer=Local")
    };
    assert!(eventually(closes).await, "{:?}", guest_lines());
}

#[tokio::test]
async fn failed_upstream_peer_closes_downstream() {
    start_tcp_proxy(6426, "exercise-tcp-refused", Origin(None)).await;
    let mut client = TcpStream::connect(("127.0.0.1", 6426)).await.unwrap();

    let at_client = read_to_end(&mut client).await;

    assert_eq!(at_client, b"");
    let closed = || {
        let lines = plugin_lines("exercise-tcp-refused");
        lines.iter().filter(|l| l.contains("close")).count() >= 1
    };
    assert!(eventually(closed).await, "{:?}", guest_lines());
    let closes: Vec<String> = plugin_lines("exercise-tcp-refused")
        .into_iter()
        .filter(|line| line.contains("close"))
        .collect();
    assert!(
        closes
            .iter()
            .all(|line| line == "downstream_close peer=Local"),
        "{closes:?}"
    );
}
