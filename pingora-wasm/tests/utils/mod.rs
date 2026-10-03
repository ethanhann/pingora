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

pub mod callout_origins;
pub mod guests;
pub mod proxy;
pub mod raw;
mod services;

pub use services::{callout_origin, metrics_text};

use bytes::Bytes;
use http::{Request, Response};
use once_cell::sync::{Lazy, OnceCell};
use pingora_core::apps::HttpServerOptions;
use pingora_core::server::Server;
use pingora_test_utils::http_origin::HttpOrigin;
use pingora_wasm::{LogContext, LogLevel, LogSink, WasmRuntime};
use proxy::TestProxy;
use services::services;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

pub const FIRST_PORT: u16 = 6380;
pub const LAST_PORT: u16 = 6420;

pub fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(format!("{name}.wasm"))
}

/// A guest log message paired with the name of its plugin.
type GuestMessage = (String, Vec<u8>);

/// Every guest log message recorded so far.
static GUEST_MESSAGES: Lazy<Mutex<Vec<GuestMessage>>> = Lazy::new(|| Mutex::new(Vec::new()));

/// The log sink shared by every runtime of the test server.
///
/// Messages are recorded under the plugin name, or under the VM id when there is none.
pub struct GuestMessageSink;

impl LogSink for GuestMessageSink {
    fn log(&self, context: LogContext<'_>, _level: LogLevel, message: &[u8]) {
        let plugin_name = context.plugin_name.as_deref().unwrap_or(&context.vm_id);
        let plugin_name = String::from_utf8_lossy(plugin_name).into_owned();
        GUEST_MESSAGES
            .lock()
            .unwrap()
            .push((plugin_name, message.to_vec()));
    }
}

/// Return every guest log line so far, formatted as `plugin_name: message`.
pub fn guest_lines() -> Vec<String> {
    let messages = GUEST_MESSAGES.lock().unwrap();
    messages
        .iter()
        .map(|(plugin, message)| format!("{plugin}: {}", String::from_utf8_lossy(message)))
        .collect()
}

/// Return the raw bytes of every message `plugin` has logged so far.
pub fn guest_messages(plugin: &str) -> Vec<Vec<u8>> {
    let messages = GUEST_MESSAGES.lock().unwrap();
    messages
        .iter()
        .filter(|(name, _)| name == plugin)
        .map(|(_, message)| message.clone())
        .collect()
}

static RUNTIMES: OnceCell<HashMap<u16, WasmRuntime>> = OnceCell::new();

/// Return the runtime behind the service on `port`.
pub fn runtime(port: u16) -> &'static WasmRuntime {
    &RUNTIMES.get().expect("test server should be running")[&port]
}

pub struct TestServer;

impl TestServer {
    fn start() -> Self {
        let services = services();
        RUNTIMES
            .set(
                services
                    .iter()
                    .map(|(port, rt, _, _)| (*port, rt.clone()))
                    .collect(),
            )
            .ok()
            .unwrap();
        thread::spawn(move || {
            let mut server = Server::new(None).unwrap();
            server.bootstrap();
            for (port, runtime, chain, threads) in services {
                let chain = runtime.chain(&chain).unwrap();
                let mut service =
                    pingora_proxy::http_proxy_service(&server.configuration, TestProxy { chain });
                let mut options = HttpServerOptions::default();
                options.h2c = true;
                service.app_logic_mut().unwrap().server_options = Some(options);
                service.threads = threads;
                service.add_tcp(&format!("127.0.0.1:{port}"));
                server.add_service(service);
            }
            server.run_forever();
        });
        let deadline = Instant::now() + Duration::from_secs(10);
        for port in FIRST_PORT..=LAST_PORT {
            let addr = format!("127.0.0.1:{port}").parse().unwrap();
            while std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(100)).is_err() {
                assert!(Instant::now() < deadline, "test proxy failed to start");
                thread::sleep(Duration::from_millis(50));
            }
        }
        TestServer
    }
}

pub static TEST_SERVER: Lazy<TestServer> = Lazy::new(TestServer::start);

pub async fn init() {
    tokio::task::spawn_blocking(|| {
        Lazy::force(&TEST_SERVER);
    })
    .await
    .unwrap();
}

/// Start a peer that reads one request with a chunked body and closes the connection without
/// responding.
pub async fn closing_peer() -> (u16, tokio::task::JoinHandle<()>) {
    use tokio::io::AsyncReadExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut part = [0u8; 4096];
        while !request.ends_with(b"0\r\n\r\n") {
            match stream.read(&mut part).await {
                Ok(n) if n > 0 => request.extend_from_slice(&part[..n]),
                _ => break,
            }
        }
    });
    (port, peer)
}

pub async fn echo_origin() -> (HttpOrigin, Arc<AtomicUsize>) {
    let count = Arc::new(AtomicUsize::new(0));
    let seen = count.clone();
    let origin = HttpOrigin::bind(move |request: Request<Bytes>| {
        seen.fetch_add(1, Ordering::SeqCst);
        let mut response = Response::builder().status(200);
        for (name, value) in request.headers() {
            response = response.header(format!("x-echo-{name}"), value);
        }
        let body = if request.headers().contains_key("x-return-body") {
            request.body().clone()
        } else {
            Bytes::from_static(b"origin")
        };
        let response = response
            .header("x-echo-body-len", request.body().len())
            .body(body)
            .unwrap();
        async move { response }
    })
    .await
    .unwrap();
    (origin, count)
}

pub fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap()
}

pub fn url(port: u16, path: &str) -> String {
    format!("http://127.0.0.1:{port}{path}")
}

/// Poll `check` every 10 ms until it returns `true`, giving up after five seconds.
pub async fn eventually(check: impl Fn() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if check() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    check()
}
