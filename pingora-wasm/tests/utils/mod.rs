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

use async_trait::async_trait;
use bytes::Bytes;
use http::{Request, Response};
use once_cell::sync::{Lazy, OnceCell};
use pingora_core::server::Server;
use pingora_core::upstreams::peer::HttpPeer;
use pingora_core::{Error, ErrorType, Result};
use pingora_http::ResponseHeader;
use pingora_proxy::{ProxyHttp, Session};
use pingora_test_utils::http_origin::HttpOrigin;
use pingora_wasm::{
    write_plugin_response, RequestOutcome, WasmChain, WasmCtx, WasmPluginConf, WasmRuntime,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

pub const FIRST_PORT: u16 = 6380;
pub const LAST_PORT: u16 = 6390;

pub fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(format!("{name}.wasm"))
}

fn plugin(name: &str, path: PathBuf, slots: usize, configuration: &str) -> WasmPluginConf {
    let mut conf = WasmPluginConf::new(name, path);
    conf.slots = slots;
    conf.configuration = configuration.as_bytes().to_vec();
    conf
}

static GUEST_LINES: Lazy<Mutex<Vec<String>>> = Lazy::new(|| Mutex::new(Vec::new()));

struct Capture;

impl log::Log for Capture {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.target() == "pingora_wasm::guest"
    }

    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            GUEST_LINES.lock().unwrap().push(record.args().to_string());
        }
    }

    fn flush(&self) {}
}

/// Every guest log line so far.
pub fn guest_lines() -> Vec<String> {
    GUEST_LINES.lock().unwrap().clone()
}

pub struct TestProxy {
    chain: WasmChain,
}

#[async_trait]
impl ProxyHttp for TestProxy {
    type CTX = Option<WasmCtx>;

    fn new_ctx(&self) -> Self::CTX {
        None
    }

    async fn request_filter(&self, session: &mut Session, ctx: &mut Self::CTX) -> Result<bool> {
        let wasm = ctx.insert(self.chain.new_ctx());
        match wasm.request_filter(session).await? {
            RequestOutcome::Respond(header, body) => {
                write_plugin_response(session, header, body).await?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn upstream_peer(
        &self,
        session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        let port: u16 = session
            .req_header()
            .headers
            .get("x-test-origin")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .ok_or_else(|| Error::explain(ErrorType::HTTPStatus(400), "no x-test-origin"))?;
        let peer = HttpPeer::new(("127.0.0.1", port), false, String::new());
        Ok(Box::new(peer))
    }

    async fn response_filter(
        &self,
        session: &mut Session,
        upstream_response: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        match ctx {
            Some(wasm) => wasm.response_filter(session, upstream_response).await,
            None => Ok(()),
        }
    }

    async fn logging(&self, session: &mut Session, _e: Option<&Error>, ctx: &mut Self::CTX) {
        if let Some(wasm) = ctx {
            wasm.logging(session).await;
        }
    }
}

static RUNTIMES: OnceCell<HashMap<u16, WasmRuntime>> = OnceCell::new();

/// The runtime that serves `port`.
pub fn runtime(port: u16) -> &'static WasmRuntime {
    &RUNTIMES.get().expect("the test server is started")[&port]
}

fn services() -> Vec<(u16, WasmRuntime, Vec<&'static str>, Option<usize>)> {
    let single = |name: &'static str, conf: WasmPluginConf| {
        (WasmRuntime::new(vec![conf]).unwrap(), vec![name])
    };
    let add = || plugin("add", fixture("add-request-header"), 2, "");
    let example = |slots| plugin("example", fixture("http-example"), slots, "");
    let config = |name, value| plugin(name, fixture("sdk-http-config"), 2, value);
    let shared = WasmRuntime::new(vec![
        plugin("add", fixture("add-request-header"), 1, ""),
        plugin("config", fixture("sdk-http-config"), 1, "hello"),
    ])
    .unwrap();

    let mut services = Vec::new();
    let mut push = |port, (runtime, chain): (WasmRuntime, Vec<&'static str>), threads| {
        services.push((port, runtime, chain, threads));
    };
    push(6380, single("add", add()), None);
    push(
        6381,
        (
            WasmRuntime::new(vec![add(), example(2)]).unwrap(),
            vec!["add", "example"],
        ),
        None,
    );
    push(6382, single("hello", config("hello", "hello")), None);
    push(
        6383,
        single(
            "headers",
            plugin("headers", fixture("sdk-http-headers"), 2, ""),
        ),
        None,
    );
    push(6384, single("example", example(1)), None);
    push(6385, single("example", example(4)), Some(4));
    push(
        6386,
        (
            WasmRuntime::new(vec![config("a", "a"), config("b", "b")]).unwrap(),
            vec!["a", "b"],
        ),
        None,
    );
    push(
        6387,
        (
            WasmRuntime::new(vec![config("hello", "hello"), example(2)]).unwrap(),
            vec!["hello", "example"],
        ),
        None,
    );
    push(6388, single("example", example(2)), None);
    push(6389, (shared.clone(), vec!["add"]), None);
    push(6390, (shared, vec!["add", "config"]), None);
    services
}

pub struct TestServer;

impl TestServer {
    fn start() -> Self {
        log::set_boxed_logger(Box::new(Capture)).unwrap();
        log::set_max_level(log::LevelFilter::Info);
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

/// An origin that echoes each request header as `x-echo-<name>` and the body length as
/// `x-echo-body-len`, and counts its requests.
pub async fn echo_origin() -> (HttpOrigin, Arc<AtomicUsize>) {
    let count = Arc::new(AtomicUsize::new(0));
    let seen = count.clone();
    let origin = HttpOrigin::bind(move |request: Request<Bytes>| {
        seen.fetch_add(1, Ordering::SeqCst);
        let mut response = Response::builder().status(200);
        for (name, value) in request.headers() {
            response = response.header(format!("x-echo-{name}"), value);
        }
        let response = response
            .header("x-echo-body-len", request.body().len())
            .body(Bytes::from_static(b"origin"))
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

/// Waits until `check` answers true, for up to five seconds.
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
