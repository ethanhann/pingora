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

//! Plugin lifecycle tests
//!
//! Each test runs its own server with `Server::run`, so that it can shut the server down.

use crate::utils::callout_origins::CalloutOriginPerPlugin;
use crate::utils::proxy::TestProxy;
use crate::utils::GuestMessageSink;
use crate::utils::{client, echo_origin, eventually, fixture, guest_lines, guests, url};
use async_trait::async_trait;
use pingora_core::connectors::http::Connector;
use pingora_core::server::configuration::ServerConf;
use pingora_core::server::{RunArgs, Server, ShutdownSignal, ShutdownSignalWatch};
use pingora_core::services::background::background_service;
use pingora_wasm::prometheus::Registry;
use pingora_wasm::{PrometheusMetricSink, WasmPluginConf, WasmPlugins, WasmRuntime, WasmServices};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::sync::Notify;

struct TestShutdownSignal(Arc<Notify>);

#[async_trait]
impl ShutdownSignalWatch for TestShutdownSignal {
    async fn recv(&self) -> ShutdownSignal {
        self.0.notified().await;
        ShutdownSignal::GracefulTerminate
    }
}

/// A server with one proxy service on its port, whose chain `default` comes from `plugins`.
struct LifecycleServer {
    plugins: Arc<WasmPlugins>,
    shutdown: Arc<Notify>,
    thread: JoinHandle<()>,
}

impl LifecycleServer {
    async fn start(port: u16, runtime: WasmRuntime, plugin: &str) -> Self {
        let plugins = WasmPlugins::new(runtime, [("default", [plugin])]).unwrap();
        let plugins_service = background_service("wasm plugins", plugins);
        let plugins = plugins_service.task();
        let chain = plugins.chain("default").unwrap();
        let shutdown = Arc::new(Notify::new());
        let shutdown_signal = Box::new(TestShutdownSignal(shutdown.clone()));
        let conf = ServerConf {
            grace_period_seconds: Some(1),
            graceful_shutdown_timeout_seconds: Some(5),
            ..Default::default()
        };
        let thread = std::thread::spawn(move || {
            let mut server = Server::new_with_opt_and_conf(None, conf);
            server.bootstrap();
            let proxy = TestProxy { chain };
            let mut proxy = pingora_proxy::http_proxy_service(&server.configuration, proxy);
            proxy.add_tcp(&format!("127.0.0.1:{port}"));
            server.add_service(proxy);
            server.add_service(plugins_service);
            server.run(RunArgs { shutdown_signal });
        });
        let listening = || std::net::TcpStream::connect(("127.0.0.1", port)).is_ok();
        assert!(
            eventually(listening).await,
            "server on {port} did not start"
        );
        LifecycleServer {
            plugins,
            shutdown,
            thread,
        }
    }

    /// Shut the server down gracefully and wait for `Server::run` to return.
    async fn shut_down(self) {
        self.shutdown.notify_one();
        let thread = self.thread;
        tokio::task::spawn_blocking(move || thread.join().unwrap())
            .await
            .unwrap();
    }
}

fn logging_runtime(plugin: WasmPluginConf) -> WasmRuntime {
    let mut services = WasmServices::default();
    services.log_sink = Arc::new(GuestMessageSink);
    WasmRuntime::new_with_services(vec![plugin], services).unwrap()
}

fn count_lines(line: &str) -> usize {
    guest_lines().iter().filter(|l| *l == line).count()
}

#[tokio::test]
async fn plugins_tick_before_requests_and_end_at_reload_and_shutdown() {
    let old = guests::root_lifecycle_logger("reload-old", "tick", "root done");
    let server = LifecycleServer::start(6421, logging_runtime(old), "reload-old").await;
    let old_ticked = eventually(|| count_lines("reload-old: tick") > 0).await;
    let new = logging_runtime(guests::root_lifecycle_logger(
        "reload-new",
        "tick",
        "root done",
    ));
    server
        .plugins
        .replace(new, [("default", ["reload-new"])])
        .unwrap();
    let old_ended_at_reload = eventually(|| count_lines("reload-old: root done") == 1).await;
    let new_ticked = eventually(|| count_lines("reload-new: tick") > 0).await;

    server.shut_down().await;

    assert!(old_ticked);
    assert!(old_ended_at_reload);
    assert!(new_ticked);
    assert_eq!(count_lines("reload-new: root done"), 1);
    assert_eq!(count_lines("reload-old: root done"), 1);
}

/// Send requests one after another until `stop` is set. Return each request's status with the
/// reload generations it started and ended in, with status 0 for a request that failed.
async fn send_until_stopped(
    port: u16,
    origin: u16,
    generation: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
) -> Vec<(usize, usize, u16)> {
    let client = client();
    let mut results = Vec::new();
    while !stop.load(Ordering::SeqCst) {
        let at = generation.load(Ordering::SeqCst);
        let request = client.get(url(port, "/")).header("x-test-origin", origin);
        let status = match request.send().await {
            Ok(response) => response.status().as_u16(),
            Err(_) => 0,
        };
        results.push((at, generation.load(Ordering::SeqCst), status));
    }
    results
}

#[tokio::test]
async fn replace_under_callout_traffic_fails_no_request() {
    let callout_origins = CalloutOriginPerPlugin::start(&[("swap-auth", Some("0"))]);
    let connector = Arc::new(Connector::new(None));
    let metric_sink = Arc::new(PrometheusMetricSink::new(Registry::new()).unwrap());
    let build = || {
        let mut services = WasmServices::default();
        services.callout_upstreams = callout_origins.clone();
        services.callout_connector = Some(connector.clone());
        services.metric_sink = metric_sink.clone();
        let mut auth = WasmPluginConf::new("swap-auth", fixture("sdk-http-auth-random"));
        auth.slots = Some(2);
        WasmRuntime::new_with_services(vec![auth], services).unwrap()
    };
    let server = LifecycleServer::start(6422, build(), "swap-auth").await;
    let (origin, _) = echo_origin().await;
    let generation = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let origin_port = origin.addr().port();
    let clients: Vec<_> = (0..8)
        .map(|_| {
            let sending = send_until_stopped(6422, origin_port, generation.clone(), stop.clone());
            tokio::spawn(sending)
        })
        .collect();
    let reload = || {
        server
            .plugins
            .replace(build(), [("default", ["swap-auth"])])
            .unwrap();
        generation.fetch_add(1, Ordering::SeqCst);
    };

    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        reload();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    stop.store(true, Ordering::SeqCst);
    let mut results = Vec::new();
    for sending in clients {
        results.extend(sending.await.unwrap());
    }
    server.shut_down().await;
    let failed: Vec<_> = results
        .iter()
        .filter(|(_, _, status)| *status != 200)
        .collect();
    assert!(failed.is_empty(), "{failed:?}");
    for at in 0..=3 {
        let count = results
            .iter()
            .filter(|(started, _, _)| *started == at)
            .count();
        assert!(count > 0, "no request started in generation {at}");
    }
    let across_a_swap = results.iter().filter(|(started, ended, _)| started < ended);
    assert!(across_a_swap.count() > 0);
    assert!(!callout_origins.origin("swap-auth").requests().is_empty());
}
