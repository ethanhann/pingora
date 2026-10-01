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

//! A proxy whose plugin works outside of each request: it logs on a timer, calls another
//! service from that timer, reads a shared queue, counts requests in a Prometheus metric, and
//! reads properties that the proxy and the session provide.

use async_trait::async_trait;
use pingora_core::protocols::Digest;
use pingora_core::server::Server;
use pingora_core::upstreams::peer::HttpPeer;
use pingora_core::{Error, Result};
use pingora_http::{RequestHeader, ResponseHeader};
use pingora_proxy::{ProxyHttp, Session};
use pingora_wasm::{
    write_plugin_response, PrometheusMetricSink, RequestOutcome, StaticCalloutUpstreams, WasmChain,
    WasmCtx, WasmPluginConf, WasmRuntime, WasmServices,
};
use std::sync::Arc;

const PLUGIN_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/plugin-services.wasm"
);

pub struct PluginProxy {
    chain: WasmChain,
}

/// Return the route of a request, which is the first segment of its path.
fn route_of(path: &str) -> &str {
    let route = path.trim_start_matches('/').split(['/', '?']).next();
    route.filter(|route| !route.is_empty()).unwrap_or("root")
}

#[async_trait]
impl ProxyHttp for PluginProxy {
    type CTX = WasmCtx;

    fn new_ctx(&self) -> Self::CTX {
        self.chain.new_ctx()
    }

    async fn request_filter(&self, session: &mut Session, ctx: &mut Self::CTX) -> Result<bool> {
        // A real proxy sets the route it chose. The plugin reads it as `xds.route_name`
        let route = route_of(session.req_header().uri.path()).to_string();
        ctx.set_property(&["xds", "route_name"], route);
        match ctx.request_filter(session).await? {
            RequestOutcome::Respond(header, body) => {
                write_plugin_response(session, header, body).await?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        ctx.upstream_attempt();
        let peer = HttpPeer::new(("httpbin.org", 80), false, "httpbin.org".into());
        Ok(Box::new(peer))
    }

    async fn connected_to_upstream(
        &self,
        _session: &mut Session,
        _reused: bool,
        peer: &HttpPeer,
        #[cfg(unix)] _fd: std::os::unix::io::RawFd,
        #[cfg(windows)] _sock: std::os::windows::io::RawSocket,
        _digest: Option<&Digest>,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        ctx.upstream_connected(peer);
        Ok(())
    }

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        upstream_request.insert_header("Host", "httpbin.org")
    }

    async fn response_filter(
        &self,
        session: &mut Session,
        upstream_response: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        ctx.response_filter(session, upstream_response).await
    }

    fn suppress_error_log(&self, _session: &Session, ctx: &Self::CTX, _e: &Error) -> bool {
        ctx.plugin_responded()
    }

    async fn logging(&self, session: &mut Session, _e: Option<&Error>, ctx: &mut Self::CTX) {
        ctx.logging(session).await
    }
}

// RUST_LOG=info cargo run -p pingora-wasm --example wasm_plugin_services
//
// The ticks start with the first request. The plugin then logs a tick each second, and it calls
// httpbin on the first tick and on every tenth tick after it.
//
// curl -i 127.0.0.1:6190/anything/hello
// The response has the headers x-route: anything, x-client, and x-node: example-node, and the
// log has the line "path seen: /anything/hello" from the shared queue.
//
// curl 127.0.0.1:6192/metrics
// The output has plugin_requests_total with the label vm_id="plugin-services".
fn main() {
    env_logger::init();

    let mut my_server = Server::new(None).unwrap();
    my_server.bootstrap();

    let mut upstreams = StaticCalloutUpstreams::new();
    upstreams.insert(
        "httpbin",
        HttpPeer::new(("httpbin.org", 80), false, String::new()),
    );
    let registry = pingora_prometheus::prometheus::default_registry().clone();
    let mut services = WasmServices::default();
    services.callout_upstreams = Arc::new(upstreams);
    services.metric_sink = Arc::new(PrometheusMetricSink::new(registry).unwrap());
    services
        .fixed_properties
        .insert(&["node", "name"], "example-node");
    // One slot gives one tick each second. With a slot for each thread, each slot ticks
    let plugin = WasmPluginConf::new("plugin-services", PLUGIN_PATH);
    let runtime = WasmRuntime::new_with_services(vec![plugin], services).unwrap();
    let chain = runtime.chain(&["plugin-services"]).unwrap();

    let mut my_proxy =
        pingora_proxy::http_proxy_service(&my_server.configuration, PluginProxy { chain });
    my_proxy.add_tcp("127.0.0.1:6190");
    let mut metrics_service = pingora_prometheus::prometheus_http_service();
    metrics_service.add_tcp("127.0.0.1:6192");

    my_server.add_service(my_proxy);
    my_server.add_service(metrics_service);
    my_server.run_forever();
}
