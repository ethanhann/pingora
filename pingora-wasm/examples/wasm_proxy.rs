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
use pingora_core::server::Server;
use pingora_core::upstreams::peer::HttpPeer;
use pingora_core::{Error, Result};
use pingora_http::{RequestHeader, ResponseHeader};
use pingora_proxy::{ProxyHttp, Session};
use pingora_wasm::{
    write_plugin_response, RequestOutcome, StaticCalloutUpstreams, WasmChain, WasmCtx,
    WasmPluginConf, WasmRuntime, WasmServices,
};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_PLUGIN: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/add-request-header.wasm"
);

const BODY_FLAG: &str = "--body";
const CALLOUT_UPSTREAM_FLAG: &str = "--callout-upstream";

pub struct PluginProxy {
    chain: WasmChain,
}

#[async_trait]
impl ProxyHttp for PluginProxy {
    type CTX = WasmCtx;

    fn new_ctx(&self) -> Self::CTX {
        self.chain.new_ctx()
    }

    async fn request_filter(&self, session: &mut Session, ctx: &mut Self::CTX) -> Result<bool> {
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

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        upstream_request.insert_header("Host", "httpbin.org")
    }

    async fn request_body_filter(
        &self,
        session: &mut Session,
        body: &mut Option<Bytes>,
        end_of_stream: bool,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        ctx.request_body_filter(session, body, end_of_stream).await
    }

    async fn response_filter(
        &self,
        session: &mut Session,
        upstream_response: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        ctx.response_filter(session, upstream_response).await
    }

    async fn response_body_filter(
        &self,
        session: &mut Session,
        body: &mut Option<Bytes>,
        end_of_stream: bool,
        ctx: &mut Self::CTX,
    ) -> Result<Option<Duration>> {
        ctx.response_body_filter(session, body, end_of_stream)
            .await?;
        Ok(None)
    }

    async fn response_trailer_filter(
        &self,
        session: &mut Session,
        upstream_trailers: &mut http::HeaderMap,
        ctx: &mut Self::CTX,
    ) -> Result<Option<Bytes>> {
        ctx.response_trailer_filter(session, upstream_trailers)
            .await
    }

    fn suppress_error_log(&self, _session: &Session, ctx: &Self::CTX, _e: &Error) -> bool {
        ctx.plugin_responded()
    }

    async fn logging(&self, session: &mut Session, _e: Option<&Error>, ctx: &mut Self::CTX) {
        ctx.logging(session).await
    }
}

// RUST_LOG=INFO cargo run --example wasm_proxy -- tests/fixtures/add-request-header.wasm
// curl 127.0.0.1:6190/headers
//
// Plugins after --body also run on bodies and trailers
// RUST_LOG=INFO cargo run --example wasm_proxy -- --body tests/fixtures/sdk-http-body.wasm
// curl -d 'a secret' 127.0.0.1:6190/anything
//
// --callout-upstream name=address sends the callouts for an upstream name to an address
// RUST_LOG=INFO cargo run --example wasm_proxy -- --callout-upstream httpbin=127.0.0.1:8080 \
//     tests/fixtures/sdk-http-auth-random.wasm
// curl -i 127.0.0.1:6190/headers
//
// When a plugin fails, the host crate logs a warning too. To hide it, use
// RUST_LOG=info,proxy_wasm_host=error
fn main() {
    env_logger::init();

    let mut my_server = Server::new(None).unwrap();
    my_server.bootstrap();

    let mut args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        args.push(DEFAULT_PLUGIN.into());
    }
    // A body phase runs a plugin on every chunk, so turn it on only for a plugin that reads bodies
    let mut body = false;
    let mut plugins = Vec::new();
    let mut upstreams = StaticCalloutUpstreams::new();
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        if arg == BODY_FLAG {
            body = true;
            continue;
        }
        if arg == CALLOUT_UPSTREAM_FLAG {
            let upstream = args.next().expect("a callout upstream as name=address");
            let (name, address) = upstream.split_once('=').expect("name=address");
            upstreams.insert(name, HttpPeer::new(address, false, String::new()));
            continue;
        }
        let path = PathBuf::from(arg);
        let name = path.file_stem().unwrap().to_string_lossy();
        let mut plugin = WasmPluginConf::new(name, &path);
        plugin.slots = my_server.configuration.threads;
        plugin.request_body = body;
        plugin.response_body = body;
        plugin.response_trailers = body;
        plugins.push(plugin);
    }
    let names: Vec<String> = plugins.iter().map(|p| p.name.clone()).collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    let mut services = WasmServices::default();
    services.callout_upstreams = Arc::new(upstreams);
    let runtime = WasmRuntime::new_with_services(plugins, services).unwrap();
    let chain = runtime.chain(&names).unwrap();

    let mut my_proxy =
        pingora_proxy::http_proxy_service(&my_server.configuration, PluginProxy { chain });
    my_proxy.add_tcp("127.0.0.1:6190");

    my_server.add_service(my_proxy);
    my_server.run_forever();
}
