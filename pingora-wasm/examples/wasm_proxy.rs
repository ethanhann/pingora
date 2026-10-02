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
use pingora_core::protocols::Digest;
use pingora_core::server::configuration::Opt;
use pingora_core::server::Server;
use pingora_core::upstreams::peer::HttpPeer;
use pingora_core::{Error, Result};
use pingora_http::{RequestHeader, ResponseHeader};
use pingora_proxy::{ProxyHttp, Session};
use pingora_wasm::{
    write_plugin_response, PrometheusMetricSink, RequestOutcome, StaticCalloutUpstreams, WasmChain,
    WasmConf, WasmCtx, WasmPluginConf, WasmRuntime, WasmServices,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_PLUGIN: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/add-request-header.wasm"
);

const BODY_FLAG: &str = "--body";
const CALLOUT_UPSTREAM_FLAG: &str = "--callout-upstream";
const CONF_FLAG: &str = "--conf";
const DEFAULT_CHAIN: &str = "default";

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

/// Build the runtime and a chain of the plugins listed on the command line.
fn runtime_and_chain_from_args(args: &[String], slots: usize) -> (WasmRuntime, WasmChain) {
    // Body phases cost a plugin call per chunk, so only enable them for plugins that need the body
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
            let upstream = args
                .next()
                .expect("callout upstream flag needs a value of the form name=address");
            let (name, address) = upstream
                .split_once('=')
                .expect("callout upstream must be given as name=address");
            upstreams.insert(name, HttpPeer::new(address, false, String::new()));
            continue;
        }
        let path = PathBuf::from(arg);
        let name = path.file_stem().unwrap().to_string_lossy();
        let mut plugin = WasmPluginConf::new(name, &path);
        plugin.slots = slots;
        plugin.request_body = body;
        plugin.response_body = body;
        plugin.response_trailers = body;
        plugins.push(plugin);
    }
    let names: Vec<String> = plugins.iter().map(|p| p.name.clone()).collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    let mut services = WasmServices::default();
    services.callout_upstreams = Arc::new(upstreams);
    services.metric_sink = prometheus_sink();
    let runtime = WasmRuntime::new_with_services(plugins, services).unwrap();
    let chain = runtime.chain(&names).unwrap();
    (runtime, chain)
}

/// Build the runtime and the `default` chain from a YAML file.
fn runtime_and_chain_from_conf(path: &str) -> (WasmRuntime, WasmChain) {
    let yaml = std::fs::read_to_string(path)
        .unwrap_or_else(|e| exit_with(format!("cannot read {path}: {e}")));
    let conf: WasmConf =
        serde_yaml::from_str(&yaml).unwrap_or_else(|e| exit_with(format!("invalid {path}: {e}")));
    let mut services = conf.services();
    services.metric_sink = prometheus_sink();
    let runtime = WasmRuntime::new_with_services(conf.plugins.clone(), services)
        .unwrap_or_else(|e| exit_with(format!("invalid {path}: {e}")));
    let chain = conf
        .chain_plugins(DEFAULT_CHAIN)
        .and_then(|names| runtime.chain(&names))
        .unwrap_or_else(|e| exit_with(format!("invalid {path}: {e}")));
    (runtime, chain)
}

fn exit_with(message: String) -> ! {
    eprintln!("{message}");
    std::process::exit(1)
}

fn prometheus_sink() -> Arc<PrometheusMetricSink> {
    let registry = pingora_prometheus::prometheus::default_registry().clone();
    Arc::new(PrometheusMetricSink::new(registry).unwrap())
}

// RUST_LOG=INFO cargo run --example wasm_proxy -- tests/fixtures/add-request-header.wasm
// curl 127.0.0.1:6190/headers
//
// Plugins you list after --body also run on request bodies, response bodies, and response trailers
// RUST_LOG=INFO cargo run --example wasm_proxy -- --body tests/fixtures/sdk-http-body.wasm
// curl -d 'a secret' 127.0.0.1:6190/anything
//
// Use --callout-upstream name=address to tell the proxy where callouts to an upstream name go
// RUST_LOG=INFO cargo run --example wasm_proxy -- --callout-upstream httpbin=127.0.0.1:8080 \
//     tests/fixtures/sdk-http-auth-random.wasm
// curl -i 127.0.0.1:6190/headers
//
// With --conf and a YAML file as the only arguments, the plugins and the chain are read from
// that file, and Pingora reads its own settings from it as well
// RUST_LOG=INFO cargo run --example wasm_proxy -- --conf examples/wasm_proxy.yaml
// curl -d 'a secret' 127.0.0.1:6190/anything
//
// A failing plugin is also logged as a warning by the proxy_wasm_host crate. You can silence
// that with RUST_LOG=info,proxy_wasm_host=error
//
// Plugin metrics are served at 127.0.0.1:6192/metrics
fn main() {
    env_logger::init();

    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let conf_path = match &args[..] {
        [flag, path] if flag == CONF_FLAG => Some(path.clone()),
        [flag] if flag == CONF_FLAG => exit_with("--conf needs a file".to_string()),
        _ if args.iter().any(|arg| arg == CONF_FLAG) => {
            exit_with("--conf cannot be combined with other arguments".to_string())
        }
        _ => None,
    };
    if let Some(path) = conf_path.as_ref().filter(|path| !Path::new(path).is_file()) {
        exit_with(format!("cannot read {path}: no such file"));
    }
    let opt = Opt {
        conf: conf_path.clone(),
        ..Opt::default()
    };
    let mut my_server = Server::new(Some(opt)).unwrap();
    my_server.bootstrap();

    let slots = my_server.configuration.threads;
    let (_runtime, chain) = match conf_path {
        Some(path) => runtime_and_chain_from_conf(&path),
        None => {
            if args.is_empty() {
                args.push(DEFAULT_PLUGIN.into());
            }
            runtime_and_chain_from_args(&args, slots)
        }
    };

    let mut my_proxy =
        pingora_proxy::http_proxy_service(&my_server.configuration, PluginProxy { chain });
    my_proxy.add_tcp("127.0.0.1:6190");

    let mut metrics_service = pingora_prometheus::prometheus_http_service();
    metrics_service.add_tcp("127.0.0.1:6192");

    my_server.add_service(my_proxy);
    my_server.add_service(metrics_service);
    my_server.run_forever();
}
