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

use crate::{WasmCtx, WasmPluginConf, WasmRuntime};
use bytes::Bytes;
use pingora_proxy::Session;
use proxy_wasm_host::HeaderMap;
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

pub(crate) fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(format!("{name}.wasm"))
}

pub(crate) fn plugin(name: &str, path: PathBuf, slots: usize) -> WasmPluginConf {
    let mut conf = WasmPluginConf::new(name, path);
    conf.slots = slots;
    conf
}

/// The body of each callback of a small guest, in WAT.
///
/// The guest is the template `tests/fixtures/guest.wat` with these callbacks.
pub(crate) struct Wat {
    pub(crate) abi: bool,
    pub(crate) vm_start: &'static str,
    pub(crate) request_headers: &'static str,
    pub(crate) done: &'static str,
    /// The callbacks below are exported only when they have a body.
    pub(crate) request_body: Option<&'static str>,
    pub(crate) response_headers: Option<&'static str>,
    pub(crate) response_body: Option<&'static str>,
    pub(crate) response_trailers: Option<&'static str>,
}

pub(crate) const CONTINUE: &str = "i32.const 0";
pub(crate) const PAUSE: &str = "i32.const 1";
pub(crate) const TRAP: &str = "unreachable";
/// A body callback that returns `Pause` until the end of the stream.
pub(crate) const HOLD: &str = "(i32.eqz (local.get 2))";
/// A body callback that holds the first chunks and traps on the last one.
pub(crate) const HOLD_THEN_TRAP: &str =
    "(if (result i32) (local.get 2) (then unreachable) (else (i32.const 1)))";
/// Callbacks that write `a` or `b` in front of the request body or the response body.
pub(crate) const MARK_A_REQUEST: &str = "(call $mark_a (i32.const 0))";
pub(crate) const MARK_B_REQUEST: &str = "(call $mark_b (i32.const 0))";
pub(crate) const MARK_A_RESPONSE: &str = "(call $mark_a (i32.const 1))";
pub(crate) const MARK_B_RESPONSE: &str = "(call $mark_b (i32.const 1))";
/// A body callback that writes `a` in front of the request body on each call, and holds the
/// body until its end.
pub(crate) const MARK_AND_HOLD: &str =
    "(drop (call $mark_a (i32.const 0))) (i32.eqz (local.get 2))";
/// A body callback that holds the response body, and replaces all of it with `replaced` at
/// the end.
pub(crate) const HOLD_THEN_REPLACE: &str = "(if (result i32) (local.get 2)
    (then (call $replace_body (i32.const 1) (local.get 1))) (else (i32.const 1)))";
/// Callbacks that send a response with the body `teapot`, and pause.
pub(crate) const TEAPOT: &str = "(call $respond (i32.const 418))";
pub(crate) const FORBIDDEN: &str = "(call $respond (i32.const 403))";
pub(crate) const SET_TRAILER: &str = "(call $set_trailer)";
pub(crate) const REMOVE_LENGTH: &str = "(call $remove_length)";

impl Default for Wat {
    fn default() -> Self {
        Wat {
            abi: true,
            vm_start: "i32.const 1",
            request_headers: CONTINUE,
            done: "i32.const 1",
            request_body: None,
            response_headers: None,
            response_body: None,
            response_trailers: None,
        }
    }
}

impl Wat {
    pub(crate) fn request_body(body: &'static str) -> Self {
        Wat {
            request_body: Some(body),
            ..Wat::default()
        }
    }

    pub(crate) fn response_headers(body: &'static str) -> Self {
        Wat {
            response_headers: Some(body),
            ..Wat::default()
        }
    }

    pub(crate) fn response_body(body: &'static str) -> Self {
        Wat {
            response_body: Some(body),
            ..Wat::default()
        }
    }

    pub(crate) fn response_trailers(body: &'static str) -> Self {
        Wat {
            response_trailers: Some(body),
            ..Wat::default()
        }
    }
}

const TEMPLATE: &str = include_str!("../tests/fixtures/guest.wat");

fn export(name: &str, params: &str, body: Option<&str>) -> String {
    match body {
        Some(body) => format!(r#"(func (export "{name}") (param {params}) (result i32) {body})"#),
        None => String::new(),
    }
}

pub(crate) fn wat_guest(label: &str, guest: Wat) -> PathBuf {
    let abi = match guest.abi {
        true => r#"(func (export "proxy_abi_version_0_2_1"))"#.to_string(),
        false => String::new(),
    };
    let callbacks = [
        abi,
        export("proxy_on_vm_start", "i32 i32", Some(guest.vm_start)),
        export(
            "proxy_on_request_headers",
            "i32 i32 i32",
            Some(guest.request_headers),
        ),
        export("proxy_on_request_body", "i32 i32 i32", guest.request_body),
        export(
            "proxy_on_response_headers",
            "i32 i32 i32",
            guest.response_headers,
        ),
        export("proxy_on_response_body", "i32 i32 i32", guest.response_body),
        export(
            "proxy_on_response_trailers",
            "i32 i32",
            guest.response_trailers,
        ),
        export("proxy_on_done", "i32", Some(guest.done)),
    ]
    .join("\n");
    let wat = TEMPLATE.replace("\nCALLBACKS\n", &format!("\n{callbacks}\n"));
    static GUESTS: AtomicUsize = AtomicUsize::new(0);
    let path = std::env::temp_dir().join(format!(
        "pingora-wasm-{label}-{}-{}.wasm",
        std::process::id(),
        GUESTS.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::write(&path, wat::parse_str(wat).unwrap()).unwrap();
    path
}

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

/// Build the configuration of a WAT plugin that runs every body phase.
pub(crate) fn body_plugin(name: &str, wat: Wat) -> WasmPluginConf {
    let mut conf = plugin(name, wat_guest(name, wat), 1);
    conf.request_body = true;
    conf.response_body = true;
    conf.response_trailers = true;
    conf
}

pub(crate) fn body_chunk(bytes: &'static str) -> Option<Bytes> {
    Some(Bytes::from_static(bytes.as_bytes()))
}

/// Build a runtime with `plugins`, and a request context from a chain of them in that order.
/// The context has run the request headers of `request` and started its first upstream attempt.
pub(crate) async fn start_request(
    plugins: Vec<WasmPluginConf>,
    request: &[u8],
) -> (WasmRuntime, WasmCtx, Session, DuplexStream) {
    let names: Vec<String> = plugins.iter().map(|p| p.name.clone()).collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    let runtime = WasmRuntime::new(plugins).unwrap();
    let mut ctx = runtime.chain(&names).unwrap().new_ctx();
    let (mut session, client) = session(request).await;
    ctx.request_filter(&mut session).await.unwrap();
    ctx.upstream_attempt();
    (runtime, ctx, session, client)
}

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

/// Build a runtime with one plugin named `a`, and a request context from a chain of it.
pub(crate) fn one_plugin(conf: WasmPluginConf) -> (WasmRuntime, WasmCtx) {
    let runtime = WasmRuntime::new(vec![conf]).unwrap();
    let ctx = runtime.chain(&["a"]).unwrap().new_ctx();
    (runtime, ctx)
}

pub(crate) fn add_request_header() -> WasmPluginConf {
    plugin("a", fixture("add-request-header"), 1)
}

/// Build the configuration of a WAT plugin named `a` whose `proxy_on_request_headers` runs
/// `request_headers`.
pub(crate) fn wat_plugin(label: &str, request_headers: &'static str) -> WasmPluginConf {
    let wat = Wat {
        request_headers,
        ..Wat::default()
    };
    plugin("a", wat_guest(label, wat), 1)
}

/// Return every pair of a header map, as text.
pub(crate) fn pairs(map: &dyn HeaderMap) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let _ = map.for_each_pair(&mut |k, v| {
        out.push((
            String::from_utf8_lossy(k).into_owned(),
            String::from_utf8_lossy(v).into_owned(),
        ));
        ControlFlow::Continue(())
    });
    out
}

pub(crate) fn get(map: &dyn HeaderMap, key: &str) -> Option<String> {
    map.get(key.as_bytes())
        .map(|v| String::from_utf8_lossy(&v).into_owned())
}
