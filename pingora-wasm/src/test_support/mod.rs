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

pub(crate) mod callouts;
mod session;

pub(crate) use session::{
    read_downstream, read_downstream_after_marker, session, GET, HEAD, MARKER_RESPONSE, POST,
    UPGRADE,
};

use crate::{LogContext, LogLevel, LogSink, WasmCtx, WasmPluginConf, WasmRuntime};
use bytes::Bytes;
use parking_lot::Mutex;
use pingora_proxy::Session;
use proxy_wasm_host::HeaderMap;
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::DuplexStream;

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
#[derive(Clone, Copy)]
pub(crate) struct Wat {
    pub(crate) abi: bool,
    pub(crate) vm_start: &'static str,
    pub(crate) configure: &'static str,
    pub(crate) request_headers: &'static str,
    pub(crate) done: &'static str,
    /// The callbacks below are exported only when they have a body.
    pub(crate) request_body: Option<&'static str>,
    pub(crate) response_headers: Option<&'static str>,
    pub(crate) response_body: Option<&'static str>,
    pub(crate) response_trailers: Option<&'static str>,
    pub(crate) http_call_response: Option<&'static str>,
    pub(crate) log: Option<&'static str>,
    pub(crate) tick: Option<&'static str>,
    pub(crate) queue_ready: Option<&'static str>,
    pub(crate) delete: &'static str,
    /// Text for the callbacks, such as `(data (i32.const 700) "text")`.
    pub(crate) data_segments: &'static str,
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
            configure: "i32.const 1",
            request_headers: CONTINUE,
            done: "i32.const 1",
            request_body: None,
            response_headers: None,
            response_body: None,
            response_trailers: None,
            http_call_response: None,
            log: None,
            tick: None,
            queue_ready: None,
            delete: "",
            data_segments: "",
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

const TEMPLATE: &str = include_str!("../../tests/fixtures/guest.wat");

/// Return a callback that returns nothing, or the empty callback that the template needs.
fn export_with_no_result(name: &str, params: &str, body: Option<&str>) -> String {
    let body = body.unwrap_or_default();
    format!(r#"(func (export "{name}") (param {params}) {body})"#)
}

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
        guest.data_segments.to_string(),
        export("proxy_on_vm_start", "i32 i32", Some(guest.vm_start)),
        export("proxy_on_configure", "i32 i32", Some(guest.configure)),
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
        export_with_no_result(
            "proxy_on_http_call_response",
            "i32 i32 i32 i32 i32",
            guest.http_call_response,
        ),
        export_with_no_result("proxy_on_log", "i32", guest.log),
        match guest.tick {
            Some(body) => export_with_no_result("proxy_on_tick", "i32", Some(body)),
            None => String::new(),
        },
        match guest.queue_ready {
            Some(body) => export_with_no_result("proxy_on_queue_ready", "i32 i32", Some(body)),
            None => String::new(),
        },
        export_with_no_result("proxy_on_delete", "i32", Some(guest.delete)),
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

/// The lines that guests logged.
#[derive(Default)]
pub(crate) struct RecordedGuestLogs(pub(crate) Mutex<Vec<String>>);

impl LogSink for RecordedGuestLogs {
    fn log(&self, _context: LogContext<'_>, _level: LogLevel, message: &[u8]) {
        self.0
            .lock()
            .push(String::from_utf8_lossy(message).into_owned());
    }
}

/// Wait until `check` returns true, for up to five seconds.
pub(crate) async fn eventually(check: impl Fn() -> bool) -> bool {
    for _ in 0..500 {
        if check() {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    check()
}

static CRATE_LOG_LINES: Mutex<Vec<String>> = Mutex::new(Vec::new());

struct CrateLogs;

impl log::Log for CrateLogs {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.target().starts_with("pingora_wasm")
    }

    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            CRATE_LOG_LINES.lock().push(record.args().to_string());
        }
    }

    fn flush(&self) {}
}

/// Start to record the log lines of the crate. Call it before the code that logs.
pub(crate) fn record_crate_logs() {
    if log::set_logger(&CrateLogs).is_ok() {
        log::set_max_level(log::LevelFilter::Debug);
    }
}

/// Return the recorded log lines of the crate that contain `text`.
pub(crate) fn crate_log_lines_with(text: &str) -> Vec<String> {
    let lines = CRATE_LOG_LINES.lock();
    lines
        .iter()
        .filter(|line| line.contains(text))
        .cloned()
        .collect()
}
