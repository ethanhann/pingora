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

//! WAT guests
//!
//! Small guests built from `guest.wat` for behavior the compiled fixtures do not cover.

use pingora_wasm::WasmPluginConf;
use std::path::PathBuf;
use std::time::Duration;

const TEMPLATE: &str = include_str!("../fixtures/guest.wat");
const REQUEST_BODY: &str = "proxy_on_request_body";
const RESPONSE_HEADERS: &str = "proxy_on_response_headers";
/// Body callback that pauses until the last chunk of the request body, then prepends `a`.
const HOLD_THEN_MARK: &str = "(if (result i32) (local.get 2)
    (then (call $mark_a (i32.const 0))) (else (i32.const 1)))";
/// Body callback that pauses until the last chunk of the request body, then traps.
const HOLD_THEN_TRAP: &str =
    "(if (result i32) (local.get 2) (then unreachable) (else (i32.const 1)))";
/// Body for `proxy_on_request_headers` that stores its context id at address 608, sends a
/// callout, and pauses.
const STORE_CONTEXT_AND_CALL: &str =
    "(i32.store (i32.const 608) (local.get 0)) (call $call_authz_and_pause)";
/// Body for `proxy_on_http_call_response` that sends another callout from the context whose id
/// is stored at address 608.
const CALL_AGAIN: &str = "(drop (call $set_effective_context (i32.load (i32.const 608))))
    (drop (call $call_authz_and_pause))";
const MARK: &str = "(call $mark_a (i32.const 0))";
const TEAPOT: &str = "(call $respond (i32.const 418))";
const REQUEST_HEADERS: &str = "proxy_on_request_headers";
const NO_DELIVERY: &str = "";
const CALL_AND_PAUSE: &str = "(call $call_authz_and_pause)";
const RELAY_CALLOUT_BODY: &str = "(call $relay_callout_body (local.get 2) (local.get 3))";

/// Write a guest whose `callback` runs `body` and whose `proxy_on_http_call_response` runs
/// `delivery`, and return its path.
fn wat_guest(label: &str, callback: &str, body: &str, delivery: &str) -> PathBuf {
    let callbacks = format!(
        r#"(func (export "proxy_abi_version_0_2_1"))
  (func (export "proxy_on_vm_start") (param i32 i32) (result i32) i32.const 1)
  (func (export "proxy_on_configure") (param i32 i32) (result i32) i32.const 1)
  (func (export "proxy_on_done") (param i32) (result i32) i32.const 1)
  (func (export "proxy_on_delete") (param i32))
  (func (export "proxy_on_log") (param i32))
  (func (export "proxy_on_http_call_response") (param i32 i32 i32 i32 i32) {delivery})
  (func (export "{callback}") (param i32 i32 i32) (result i32) {body})"#
    );
    let wat = TEMPLATE.replace("\nCALLBACKS\n", &format!("\n{callbacks}\n"));
    let path = std::env::temp_dir().join(format!(
        "pingora-wasm-test-{label}-{}.wasm",
        std::process::id()
    ));
    std::fs::write(&path, wat::parse_str(wat).unwrap()).unwrap();
    path
}

fn guest(name: &str, label: &str, callback: &str, body: &str) -> WasmPluginConf {
    let mut conf = WasmPluginConf::new(name, wat_guest(label, callback, body, NO_DELIVERY));
    conf.slots = 2;
    conf.request_body = callback == REQUEST_BODY;
    conf
}

/// Build the conf for a plugin that buffers the whole request body, with `limit` as its
/// `request_body_limit`.
pub fn hold(label: &str, limit: usize) -> WasmPluginConf {
    let mut conf = guest("hold", label, REQUEST_BODY, HOLD_THEN_MARK);
    conf.request_body_limit = limit;
    conf
}

/// Build the conf for a plugin that prepends `a` to every request body chunk.
pub fn mark() -> WasmPluginConf {
    guest("mark", "mark", REQUEST_BODY, MARK)
}

/// Build the conf for a plugin that responds with 418 from `proxy_on_request_body`.
pub fn teapot_for_a_request_body() -> WasmPluginConf {
    guest("teapot", "teapot-request", REQUEST_BODY, TEAPOT)
}

/// Build the conf for a plugin that responds with 418 from `proxy_on_response_headers`.
pub fn teapot_for_a_response() -> WasmPluginConf {
    guest("teapot", "teapot-response", RESPONSE_HEADERS, TEAPOT)
}

/// Build the conf for a plugin that sends a callout from `proxy_on_request_headers` and
/// responds with the body of the callout response.
///
/// `timeout_limit` overrides the plugin's `callout_timeout_limit` when set.
pub fn relay_callout_body_plugin(name: &str, timeout_limit: Option<Duration>) -> WasmPluginConf {
    let path = wat_guest(name, REQUEST_HEADERS, CALL_AND_PAUSE, RELAY_CALLOUT_BODY);
    let mut conf = WasmPluginConf::new(name, path);
    conf.slots = 2;
    if let Some(limit) = timeout_limit {
        conf.callout_timeout_limit = limit;
    }
    conf
}

/// Write a guest made of the template and `callbacks`, and return its path.
///
/// `callbacks` must export every callback the template does not. The ABI version marker is
/// added here.
fn write_module(label: &str, callbacks: &str) -> PathBuf {
    let wat = TEMPLATE.replace(
        "\nCALLBACKS\n",
        &format!("\n(func (export \"proxy_abi_version_0_2_1\"))\n{callbacks}\n"),
    );
    let path = std::env::temp_dir().join(format!(
        "pingora-wasm-test-{label}-{}.wasm",
        std::process::id()
    ));
    std::fs::write(&path, wat::parse_str(wat).unwrap()).unwrap();
    path
}

/// Callback bodies of a test guest. The defaults continue and do nothing else.
struct Exports {
    configure: String,
    request_headers: String,
    response_headers: String,
    done: String,
    log: String,
    tick: String,
    http_call_response: String,
}

impl Default for Exports {
    fn default() -> Self {
        Exports {
            configure: "i32.const 1".to_string(),
            request_headers: "i32.const 0".to_string(),
            response_headers: "i32.const 0".to_string(),
            done: "i32.const 1".to_string(),
            log: String::new(),
            tick: String::new(),
            http_call_response: String::new(),
        }
    }
}

impl Exports {
    fn into_wat(self, data_segments: &str) -> String {
        format!(
            r#"{data_segments}
  (func (export "proxy_on_vm_start") (param i32 i32) (result i32) i32.const 1)
  (func (export "proxy_on_configure") (param i32 i32) (result i32) {})
  (func (export "proxy_on_request_headers") (param i32 i32 i32) (result i32) {})
  (func (export "proxy_on_response_headers") (param i32 i32 i32) (result i32) {})
  (func (export "proxy_on_done") (param i32) (result i32) {})
  (func (export "proxy_on_delete") (param i32))
  (func (export "proxy_on_log") (param i32) {})
  (func (export "proxy_on_tick") (param i32) {})
  (func (export "proxy_on_http_call_response") (param i32 i32 i32 i32 i32) {})"#,
            self.configure,
            self.request_headers,
            self.response_headers,
            self.done,
            self.log,
            self.tick,
            self.http_call_response,
        )
    }
}

/// Data segments for strings placed in guest memory from address 700 up.
struct MemoryTexts {
    data_segments: String,
    next_address: usize,
}

impl MemoryTexts {
    fn new() -> Self {
        MemoryTexts {
            data_segments: String::new(),
            next_address: 700,
        }
    }

    /// Add `text` and return its address and length.
    ///
    /// Each `/` is stored as a NUL byte, the separator between the segments of a property path.
    fn add(&mut self, text: &str) -> (usize, usize) {
        let at = self.next_address;
        let escaped = text.replace('/', "\\00");
        self.data_segments += &format!("(data (i32.const {at}) \"{escaped}\")\n");
        self.next_address += text.len() + 1;
        (at, text.len())
    }

    fn log_call(&mut self, text: &str) -> String {
        let (at, len) = self.add(text);
        format!("(drop (call $log (i32.const 2) (i32.const {at}) (i32.const {len})))")
    }

    fn log_property_calls(&mut self, paths: &[&str]) -> String {
        let calls: Vec<_> = paths
            .iter()
            .map(|path| {
                let (at, len) = self.add(path);
                format!("(call $log_property (i32.const {at}) (i32.const {len}))")
            })
            .collect();
        calls.join(" ")
    }

    fn property_to_header_calls(&mut self, map: u32, pairs: &[(&str, &str)]) -> String {
        let calls: Vec<_> = pairs
            .iter()
            .map(|(path, header)| {
                let (path_at, path_len) = self.add(path);
                let (name_at, name_len) = self.add(header);
                format!(
                    "(call $property_to_header (i32.const {map}) (i32.const {path_at}) (i32.const {path_len}) (i32.const {name_at}) (i32.const {name_len}))"
                )
            })
            .collect();
        calls.join(" ")
    }
}

fn one_slot_plugin(name: &str, path: PathBuf) -> WasmPluginConf {
    let mut conf = WasmPluginConf::new(name, path);
    conf.slots = 1;
    conf
}

/// Write a guest that adds the request header `header` with `value`, and return its path.
pub fn request_header_adder_module(label: &str, header: &str, value: &str) -> PathBuf {
    let mut texts = MemoryTexts::new();
    let (name_at, name_len) = texts.add(header);
    let (value_at, value_len) = texts.add(value);
    let exports = Exports {
        request_headers: format!(
            "(drop (call $add_header (i32.const 0) (i32.const {name_at}) (i32.const {name_len}) (i32.const {value_at}) (i32.const {value_len}))) i32.const 0"
        ),
        ..Exports::default()
    };
    write_module(label, &exports.into_wat(&texts.data_segments))
}

/// Build the conf for a plugin that adds the request header `header` with `value`.
pub fn request_header_adder(name: &str, header: &str, value: &str) -> WasmPluginConf {
    one_slot_plugin(name, request_header_adder_module(name, header, value))
}

/// Build the conf for a plugin that traps in `proxy_on_request_headers`.
pub fn trap_on_request_headers(name: &str) -> WasmPluginConf {
    let exports = Exports {
        request_headers: "unreachable".to_string(),
        ..Exports::default()
    };
    one_slot_plugin(name, write_module(name, &exports.into_wat("")))
}

/// Build the conf for a plugin that holds the request body and traps on its last chunk.
pub fn hold_request_body_then_trap(name: &str) -> WasmPluginConf {
    let mut conf = guest(name, name, REQUEST_BODY, HOLD_THEN_TRAP);
    conf.slots = 1;
    conf
}

/// Build the conf for a plugin that sends a callout from `proxy_on_request_headers`, pauses,
/// and never continues.
pub fn stay_paused_after_callout(name: &str) -> WasmPluginConf {
    let path = wat_guest(name, REQUEST_HEADERS, CALL_AND_PAUSE, NO_DELIVERY);
    one_slot_plugin(name, path)
}

/// Build the conf for a plugin that sends a callout from `proxy_on_request_headers`, pauses,
/// and sends another callout each time a callout result arrives.
///
/// A callout result is delivered to the root context, so the plugin makes its stream context
/// the effective one before it sends the next callout, as an SDK does.
pub fn callout_on_each_delivery(name: &str) -> WasmPluginConf {
    let path = wat_guest(name, REQUEST_HEADERS, STORE_CONTEXT_AND_CALL, CALL_AGAIN);
    one_slot_plugin(name, path)
}

/// Build the conf for a plugin that sets a 100 ms tick period and logs `log_text` on every
/// tick.
pub fn tick_logger(name: &str, log_text: &str) -> WasmPluginConf {
    let mut texts = MemoryTexts::new();
    let exports = Exports {
        configure: "(drop (call $set_tick_period (i32.const 100))) i32.const 1".to_string(),
        tick: texts.log_call(log_text),
        ..Exports::default()
    };
    one_slot_plugin(
        name,
        write_module(name, &exports.into_wat(&texts.data_segments)),
    )
}

/// Build the conf for a plugin that defines the counter `counter_name` and increments it on
/// every request.
pub fn request_counter(name: &str, counter_name: &str) -> WasmPluginConf {
    let mut texts = MemoryTexts::new();
    let (at, len) = texts.add(counter_name);
    let exports = Exports {
        configure: format!("(drop (call $define_metric (i32.const 0) (i32.const {at}) (i32.const {len}) (i32.const 620))) i32.const 1"),
        request_headers: "(drop (call $increment_metric (i32.load (i32.const 620)) (i64.const 1))) i32.const 0".to_string(),
        ..Exports::default()
    };
    one_slot_plugin(
        name,
        write_module(name, &exports.into_wat(&texts.data_segments)),
    )
}

/// The properties a [property_reader] plugin reads, and where each one goes.
#[derive(Default)]
pub struct PropertyReads<'a> {
    /// Property path and header name pairs, each added as a request header.
    pub to_request_headers: &'a [(&'a str, &'a str)],
    /// Property path and header name pairs, each added as a response header.
    pub to_response_headers: &'a [(&'a str, &'a str)],
    /// Property paths to log in `proxy_on_response_headers`.
    pub logged_in_on_response_headers: &'a [&'a str],
    /// Property paths to log in `proxy_on_log`.
    pub logged_in_on_log: &'a [&'a str],
}

/// Build the conf for a plugin that reads the properties listed in `reads`.
pub fn property_reader(name: &str, reads: PropertyReads<'_>) -> WasmPluginConf {
    let mut texts = MemoryTexts::new();
    let request = texts.property_to_header_calls(0, reads.to_request_headers);
    let response = texts.property_to_header_calls(2, reads.to_response_headers);
    let response_logs = texts.log_property_calls(reads.logged_in_on_response_headers);
    let exports = Exports {
        request_headers: format!("{request} i32.const 0"),
        response_headers: format!("{response} {response_logs} i32.const 0"),
        log: texts.log_property_calls(reads.logged_in_on_log),
        ..Exports::default()
    };
    one_slot_plugin(
        name,
        write_module(name, &exports.into_wat(&texts.data_segments)),
    )
}

/// Build the conf for a plugin that sends one callout from its first tick and logs `log_text`
/// when the callout response arrives.
pub fn tick_callout_sender(name: &str, log_text: &str) -> WasmPluginConf {
    let mut texts = MemoryTexts::new();
    let log = texts.log_call(log_text);
    let exports = Exports {
        configure: "(drop (call $set_tick_period (i32.const 50))) i32.const 1".to_string(),
        tick: "(if (i32.eqz (i32.load (i32.const 604)))
            (then (i32.store (i32.const 604) (i32.const 1)) (call $call_and_log_status)))"
            .to_string(),
        http_call_response: format!("(if (local.get 2) (then {log}))"),
        ..Exports::default()
    };
    one_slot_plugin(
        name,
        write_module(name, &exports.into_wat(&texts.data_segments)),
    )
}

/// Body for `proxy_on_http_call_response` that calls `proxy_done` on the context whose id
/// `proxy_on_done` stored at address 608.
const DONE_FOR_THE_HELD_CONTEXT: &str =
    "(drop (call $set_effective_context (i32.load (i32.const 608)))) (drop (call $proxy_done))";

/// Build the conf for a plugin that keeps its context after the request has ended.
///
/// The plugin sends a callout from `proxy_on_done` and returns `false`, calls `proxy_done` once
/// the callout result arrives, and logs `log_text` from `proxy_on_log`.
pub fn context_holder(name: &str, log_text: &str) -> WasmPluginConf {
    let mut texts = MemoryTexts::new();
    let exports = Exports {
        done: "(i32.store (i32.const 608) (local.get 0)) (call $call_and_log_status) i32.const 0"
            .to_string(),
        http_call_response: DONE_FOR_THE_HELD_CONTEXT.to_string(),
        log: texts.log_call(log_text),
        ..Exports::default()
    };
    one_slot_plugin(
        name,
        write_module(name, &exports.into_wat(&texts.data_segments)),
    )
}

/// Build the conf for a plugin that keeps its context while a callout of the request is still
/// in flight.
///
/// The plugin sends a callout from `proxy_on_request_headers` without pausing, returns `false`
/// from `proxy_on_done`, calls `proxy_done` when any callout result arrives, and logs
/// `log_text` from `proxy_on_log`.
pub fn context_holder_with_an_earlier_callout(name: &str, log_text: &str) -> WasmPluginConf {
    let mut texts = MemoryTexts::new();
    let exports = Exports {
        request_headers: "(call $call_without_pause)".to_string(),
        done: "(i32.store (i32.const 608) (local.get 0)) i32.const 0".to_string(),
        http_call_response: DONE_FOR_THE_HELD_CONTEXT.to_string(),
        log: texts.log_call(log_text),
        ..Exports::default()
    };
    one_slot_plugin(
        name,
        write_module(name, &exports.into_wat(&texts.data_segments)),
    )
}
