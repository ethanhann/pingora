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

//! Small guests for behavior that no fixture has.

use pingora_wasm::WasmPluginConf;
use std::path::PathBuf;
use std::time::Duration;

const TEMPLATE: &str = include_str!("../fixtures/guest.wat");
const REQUEST_BODY: &str = "proxy_on_request_body";
const RESPONSE_HEADERS: &str = "proxy_on_response_headers";
/// Hold the request body until its end, then write `a` in front of it.
const HOLD_THEN_MARK: &str = "(if (result i32) (local.get 2)
    (then (call $mark_a (i32.const 0))) (else (i32.const 1)))";
const MARK: &str = "(call $mark_a (i32.const 0))";
const TEAPOT: &str = "(call $respond (i32.const 418))";
const REQUEST_HEADERS: &str = "proxy_on_request_headers";
const NO_DELIVERY: &str = "";
const CALL_AND_PAUSE: &str = "(call $call_authz_and_pause)";
const RELAY_CALLOUT_BODY: &str = "(call $relay_callout_body (local.get 2) (local.get 3))";

/// Build a guest whose `callback` has the body `body` and whose
/// `proxy_on_http_call_response` has the body `delivery`, and return its path.
fn wat_guest(label: &str, callback: &str, body: &str, delivery: &str) -> PathBuf {
    let callbacks = format!(
        r#"(func (export "proxy_abi_version_0_2_1"))
  (func (export "proxy_on_vm_start") (param i32 i32) (result i32) i32.const 1)
  (func (export "proxy_on_done") (param i32) (result i32) i32.const 1)
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

/// Build the configuration of a guest that holds the request body until its end.
pub fn hold(label: &str, limit: usize) -> WasmPluginConf {
    let mut conf = guest("hold", label, REQUEST_BODY, HOLD_THEN_MARK);
    conf.request_body_limit = limit;
    conf
}

/// Build the configuration of a guest that writes `a` in front of each request body chunk.
pub fn mark() -> WasmPluginConf {
    guest("mark", "mark", REQUEST_BODY, MARK)
}

/// Build the configuration of a guest that responds to the request body with 418.
pub fn teapot_for_a_request_body() -> WasmPluginConf {
    guest("teapot", "teapot-request", REQUEST_BODY, TEAPOT)
}

/// Build the configuration of a guest that responds to the response headers with 418.
pub fn teapot_for_a_response() -> WasmPluginConf {
    guest("teapot", "teapot-response", RESPONSE_HEADERS, TEAPOT)
}

/// Build the configuration of a guest that makes a callout from the request headers, and
/// responds with the body of the callout response. `timeout_limit` sets the callout timeout
/// limit of the plugin.
pub fn relay_callout_body_plugin(name: &str, timeout_limit: Option<Duration>) -> WasmPluginConf {
    let path = wat_guest(name, REQUEST_HEADERS, CALL_AND_PAUSE, RELAY_CALLOUT_BODY);
    let mut conf = WasmPluginConf::new(name, path);
    conf.slots = 2;
    if let Some(limit) = timeout_limit {
        conf.callout_timeout_limit = limit;
    }
    conf
}
