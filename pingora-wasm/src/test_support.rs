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
use pingora_proxy::Session;
use proxy_wasm_host::HeaderMap;
use std::ops::ControlFlow;
use std::path::PathBuf;
use tokio::io::{AsyncWriteExt, DuplexStream};

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
pub(crate) struct Wat {
    pub(crate) abi: bool,
    pub(crate) vm_start: &'static str,
    pub(crate) request_headers: &'static str,
    pub(crate) done: &'static str,
}

impl Default for Wat {
    fn default() -> Self {
        Wat {
            abi: true,
            vm_start: "i32.const 1",
            request_headers: "i32.const 0",
            done: "i32.const 1",
        }
    }
}

pub(crate) fn wat_guest(label: &str, guest: Wat) -> PathBuf {
    let abi = if guest.abi {
        r#"(func (export "proxy_abi_version_0_2_1"))"#
    } else {
        ""
    };
    let wat = format!(
        r#"(module
  (memory (export "memory") 1)
  (func (export "proxy_on_memory_allocate") (param i32) (result i32) i32.const 1024)
  {abi}
  (func (export "proxy_on_context_create") (param i32 i32))
  (func (export "proxy_on_vm_start") (param i32 i32) (result i32) {})
  (func (export "proxy_on_configure") (param i32 i32) (result i32) i32.const 1)
  (func (export "proxy_on_request_headers") (param i32 i32 i32) (result i32) {})
  (func (export "proxy_on_done") (param i32) (result i32) {})
  (func (export "proxy_on_log") (param i32))
  (func (export "proxy_on_delete") (param i32)))"#,
        guest.vm_start, guest.request_headers, guest.done
    );
    let path =
        std::env::temp_dir().join(format!("pingora-wasm-{label}-{}.wasm", std::process::id()));
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
