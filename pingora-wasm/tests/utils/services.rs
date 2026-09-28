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

//! The services of the test server: the port, the runtime, and the chain of each one.

use super::callout_origins::{CalloutOrigin, CalloutOriginPerPlugin};
use super::{fixture, guests};
use once_cell::sync::Lazy;
use pingora_wasm::{WasmPluginConf, WasmRuntime, WasmServices};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

const CALLOUT_TIMEOUT_LIMIT: Duration = Duration::from_millis(300);

fn plugin(name: &str, path: PathBuf, slots: usize, configuration: &str) -> WasmPluginConf {
    let mut conf = WasmPluginConf::new(name, path);
    conf.slots = slots;
    conf.configuration = configuration.as_bytes().to_vec();
    conf
}

static CALLOUT_ORIGINS: Lazy<Arc<CalloutOriginPerPlugin>> = Lazy::new(|| {
    CalloutOriginPerPlugin::start(&[
        ("auth-even", Some("0")),
        ("auth-odd", Some("1")),
        ("auth-no-response", None),
        ("relay-limit", None),
        ("relay-h1-close", None),
        ("relay-h2-close", None),
    ])
});

/// Return the origin that receives the callouts of `plugin`.
pub fn callout_origin(plugin: &str) -> Arc<CalloutOrigin> {
    CALLOUT_ORIGINS.origin(plugin)
}

/// Build a service with one plugin that sends its callouts to its own origin.
fn runtime_with_callout_origins(conf: WasmPluginConf) -> (WasmRuntime, Vec<&'static str>) {
    let name: &'static str = conf.name.clone().leak();
    let mut services = WasmServices::default();
    services.callout_upstreams = CALLOUT_ORIGINS.clone();
    let runtime = WasmRuntime::new_with_services(vec![conf], services).unwrap();
    (runtime, vec![name])
}

pub fn services() -> Vec<(u16, WasmRuntime, Vec<&'static str>, Option<usize>)> {
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
    let mut body = plugin("body", fixture("sdk-http-body"), 2, "");
    body.response_body = true;
    push(6391, single("body", body), None);
    push(6392, single("hold", guests::hold("hold", 1024)), None);
    push(6393, single("hold", guests::hold("hold-limit", 16)), None);
    push(
        6394,
        single("teapot", guests::teapot_for_a_response()),
        None,
    );
    push(
        6395,
        single("teapot", guests::teapot_for_a_request_body()),
        None,
    );
    push(6396, single("mark", guests::mark()), None);
    let auth = |name| plugin(name, fixture("sdk-http-auth-random"), 2, "");
    push(6397, runtime_with_callout_origins(auth("auth-even")), None);
    push(6398, runtime_with_callout_origins(auth("auth-odd")), None);
    push(
        6399,
        runtime_with_callout_origins(auth("auth-no-response")),
        None,
    );
    let relay = guests::relay_callout_body_plugin;
    let limit = Some(CALLOUT_TIMEOUT_LIMIT);
    push(
        6400,
        runtime_with_callout_origins(relay("relay-limit", limit)),
        None,
    );
    push(
        6401,
        runtime_with_callout_origins(relay("relay-h1-close", limit)),
        None,
    );
    push(
        6402,
        runtime_with_callout_origins(relay("relay-h2-close", None)),
        None,
    );
    services
}
