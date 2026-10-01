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
use pingora_wasm::{
    PrometheusMetricSink, WasmPluginConf, WasmProperties, WasmRuntime, WasmServices,
};
use prometheus::{Encoder, Registry, TextEncoder};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
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
        ("root-callout", Some("0")),
        ("held-context", Some("0")),
        ("relay-metric", None),
        ("held-earlier", None),
    ])
});

/// The registry of the services that publish metrics, with the one sink that all of them use.
static METRICS: Lazy<(Registry, Arc<PrometheusMetricSink>)> = Lazy::new(|| {
    let registry = Registry::new();
    let sink = Arc::new(PrometheusMetricSink::new(registry.clone()).unwrap());
    (registry, sink)
});

/// Return the text that a Prometheus scrape of the test metrics returns.
pub fn metrics_output() -> String {
    let mut buffer = Vec::new();
    TextEncoder::new()
        .encode(&METRICS.0.gather(), &mut buffer)
        .unwrap();
    String::from_utf8(buffer).unwrap()
}

/// Publish the metrics of a runtime, and give it the fixed property `node.name`.
fn with_metrics_and_node_name(services: &mut WasmServices) {
    services.metric_sink = METRICS.1.clone();
    let mut fixed = WasmProperties::new();
    fixed.insert(&["node", "name"], "test-node");
    services.fixed_properties = fixed;
}

/// Return the origin that receives the callouts of `plugin`.
pub fn callout_origin(plugin: &str) -> Arc<CalloutOrigin> {
    CALLOUT_ORIGINS.origin(plugin)
}

/// The plugins of one runtime.
struct RuntimePlan {
    plugins: Vec<WasmPluginConf>,
    /// Whether each plugin sends its callouts to its own origin.
    has_callout_origins: bool,
}

impl RuntimePlan {
    fn build(self) -> WasmRuntime {
        let mut services = WasmServices::default();
        if self.has_callout_origins {
            services.callout_upstreams = CALLOUT_ORIGINS.clone();
        }
        with_metrics_and_node_name(&mut services);
        WasmRuntime::new_with_services(self.plugins, services).unwrap()
    }
}

/// The services to start, and the runtimes that they use.
#[derive(Default)]
struct ServicePlans {
    runtimes: Vec<RuntimePlan>,
    /// The port, the index of the runtime, the chain, and the thread count of each service.
    services: Vec<(u16, usize, Vec<&'static str>, Option<usize>)>,
}

impl ServicePlans {
    /// Add a runtime with `plugins`, and return its index.
    fn runtime(&mut self, plugins: Vec<WasmPluginConf>, has_callout_origins: bool) -> usize {
        self.runtimes.push(RuntimePlan {
            plugins,
            has_callout_origins,
        });
        self.runtimes.len() - 1
    }

    /// Add a service on `port` that runs a chain of all of `plugins`, in that order.
    fn service(&mut self, port: u16, plugins: Vec<WasmPluginConf>, threads: Option<usize>) {
        let chain = chain_of(&plugins);
        let runtime = self.runtime(plugins, false);
        self.services.push((port, runtime, chain, threads));
    }

    /// Add a service on `port` with one plugin that sends its callouts to its own origin.
    fn service_with_callout_origin(&mut self, port: u16, plugin: WasmPluginConf) {
        let chain = chain_of(std::slice::from_ref(&plugin));
        let runtime = self.runtime(vec![plugin], true);
        self.services.push((port, runtime, chain, None));
    }
}

fn chain_of(plugins: &[WasmPluginConf]) -> Vec<&'static str> {
    plugins
        .iter()
        .map(|plugin| &*plugin.name.clone().leak())
        .collect()
}

/// Return the port, the runtime, the chain, and the thread count of each service.
///
/// A runtime compiles its plugins when it is built, so the runtimes are built on one thread
/// each.
pub fn services() -> Vec<(u16, WasmRuntime, Vec<&'static str>, Option<usize>)> {
    let add = || plugin("add", fixture("add-request-header"), 2, "");
    let example = |slots| plugin("example", fixture("http-example"), slots, "");
    let config = |name, value| plugin(name, fixture("sdk-http-config"), 2, value);
    let headers = plugin("headers", fixture("sdk-http-headers"), 2, "");
    let mut body = plugin("body", fixture("sdk-http-body"), 2, "");
    body.response_body = true;
    let auth = |name| plugin(name, fixture("sdk-http-auth-random"), 2, "");
    let relay = guests::relay_callout_body_plugin;
    let limit = Some(CALLOUT_TIMEOUT_LIMIT);

    let mut plans = ServicePlans::default();
    plans.service(6380, vec![add()], None);
    plans.service(6381, vec![add(), example(2)], None);
    plans.service(6382, vec![config("hello", "hello")], None);
    plans.service(6383, vec![headers], None);
    plans.service(6384, vec![example(1)], None);
    plans.service(6385, vec![example(4)], Some(4));
    plans.service(6386, vec![config("a", "a"), config("b", "b")], None);
    plans.service(6387, vec![config("hello", "hello"), example(2)], None);
    plans.service(6388, vec![example(2)], None);
    let shared = plans.runtime(
        vec![
            plugin("add", fixture("add-request-header"), 1, ""),
            plugin("config", fixture("sdk-http-config"), 1, "hello"),
        ],
        false,
    );
    plans.services.push((6389, shared, vec!["add"], None));
    plans
        .services
        .push((6390, shared, vec!["add", "config"], None));
    plans.service(6391, vec![body], None);
    plans.service(6392, vec![guests::hold("hold", 1024)], None);
    plans.service(6393, vec![guests::hold("hold-limit", 16)], None);
    plans.service(6394, vec![guests::teapot_for_a_response()], None);
    plans.service(6395, vec![guests::teapot_for_a_request_body()], None);
    plans.service(6396, vec![guests::mark()], None);
    plans.service_with_callout_origin(6397, auth("auth-even"));
    plans.service_with_callout_origin(6398, auth("auth-odd"));
    plans.service_with_callout_origin(6399, auth("auth-no-response"));
    plans.service_with_callout_origin(6400, relay("relay-limit", limit));
    plans.service_with_callout_origin(6401, relay("relay-h1-close", limit));
    plans.service_with_callout_origin(6402, relay("relay-h2-close", None));
    plans.service(
        6403,
        vec![guests::tick_logger("ticks", "tick of 6403")],
        None,
    );
    plans.service(6404, vec![example(2)], None);
    let counter = guests::request_counter("counter", "wasm_test_requests");
    plans.service(6405, vec![counter], None);
    let request_properties = guests::property_reader(
        "request-properties",
        &[
            ("request/path", "x-path"),
            ("request/method", "x-method"),
            ("request/protocol", "x-protocol"),
            ("request/scheme", "x-scheme"),
            ("request/host", "x-host"),
            ("source/address", "x-source"),
            ("destination/address", "x-destination"),
            ("xds/route_name", "x-route"),
            ("node/name", "x-node"),
        ],
        &[],
        &[],
    );
    plans.service(6406, vec![request_properties], None);
    let response_properties = guests::property_reader(
        "response-properties",
        &[],
        &[("upstream/address", "x-upstream")],
        &["response/code"],
    );
    plans.service(6407, vec![response_properties], None);
    let logging_properties = guests::property_reader(
        "logging-properties",
        &[],
        &[],
        &["request/size", "response/size"],
    );
    plans.service(6408, vec![logging_properties], None);
    let root_callout = guests::root_callout("root-callout", "root callout response of 6409");
    plans.service_with_callout_origin(6409, root_callout);
    let held_context = guests::held_context("held-context", "held context of 6410 logged");
    plans.service_with_callout_origin(6410, held_context);
    plans.service_with_callout_origin(6411, relay("relay-metric", limit));
    let held_earlier =
        guests::held_context_with_an_earlier_callout("held-earlier", "held context of 6412 logged");
    plans.service_with_callout_origin(6412, held_earlier);

    let runtimes: Vec<WasmRuntime> = thread::scope(|scope| {
        let builds: Vec<_> = plans
            .runtimes
            .into_iter()
            .map(|plan| scope.spawn(|| plan.build()))
            .collect();
        builds
            .into_iter()
            .map(|build| build.join().unwrap())
            .collect()
    });
    plans
        .services
        .into_iter()
        .map(|(port, runtime, chain, threads)| (port, runtimes[runtime].clone(), chain, threads))
        .collect()
}
