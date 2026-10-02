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

//! Test server services
//!
//! Each service has a port, a runtime, and a chain of that runtime's plugins.

use super::callout_origins::{CalloutOrigin, CalloutOriginPerPlugin};
use super::{fixture, guests};
use once_cell::sync::Lazy;
use pingora_wasm::{
    FailPolicy, PrometheusMetricSink, WasmConf, WasmPluginConf, WasmRuntime, WasmServices,
};
use prometheus::{Encoder, Registry, TextEncoder};
use std::fs;
use std::path::{Path, PathBuf};
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
        ("stay-paused", Some("0")),
        ("callout-chain", Some("0")),
    ])
});

/// The Prometheus registry for the test metrics, and the one sink every runtime publishes to.
static METRIC_REGISTRY_AND_SINK: Lazy<(Registry, Arc<PrometheusMetricSink>)> = Lazy::new(|| {
    let registry = Registry::new();
    let sink = Arc::new(PrometheusMetricSink::new(registry.clone()).unwrap());
    (registry, sink)
});

/// Return the test metrics in the Prometheus text format, as a scrape would.
pub fn metrics_text() -> String {
    let mut buffer = Vec::new();
    TextEncoder::new()
        .encode(&METRIC_REGISTRY_AND_SINK.0.gather(), &mut buffer)
        .unwrap();
    String::from_utf8(buffer).unwrap()
}

/// Attach the shared metric sink and set the fixed property `node.name` to `test-node`.
fn add_metric_sink_and_node_name(services: &mut WasmServices) {
    services.metric_sink = METRIC_REGISTRY_AND_SINK.1.clone();
    services
        .fixed_properties
        .insert(&["node", "name"], "test-node");
}

/// Return the origin receiving the callouts of `plugin`.
pub fn callout_origin(plugin: &str) -> Arc<CalloutOrigin> {
    CALLOUT_ORIGINS.origin(plugin)
}

/// The plugins of one runtime and the services it is built with.
struct RuntimePlan {
    plugins: Vec<WasmPluginConf>,
    services: WasmServices,
}

impl RuntimePlan {
    fn build(mut self) -> WasmRuntime {
        add_metric_sink_and_node_name(&mut self.services);
        self.services.log_sink = Arc::new(super::GuestMessageSink);
        WasmRuntime::new_with_services(self.plugins, self.services).unwrap()
    }
}

/// The services to start and the runtimes they run on.
#[derive(Default)]
struct ServicePlans {
    runtimes: Vec<RuntimePlan>,
    /// Port, runtime index, chain, and thread count of each service.
    services: Vec<(u16, usize, Vec<&'static str>, Option<usize>)>,
}

impl ServicePlans {
    /// Add a runtime with `plugins` and return its index.
    ///
    /// With `has_callout_origins` set, callouts are routed to the per-plugin origins.
    fn runtime(&mut self, plugins: Vec<WasmPluginConf>, has_callout_origins: bool) -> usize {
        let mut services = WasmServices::default();
        if has_callout_origins {
            services.callout_upstreams = CALLOUT_ORIGINS.clone();
        }
        self.runtimes.push(RuntimePlan { plugins, services });
        self.runtimes.len() - 1
    }

    /// Add a service on `port` whose runtime and `default` chain are read from a YAML file.
    fn service_from_conf_file(&mut self, port: u16, path: &Path) {
        let conf: WasmConf = serde_yaml::from_str(&fs::read_to_string(path).unwrap()).unwrap();
        let chain = conf.chain_plugins("default").unwrap();
        let chain = chain.iter().map(|name| &*name.to_string().leak()).collect();
        self.runtimes.push(RuntimePlan {
            plugins: conf.plugins.clone(),
            services: conf.services(),
        });
        let runtime = self.runtimes.len() - 1;
        self.services.push((port, runtime, chain, None));
    }

    /// Add a service on `port` with its own runtime and a chain of all `plugins`, in order.
    fn service(&mut self, port: u16, plugins: Vec<WasmPluginConf>, threads: Option<usize>) {
        let chain = chain_of(&plugins);
        let runtime = self.runtime(plugins, false);
        self.services.push((port, runtime, chain, threads));
    }

    /// Add a service on `port` running one plugin whose callouts go to its own origin.
    fn service_with_callout_origin(&mut self, port: u16, plugin: WasmPluginConf) {
        let chain = chain_of(std::slice::from_ref(&plugin));
        let runtime = self.runtime(vec![plugin], true);
        self.services.push((port, runtime, chain, None));
    }
}

/// Write a configuration file with two plugins that each add an `x-order` request header, and a
/// `default` chain of both.
///
/// The file is written at run time because the guests are built in a temporary directory.
fn conf_file_with_two_plugins() -> PathBuf {
    let first = guests::request_header_adder_module("yaml-first", "x-order", "first");
    let second = guests::request_header_adder_module("yaml-second", "x-order", "second");
    let yaml = format!(
        "version: 1\nplugins:\n  - name: first\n    path: '{}'\n  - name: second\n    path: '{}'\n\
         chains:\n  default: [first, second]\n",
        first.display(),
        second.display()
    );
    let path = first.with_extension("yaml");
    fs::write(&path, yaml).unwrap();
    path
}

fn chain_of(plugins: &[WasmPluginConf]) -> Vec<&'static str> {
    plugins
        .iter()
        .map(|plugin| &*plugin.name.clone().leak())
        .collect()
}

/// Return the port, runtime, chain, and thread count of each service.
///
/// Building a runtime compiles its plugins, so each runtime is built on its own thread.
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
        guests::PropertyReads {
            to_request_headers: &[
                ("request/path", "x-path"),
                ("request/url_path", "x-url-path"),
                ("request/method", "x-method"),
                ("request/protocol", "x-protocol"),
                ("request/scheme", "x-scheme"),
                ("request/host", "x-host"),
                ("source/address", "x-source"),
                ("destination/address", "x-destination"),
                ("xds/route_name", "x-route"),
                ("node/name", "x-node"),
            ],
            logged_in_on_log: &["destination/port", "request/time"],
            ..guests::PropertyReads::default()
        },
    );
    plans.service(6406, vec![request_properties], None);
    let response_properties = guests::property_reader(
        "response-properties",
        guests::PropertyReads {
            to_response_headers: &[("upstream/address", "x-upstream")],
            logged_in_on_response_headers: &["response/code", "upstream/port"],
            ..guests::PropertyReads::default()
        },
    );
    plans.service(6407, vec![response_properties], None);
    let logging_properties = guests::property_reader(
        "logging-properties",
        guests::PropertyReads {
            logged_in_on_log: &["request/size", "response/size", "request/duration"],
            ..guests::PropertyReads::default()
        },
    );
    plans.service(6408, vec![logging_properties], None);
    let tick_callout_sender =
        guests::tick_callout_sender("root-callout", "root callout response of 6409");
    plans.service_with_callout_origin(6409, tick_callout_sender);
    let context_holder = guests::context_holder("held-context", "held context of 6410 logged");
    plans.service_with_callout_origin(6410, context_holder);
    plans.service_with_callout_origin(6411, relay("relay-metric", limit));
    let held_earlier = guests::context_holder_with_an_earlier_callout(
        "held-earlier",
        "held context of 6412 logged",
    );
    plans.service_with_callout_origin(6412, held_earlier);

    plans.service_from_conf_file(6413, &conf_file_with_two_plugins());
    plans.service(
        6414,
        vec![plugin("trap-closed", fixture("http-example"), 1, "")],
        None,
    );
    let open = |mut conf: WasmPluginConf| {
        conf.fail_policy = FailPolicy::Open;
        conf
    };
    let adds_header = guests::request_header_adder("next", "x-order", "next");
    let trap_open = open(guests::trap_on_request_headers("trap-open"));
    plans.service(6415, vec![trap_open, adds_header], None);
    let hold_then_trap = open(guests::hold_request_body_then_trap("hold-trap-open"));
    plans.service(6416, vec![hold_then_trap], None);
    let stay_paused = open(guests::stay_paused_after_callout("stay-paused"));
    plans.service_with_callout_origin(6417, stay_paused);
    let mut callout_chain = guests::callout_on_each_delivery("callout-chain");
    callout_chain.callout_timeout_limit = CALLOUT_TIMEOUT_LIMIT;
    callout_chain.callout_wait_limit = CALLOUT_TIMEOUT_LIMIT;
    plans.service_with_callout_origin(6418, callout_chain);
    let code_logger = guests::property_reader(
        "code-logger",
        guests::PropertyReads {
            logged_in_on_log: &["response/code"],
            ..guests::PropertyReads::default()
        },
    );
    let trap_after_logger = guests::trap_on_request_headers("trap-after-logger");
    plans.service(6419, vec![code_logger, trap_after_logger], None);
    plans.service(6420, vec![open(guests::hold("hold-limit-open", 16))], None);

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
