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

//! Configuration from a file

mod plugin;
mod properties;

use crate::callout::StaticCalloutUpstreams;
use crate::invalid_conf;
use crate::properties::WasmProperties;
use crate::runtime::{WasmPluginConf, WasmServices};
use pingora_core::upstreams::peer::HttpPeer;
use pingora_error::Result;
use serde::Deserialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

/// The plugins, chains, and plugin services of a proxy, as read from a configuration file.
///
/// It implements serde's `Deserialize`, and this crate does not read the file for you. Add it
/// as a field of your own configuration struct, or parse it from the top level of a file. The
/// format has to be self-describing, such as YAML, JSON, or TOML.
///
/// At its top level it ignores the keys it does not know, as Pingora's `ServerConf` does, so one
/// file can hold the settings for Pingora, your proxy, and your plugins. An unknown key inside a
/// plugin entry, its `limits`, or a callout upstream is an error.
///
/// To build a runtime from it, call [services](Self::services) and set on the result whatever
/// a file cannot hold, such as a log sink, a metric sink, a connector, or your own
/// [CalloutUpstreams](crate::CalloutUpstreams). Pass that to
/// [WasmRuntime::new_with_services](crate::WasmRuntime::new_with_services) together with
/// [plugins](Self::plugins), then build each chain with
/// [WasmRuntime::chain](crate::WasmRuntime::chain) from the names
/// [chain_plugins](Self::chain_plugins) returns. See `examples/wasm_proxy.yaml` for an
/// example.
#[non_exhaustive]
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct WasmConf {
    /// The plugins of the runtime. Default empty.
    ///
    /// In a file, a plugin entry needs `name` and `path`. Every other key is optional and
    /// defaults to what [WasmPluginConf::new] sets. A key sets the [WasmPluginConf] field of the
    /// same name, with these exceptions:
    ///
    /// - `configuration` and `vm_configuration` are strings. Set a configuration that is not
    ///   text in code.
    /// - `log_level` is one of `trace`, `debug`, `info`, `warn`, `error`, and `critical`.
    /// - `callout_timeout_limit_seconds`, `callout_wait_limit_seconds`, and
    ///   `rebuild_interval_seconds` set those times in whole seconds. Set a time below one
    ///   second in code.
    /// - `fail_policy` is `closed` or `open`.
    /// - `limits` is a mapping with the keys `cpu_time_ms`, `memory_bytes`,
    ///   `max_decoded_pairs`, `max_decoded_map_bytes`, `max_shared_names`, `max_name_bytes`,
    ///   `max_log_bytes`, `max_open_callouts`, and `table_elements`. A key that is left out
    ///   keeps the default of [Limits](crate::Limits). A file cannot remove a limit, and has no
    ///   key for a fuel limit.
    ///
    /// A relative `path` is resolved against the working directory of the process at the time
    /// the runtime is built.
    pub plugins: Vec<WasmPluginConf>,
    /// The chains by name, each with the names of its plugins in chain order. Default empty.
    pub chains: HashMap<String, Vec<String>>,
    /// The value for [WasmServices::max_callouts_in_flight]. Default `None`, in which case
    /// [WasmServices] keeps its own default.
    pub max_callouts_in_flight: Option<usize>,
    /// The value for [WasmServices::fixed_properties]. Default empty.
    ///
    /// In a file this is a nested mapping with one level per path segment, since a segment may
    /// itself contain a dot. Every leaf must be a string. Set a value of any other type in code.
    pub fixed_properties: WasmProperties,
    /// The upstreams plugins may send callouts to, by upstream name. Default empty.
    ///
    /// Each upstream is one fixed peer, which is enough for an example or a small deployment.
    /// For several backends, health checks, or other TLS options, implement
    /// [CalloutUpstreams](crate::CalloutUpstreams) and set it on the result of
    /// [services](Self::services).
    pub static_callout_upstreams: HashMap<String, CalloutUpstreamConf>,
    /// The value for [WasmServices::shutdown_wait_limit] in whole seconds. Default `None`, in
    /// which case [WasmServices] keeps its own default.
    pub shutdown_wait_limit_seconds: Option<u64>,
    /// The value for [WasmServices::threads], which is Pingora's own `threads` key when one file
    /// holds both. Default `None`, in which case [WasmServices] keeps its own default.
    pub threads: Option<usize>,
}

/// The peer of one callout upstream in [WasmConf::static_callout_upstreams].
#[non_exhaustive]
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CalloutUpstreamConf {
    /// The address of the peer as an IP address and a port, e.g. `10.0.0.5:8181`.
    ///
    /// A hostname is not accepted, so no DNS lookup can block startup.
    pub address: SocketAddr,
    /// Whether to connect to the peer over TLS. Default `false`.
    #[serde(default)]
    pub tls: bool,
    /// The SNI to send to a TLS peer. Default empty.
    #[serde(default)]
    pub sni: String,
}

impl CalloutUpstreamConf {
    /// Create the configuration for a peer at `address`, without TLS and with an empty SNI.
    pub fn new(address: SocketAddr) -> Self {
        CalloutUpstreamConf {
            address,
            tls: false,
            sni: String::new(),
        }
    }
}

impl WasmConf {
    /// Return the [WasmServices] this configuration describes.
    ///
    /// The result has [max_callouts_in_flight](Self::max_callouts_in_flight),
    /// [shutdown_wait_limit_seconds](Self::shutdown_wait_limit_seconds), and
    /// [threads](Self::threads) if they are set, the
    /// [fixed_properties](Self::fixed_properties), and, unless
    /// [static_callout_upstreams](Self::static_callout_upstreams) is empty, a
    /// [StaticCalloutUpstreams] with one peer per upstream.
    pub fn services(&self) -> WasmServices {
        let mut services = WasmServices::default();
        if let Some(limit) = self.max_callouts_in_flight {
            services.max_callouts_in_flight = limit;
        }
        if let Some(threads) = self.threads {
            services.threads = threads;
        }
        if let Some(seconds) = self.shutdown_wait_limit_seconds {
            services.shutdown_wait_limit = Duration::from_secs(seconds);
        }
        services.fixed_properties = self.fixed_properties.clone();
        if self.static_callout_upstreams.is_empty() {
            return services;
        }
        let mut upstreams = StaticCalloutUpstreams::new();
        for (name, upstream) in &self.static_callout_upstreams {
            let peer = HttpPeer::new(upstream.address, upstream.tls, upstream.sni.clone());
            upstreams.insert(name, peer);
        }
        services.callout_upstreams = Arc::new(upstreams);
        services
    }

    /// Return the plugin names of the chain `name`, in chain order.
    ///
    /// Pass the result to [WasmRuntime::chain](crate::WasmRuntime::chain) to build the chain.
    /// Returns [ERR_INVALID_CONF](crate::ERR_INVALID_CONF) if the configuration has no chain with
    /// that name.
    pub fn chain_plugins(&self, name: &str) -> Result<Vec<&str>> {
        match self.chains.get(name) {
            Some(plugins) => Ok(plugins.iter().map(String::as_str).collect()),
            None => Err(invalid_conf(format!(
                "wasm chain {name}: not in the configuration"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::callout::CalloutTarget;
    use crate::test_support::fixture;
    use crate::{WasmRuntime, ERR_INVALID_CONF};
    use pingora_http::RequestHeader;

    const CONF: &str = r#"
threads: 2
max_callouts_in_flight: 16
shutdown_wait_limit_seconds: 2
plugins:
  - name: auth
    path: auth.wasm
  - name: stats
    path: stats.wasm
chains:
  default: [auth, stats]
fixed_properties:
  node:
    metadata:
      NAME: edge-1
static_callout_upstreams:
  authz:
    address: 10.0.0.5:8181
  audit:
    address: "[::1]:443"
    tls: true
    sni: audit.internal
"#;

    fn conf(yaml: &str) -> WasmConf {
        serde_yaml::from_str(yaml).unwrap()
    }

    #[test]
    fn conf_reads_plugins_chains_and_services() {
        let conf = conf(CONF);

        let services = conf.services();

        let names: Vec<_> = conf.plugins.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, ["auth", "stats"]);
        assert_eq!(conf.chain_plugins("default").unwrap(), ["auth", "stats"]);
        assert_eq!(services.max_callouts_in_flight, 16);
        assert_eq!(services.shutdown_wait_limit, Duration::from_secs(2));
        assert_eq!(services.threads, 2);
        let node_name = services.fixed_properties.get(&["node", "metadata", "NAME"]);
        assert_eq!(node_name, Some(&b"edge-1"[..]));
        let upstreams = &services.callout_upstreams;
        assert!(upstreams.has_upstream("auth", "authz"));
        assert!(upstreams.has_upstream("auth", "audit"));
        assert!(!upstreams.has_upstream("auth", "other"));
    }

    #[tokio::test]
    async fn static_upstream_becomes_peer() {
        let services = conf(CONF).services();
        let request = RequestHeader::build("GET", b"/", None).unwrap();
        let target = CalloutTarget::new("auth", "audit", &request);

        let peer = services.callout_upstreams.callout_peer(&target).await;

        let peer = peer.unwrap();
        assert_eq!(peer._address.to_string(), "[::1]:443");
        assert!(peer.is_tls());
        assert_eq!(peer.sni, "audit.internal");
    }

    #[test]
    fn empty_conf_keeps_service_defaults() {
        let services = conf("{}").services();

        let defaults = WasmServices::default();
        assert_eq!(
            services.max_callouts_in_flight,
            defaults.max_callouts_in_flight
        );
        assert_eq!(services.shutdown_wait_limit, defaults.shutdown_wait_limit);
        assert_eq!(services.threads, defaults.threads);
        assert_eq!(services.fixed_properties, defaults.fixed_properties);
        assert!(!services.callout_upstreams.has_upstream("a", "authz"));
    }

    #[test]
    fn upstream_address_must_be_ip_and_port() {
        let cases = ["authz.internal:8181", "10.0.0.5", ""];

        for address in cases {
            let yaml = format!("static_callout_upstreams:\n  authz:\n    address: '{address}'\n");

            let read = serde_yaml::from_str::<WasmConf>(&yaml);

            let err = read.unwrap_err().to_string();
            assert!(
                err.contains("static_callout_upstreams.authz.address"),
                "{err}"
            );
        }
    }

    #[test]
    fn upstream_built_in_code_becomes_peer_without_tls() {
        let mut conf = WasmConf::default();
        let address = "10.0.0.5:8181".parse().unwrap();
        conf.static_callout_upstreams
            .insert("authz".to_string(), CalloutUpstreamConf::new(address));

        let services = conf.services();

        assert!(services.callout_upstreams.has_upstream("auth", "authz"));
        let upstream = &conf.static_callout_upstreams["authz"];
        assert!(!upstream.tls && upstream.sni.is_empty());
    }

    #[test]
    fn chain_plugins_rejects_unknown_chain() {
        let err = conf(CONF).chain_plugins("admin").unwrap_err();

        assert_eq!(err.etype(), &ERR_INVALID_CONF);
        let message = "wasm chain admin: not in the configuration";
        assert!(err.to_string().contains(message), "{err}");
    }

    #[test]
    fn unknown_key_is_ignored_at_top_level_and_rejected_in_upstream() {
        let top_level = serde_yaml::from_str::<WasmConf>("version: 1\nthreads: 2\n");
        let upstream = serde_yaml::from_str::<WasmConf>(
            "static_callout_upstreams:\n  authz:\n    address: 10.0.0.5:80\n    tsl: true\n",
        );

        assert!(top_level.is_ok());
        let err = upstream.unwrap_err().to_string();
        assert!(err.contains("unknown field `tsl`"), "{err}");
    }

    #[test]
    fn example_file_builds_its_chain() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/examples/wasm_proxy.yaml");
        let mut conf = conf(&std::fs::read_to_string(path).unwrap());
        for plugin in &mut conf.plugins {
            let file_name = plugin
                .path
                .file_stem()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();
            plugin.path = fixture(&file_name);
        }

        let runtime = WasmRuntime::new_with_services(conf.plugins.clone(), conf.services());

        let chain = runtime
            .unwrap()
            .chain(&conf.chain_plugins("default").unwrap());
        assert!(chain.is_ok(), "{chain:?}");
    }
}
