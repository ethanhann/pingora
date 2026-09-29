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

//! The plugins of a proxy and their guests, built once and shared by every request.

mod log_sink;
mod plugin;
pub(crate) mod pool;
mod services;
mod ticker;

pub use plugin::WasmPluginConf;
pub(crate) use services::CalloutLauncher;
pub use services::WasmServices;

use crate::callout::{CalloutSender, ConnectorSender};
use crate::chain::WasmChain;
use pingora_core::connectors::http::Connector;
use pingora_error::{Error, ErrorType, OrErr, Result};
use pool::GuestPool;
use proxy_wasm_host::abi::v0_2_1::{GuestSpec, Host, InMemoryStore, SharedServices};
use proxy_wasm_host::{Engine, EngineConfig, Module};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use ticker::{with_ticker, Ticker};

/// The compiled plugins of a proxy and their guests.
///
/// Build it once, before the server starts, and clone it where you need it. Clones share the
/// same plugins. All plugins share one data store, and plugins with the same VM id see the same
/// data.
///
/// To reload plugins, build a new runtime and use it for new requests. A request that started
/// on the old runtime finishes on it.
#[derive(Clone)]
pub struct WasmRuntime {
    pub(crate) inner: Arc<RuntimeInner>,
}

pub(crate) struct RuntimeInner {
    engine: Engine,
    ticker: Ticker,
    pub(crate) pools: Vec<GuestPool>,
    pub(crate) callout_launcher: CalloutLauncher,
    names: HashMap<String, usize>,
}

impl WasmRuntime {
    /// Compile each plugin and start its guests.
    ///
    /// Guest log lines go to the `log` crate with the target `pingora_wasm::guest`.
    ///
    /// # Errors
    ///
    /// The error message names the plugin. The build fails when a file cannot be read, when a
    /// file is not a Proxy-Wasm module, when a plugin refuses to start or traps while it starts,
    /// when two plugins have the same name, when a plugin has zero slots, when its limits set
    /// fuel, or when one of its body or callout limits is zero.
    pub fn new(plugins: Vec<WasmPluginConf>) -> Result<Self> {
        Self::new_with_services(plugins, WasmServices::default())
    }

    /// Compile each plugin and start its guests, with the services of your proxy.
    ///
    /// Use it when your plugins send callouts, or to send guest log lines to your own logger,
    /// for example to keep them in the `tracing` span of the request. Otherwise it is the same
    /// as [WasmRuntime::new].
    ///
    /// # Errors
    ///
    /// The errors of [WasmRuntime::new], and an error when
    /// [max_callouts_in_flight](WasmServices::max_callouts_in_flight) is zero or over its
    /// maximum.
    pub fn new_with_services(plugins: Vec<WasmPluginConf>, services: WasmServices) -> Result<Self> {
        let connector = services
            .callout_connector
            .clone()
            .unwrap_or_else(|| Arc::new(Connector::new(None)));
        let sender = Arc::new(ConnectorSender {
            connector,
            upstreams: services.callout_upstreams.clone(),
        });
        Self::new_with_callout_sender(plugins, services, sender)
    }

    pub(crate) fn new_with_callout_sender(
        plugins: Vec<WasmPluginConf>,
        services: WasmServices,
        sender: Arc<dyn CalloutSender>,
    ) -> Result<Self> {
        let sink = services.log_sink;
        let upstreams = services.callout_upstreams;
        let callout_launcher = CalloutLauncher::new(sender, services.max_callouts_in_flight)?;
        if plugins.is_empty() {
            return Error::e_explain(ErrorType::InternalError, "no wasm plugin to run");
        }
        let mut names = HashMap::with_capacity(plugins.len());
        for (index, plugin) in plugins.iter().enumerate() {
            plugin.check()?;
            if names.insert(plugin.name.clone(), index).is_some() {
                return Error::e_explain(
                    ErrorType::InternalError,
                    format!("wasm plugin {} is listed twice", plugin.name),
                );
            }
        }
        let engine = EngineConfig::new()
            .with_external_ticks(true)
            .build()
            .or_err(ErrorType::InternalError, "failed to build the wasm engine")?;
        let host =
            Host::new(&engine).or_err(ErrorType::InternalError, "failed to link the host")?;
        let shared: Arc<dyn SharedServices> = Arc::new(InMemoryStore::new());
        let pools = with_ticker(&engine, || {
            plugins
                .iter()
                .map(|plugin| {
                    let bytes =
                        std::fs::read(&plugin.path).or_err_with(ErrorType::ReadError, || {
                            format!(
                                "wasm plugin {} cannot read {}",
                                plugin.name,
                                plugin.path.display()
                            )
                        })?;
                    let module = Module::new(&engine, &bytes)
                        .or_err_with(ErrorType::InternalError, || {
                            format!("wasm plugin {} does not compile", plugin.name)
                        })?;
                    let services = plugin.services(sink.clone(), shared.clone());
                    let spec = GuestSpec::new(&host, &module, services, &plugin.limits)
                        .or_err_with(ErrorType::InternalError, || {
                            format!("wasm plugin {} is not a supported module", plugin.name)
                        })?;
                    GuestPool::new(
                        plugin.name.clone(),
                        spec,
                        plugin.plugin_config(),
                        plugin.slots,
                        plugin.phase_conf(),
                        plugin.callout_conf(upstreams.clone()),
                    )
                })
                .collect::<Result<Vec<_>>>()
        })?;
        Ok(WasmRuntime {
            inner: Arc::new(RuntimeInner {
                engine,
                ticker: Ticker::new(),
                pools,
                callout_launcher,
                names,
            }),
        })
    }

    /// Build a chain of the named plugins.
    ///
    /// The request phase runs the plugins in this order, and the response phase runs them in
    /// reverse. A plugin can be in several chains, and all of them use its guests.
    ///
    /// # Errors
    ///
    /// An empty list, a name that is not in the runtime, or a name listed twice.
    pub fn chain(&self, names: &[&str]) -> Result<WasmChain> {
        if names.is_empty() {
            return Error::e_explain(ErrorType::InternalError, "a wasm chain needs a plugin");
        }
        let mut plugins = Vec::with_capacity(names.len());
        for name in names {
            let Some(index) = self.inner.names.get(*name) else {
                return Error::e_explain(
                    ErrorType::InternalError,
                    format!("wasm plugin {name} is not in the runtime"),
                );
            };
            if plugins.contains(index) {
                return Error::e_explain(
                    ErrorType::InternalError,
                    format!("wasm plugin {name} is in the chain twice"),
                );
            }
            plugins.push(*index);
        }
        Ok(WasmChain::new(self.inner.clone(), plugins))
    }

    /// Return the number of plugin contexts that are open.
    ///
    /// Each plugin of a chain opens one context for each request, and [WasmCtx::logging](crate::WasmCtx::logging) closes
    /// it. The number returns to zero when no request is in progress.
    pub fn open_contexts(&self) -> usize {
        self.inner.pools.iter().map(GuestPool::open_contexts).sum()
    }

    /// Return the number of plugin contexts that a guest keeps after its request ended.
    ///
    /// A guest keeps a context when its `proxy_on_done` returns `false`. The context stays until
    /// that guest is replaced.
    pub fn held_contexts(&self) -> usize {
        self.inner.pools.iter().map(GuestPool::held_contexts).sum()
    }

    /// Return the number of callouts that the runtime is sending.
    ///
    /// A callout counts until it receives its response or reaches its timeout, even when its
    /// request has already ended.
    pub fn callouts_in_flight(&self) -> usize {
        self.inner.callout_launcher.in_flight_count()
    }
}

impl fmt::Debug for WasmRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WasmRuntime")
            .field("plugins", &self.inner.plugin_names())
            .finish()
    }
}

impl RuntimeInner {
    /// Start the epoch ticker on the first call, as [Ticker::start] describes.
    pub(crate) fn start_ticker(self: &Arc<Self>) -> Result<()> {
        self.ticker.start(self)
    }

    pub(crate) fn plugin_names(&self) -> Vec<&str> {
        self.pools.iter().map(|pool| pool.name.as_str()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{fixture, plugin, wat_guest, Wat};
    use proxy_wasm_host::abi::v0_2_1::types::LogLevel;
    use proxy_wasm_host::abi::v0_2_1::{LogContext, LogSink};
    use proxy_wasm_host::Limits;

    #[test]
    fn new_with_services_refuses_a_limit_that_it_cannot_apply() {
        let cases = [0, usize::MAX];

        for limit in cases {
            let services = WasmServices {
                max_callouts_in_flight: limit,
                ..WasmServices::default()
            };
            let plugins = vec![plugin("a", fixture("add-request-header"), 1)];

            let err = WasmRuntime::new_with_services(plugins, services)
                .err()
                .unwrap();

            assert!(err.to_string().contains("max_callouts_in_flight"), "{err}");
        }
    }

    fn refusal(plugins: Vec<WasmPluginConf>) -> String {
        WasmRuntime::new(plugins).err().unwrap().to_string()
    }

    #[test]
    fn new_refuses_each_bad_input() {
        let text = std::env::temp_dir().join(format!("pingora-wasm-text-{}", std::process::id()));
        std::fs::write(&text, "not wasm").unwrap();
        let mut fuel = plugin("fuel", fixture("add-request-header"), 1);
        fuel.limits = Limits::default().with_fuel(10);
        let cases = [
            (vec![], "no wasm plugin to run"),
            (
                vec![
                    plugin("a", fixture("add-request-header"), 1),
                    plugin("a", fixture("add-request-header"), 1),
                ],
                "wasm plugin a is listed twice",
            ),
            (
                vec![plugin("zero", fixture("add-request-header"), 0)],
                "zero has zero slots",
            ),
            (vec![fuel], "fuel sets a fuel limit"),
            (
                vec![plugin("gone", "/no/such/file.wasm".into(), 1)],
                "gone cannot read",
            ),
            (vec![plugin("text", text, 1)], "text does not compile"),
            (
                vec![plugin(
                    "noabi",
                    wat_guest(
                        "noabi",
                        Wat {
                            abi: false,
                            ..Wat::default()
                        },
                    ),
                    1,
                )],
                "noabi is not a supported module",
            ),
            (
                vec![plugin(
                    "refused",
                    wat_guest(
                        "refused",
                        Wat {
                            vm_start: "i32.const 0",
                            ..Wat::default()
                        },
                    ),
                    1,
                )],
                "refused refused its start",
            ),
            (
                vec![plugin(
                    "trapped",
                    wat_guest(
                        "trapped",
                        Wat {
                            vm_start: "unreachable",
                            ..Wat::default()
                        },
                    ),
                    1,
                )],
                "trapped failed to start",
            ),
        ];

        for (plugins, message) in cases {
            let err = refusal(plugins);

            assert!(err.contains(message), "{err} lacks {message}");
        }
    }

    #[test]
    fn chain_refuses_bad_names() {
        let runtime =
            WasmRuntime::new(vec![plugin("a", fixture("add-request-header"), 1)]).unwrap();

        let errors: Vec<_> = [&[][..], &["missing"][..], &["a", "a"][..]]
            .iter()
            .map(|names| runtime.chain(names).err().unwrap().to_string())
            .collect();

        assert!(errors[0].contains("a wasm chain needs a plugin"));
        assert!(errors[1].contains("wasm plugin missing is not in the runtime"));
        assert!(errors[2].contains("wasm plugin a is in the chain twice"));
    }

    #[test]
    fn every_plugin_holds_the_same_store() {
        let runtime = WasmRuntime::new(vec![
            plugin("a", fixture("add-request-header"), 2),
            plugin("b", fixture("http-example"), 1),
        ])
        .unwrap();

        let stores: Vec<_> = runtime
            .inner
            .pools
            .iter()
            .flat_map(|pool| {
                (0..pool.slot_count()).map(move |slot| {
                    let guard = pool.lock_slot(slot);
                    guard.as_ref().unwrap().guest.services().shared().clone()
                })
            })
            .collect();

        assert_eq!(stores.len(), 3);
        assert!(stores.iter().all(|s| Arc::ptr_eq(s, &stores[0])));
    }

    #[derive(Default)]
    struct Recording(parking_lot::Mutex<Vec<String>>);

    impl LogSink for Recording {
        fn log(&self, _context: LogContext<'_>, _level: LogLevel, message: &[u8]) {
            self.0
                .lock()
                .push(String::from_utf8_lossy(message).into_owned());
        }
    }

    #[test]
    fn new_with_services_sends_guest_lines_to_the_sink() {
        let sink = Arc::new(Recording::default());

        let services = WasmServices {
            log_sink: sink.clone(),
            ..WasmServices::default()
        };

        WasmRuntime::new_with_services(vec![plugin("b", fixture("http-example"), 1)], services)
            .unwrap();

        let lines = sink.0.lock();
        assert!(
            lines.iter().any(|l| l.starts_with("registered queue")),
            "{lines:?}"
        );
    }

    #[test]
    fn the_public_types_are_send_and_sync() {
        fn send_sync<T: Send + Sync>() {}

        send_sync::<WasmRuntime>();
        send_sync::<WasmChain>();
        send_sync::<crate::WasmCtx>();
    }
}
