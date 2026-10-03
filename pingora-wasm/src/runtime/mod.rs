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

//! Wasm runtime

mod build;
use build::{build_pool, checked_plugin_indexes, new_shared_store, PoolInputs};
mod fail_policy;
mod lifecycle;
mod plugin;
pub(crate) mod pool;
mod services;
mod shared_store;
mod ticker;

pub use fail_policy::FailPolicy;
pub use plugin::WasmPluginConf;
pub use services::WasmServices;
pub(crate) use services::{CalloutLauncher, CalloutSenders};

#[cfg(test)]
use crate::callout::CalloutSender;
use crate::callout::ConnectorSender;
use crate::chain::WasmChain;
use crate::invalid_conf;
use crate::observability::WasmMetricSink;
use crate::properties::WasmProperties;
use crate::root_callbacks::RootCallbackThread;
use lifecycle::Lifecycle;
use pingora_core::connectors::http::Connector;
use pingora_error::{ErrorType, OrErr, Result};
use pool::GuestPool;
use proxy_wasm_host::abi::v0_2_1::Host;
use proxy_wasm_host::{Engine, EngineConfig};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use ticker::{with_ticker, Ticker};

/// The compiled plugins of a proxy and the guests that run them.
///
/// Build one before the server starts and clone it wherever you need it. All clones refer to
/// the same plugins and guests. The plugins of a runtime share one store for shared data,
/// queues, and metrics, in which plugins with the same VM id see the same data.
///
/// To reload plugins, build a new runtime and pass it to
/// [WasmPlugins::replace](crate::WasmPlugins::replace). Requests that started on the old runtime
/// finish on it.
///
/// Some plugins do work on a timer in `proxy_on_tick`, and some wait for items on a shared
/// queue. The runtime runs this work on a thread of its own, named `wasm-root-calls`, so it
/// never runs on the threads of your Pingora services. The thread starts when the
/// [WasmPlugins](crate::WasmPlugins) service starts, or with the first request if you do not use
/// one, and stops when the runtime ends or is dropped.
///
/// Each slot of a plugin is a separate guest, so each slot gets its own ticks. A tick waits
/// while a request callback runs in the same slot, and a request waits while a tick runs in its
/// slot. When a queue gets an item, the guest that registered the queue last receives
/// `proxy_on_queue_ready`. A plugin can send callouts from these callbacks to the upstreams in
/// [WasmServices::callout_upstreams].
#[derive(Clone)]
pub struct WasmRuntime {
    pub(crate) inner: Arc<RuntimeInner>,
}

pub(crate) struct RuntimeInner {
    engine: Engine,
    ticker: Ticker,
    pub(crate) lifecycle: Lifecycle,
    pub(crate) pools: Vec<GuestPool>,
    pub(crate) callout_launcher: CalloutLauncher,
    pub(crate) root_callback_thread: RootCallbackThread,
    pub(crate) fixed_properties: Arc<WasmProperties>,
    pub(crate) metric_sink: Arc<dyn WasmMetricSink>,
    names: HashMap<String, usize>,
    shutdown_wait_limit: Duration,
}

impl WasmRuntime {
    /// Compile the plugins and start their guests.
    ///
    /// Guest log lines are written to the `log` crate under the target `pingora_wasm::guest`,
    /// and plugins cannot send callouts. Use [WasmRuntime::new_with_services] to change either.
    ///
    /// Returns [ERR_INVALID_CONF](crate::ERR_INVALID_CONF) if `plugins` is empty, if two plugins
    /// have the same name, if a plugin's configuration is invalid, or if a plugin cannot be
    /// compiled or does not start.
    pub fn new(plugins: Vec<WasmPluginConf>) -> Result<Self> {
        Self::new_with_services(plugins, WasmServices::default())
    }

    /// Compile the plugins and start their guests, using your proxy's services.
    ///
    /// Use this when your plugins send callouts, define metrics, or read fixed properties, or
    /// when guest log lines should go to your own logger, e.g. to keep them in the request's
    /// `tracing` span. With [WasmServices::default] this is the same as [WasmRuntime::new].
    ///
    /// Returns the same errors as [WasmRuntime::new]. Also returns
    /// [ERR_INVALID_CONF](crate::ERR_INVALID_CONF) if
    /// [max_callouts_in_flight](WasmServices::max_callouts_in_flight) is out of range.
    pub fn new_with_services(plugins: Vec<WasmPluginConf>, services: WasmServices) -> Result<Self> {
        let connector = services
            .callout_connector
            .clone()
            .unwrap_or_else(|| Arc::new(Connector::new(None)));
        let request_sender = Arc::new(ConnectorSender::new(connector, &services));
        // Callouts sent from the root callback thread open their connections on that thread's
        // tokio runtime, which is shut down when this `WasmRuntime` is dropped. Give them a
        // connector of their own so that requests never reuse one of those connections.
        let root_callback_connector = Arc::new(Connector::new(None));
        let root_callback_sender =
            Arc::new(ConnectorSender::new(root_callback_connector, &services));
        Self::new_with_callout_senders(
            plugins,
            services,
            CalloutSenders {
                for_requests: request_sender,
                root_callback: root_callback_sender,
            },
        )
    }

    #[cfg(test)]
    pub(crate) fn new_with_callout_sender(
        plugins: Vec<WasmPluginConf>,
        services: WasmServices,
        sender: Arc<dyn CalloutSender>,
    ) -> Result<Self> {
        let senders = CalloutSenders {
            for_requests: sender.clone(),
            root_callback: sender,
        };
        Self::new_with_callout_senders(plugins, services, senders)
    }

    fn new_with_callout_senders(
        plugins: Vec<WasmPluginConf>,
        services: WasmServices,
        senders: CalloutSenders,
    ) -> Result<Self> {
        let callout_launcher = CalloutLauncher::new(
            senders,
            services.metric_sink.clone(),
            services.max_callouts_in_flight,
        )?;
        let names = checked_plugin_indexes(&plugins)?;
        let shutdown_wait_limit = services.shutdown_wait_limit;
        if shutdown_wait_limit.is_zero() {
            return Err(invalid_conf(
                "invalid shutdown_wait_limit 0s in wasm services, must be greater than zero",
            ));
        }
        if services.threads == 0 {
            return Err(invalid_conf(
                "invalid threads 0 in wasm services, must be at least 1",
            ));
        }
        let engine = EngineConfig::new()
            .with_external_ticks(true)
            .build()
            .or_err(ErrorType::InternalError, "failed to build the wasm engine")?;
        let host = Host::new(&engine).or_err(
            ErrorType::InternalError,
            "failed to link the Proxy-Wasm host functions",
        )?;
        let root_callback_thread = RootCallbackThread::new();
        let metric_sink = services.metric_sink;
        let shared_store = new_shared_store(&root_callback_thread, metric_sink.clone());
        let fixed_properties = Arc::new(services.fixed_properties);
        let inputs = PoolInputs {
            engine: &engine,
            host: &host,
            log_sink: services.log_sink,
            shared_store,
            upstreams: services.callout_upstreams,
            metric_sink: metric_sink.clone(),
            fixed_properties: fixed_properties.clone(),
            root_callback_thread: &root_callback_thread,
            threads: services.threads,
        };
        // Guests are started here, before the runtime's own ticker thread exists. The temporary
        // ticker keeps the CPU time limit in force for a guest that loops forever during startup.
        let pools = with_ticker(&engine, || {
            let indexed_plugins = plugins.iter().enumerate();
            indexed_plugins
                .map(|(pool_index, plugin)| build_pool(pool_index, plugin, &inputs))
                .collect::<Result<Vec<_>>>()
        })?;
        Ok(WasmRuntime {
            inner: Arc::new(RuntimeInner {
                engine,
                ticker: Ticker::new(),
                lifecycle: Lifecycle::default(),
                pools,
                callout_launcher,
                root_callback_thread,
                fixed_properties,
                metric_sink,
                names,
                shutdown_wait_limit,
            }),
        })
    }

    /// Build a chain from the plugins listed in `names`.
    ///
    /// Plugins run in the given order on the request and in reverse order on the response. A
    /// plugin may be part of several chains, which then share its guests.
    ///
    /// Returns [ERR_INVALID_CONF](crate::ERR_INVALID_CONF) if `names` is empty, if a name does
    /// not belong to a plugin of this runtime, or if a name is listed more than once.
    pub fn chain(&self, names: &[&str]) -> Result<WasmChain> {
        if names.is_empty() {
            return Err(invalid_conf("wasm chain needs at least one plugin"));
        }
        let mut plugins = Vec::with_capacity(names.len());
        for name in names {
            let Some(index) = self.inner.names.get(*name) else {
                return Err(invalid_conf(format!(
                    "wasm plugin {name}: not in the runtime"
                )));
            };
            if plugins.contains(index) {
                return Err(invalid_conf(format!(
                    "wasm plugin {name}: listed twice in the chain"
                )));
            }
            plugins.push(*index);
        }
        Ok(WasmChain::new(self.inner.clone(), plugins))
    }

    /// Return the number of open plugin contexts.
    ///
    /// Each plugin in a chain opens one context per request, which is closed by
    /// [WasmCtx::logging](crate::WasmCtx::logging). The count drops back to zero once no request
    /// is in progress.
    pub fn open_contexts(&self) -> usize {
        self.inner.pools.iter().map(GuestPool::open_contexts).sum()
    }

    /// Return the number of plugin contexts kept open after their requests ended.
    ///
    /// A plugin keeps a context by returning `false` from `proxy_on_done` and releases it later
    /// by calling `proxy_done`, e.g. once a callout response has arrived.
    pub fn held_contexts(&self) -> usize {
        self.inner.pools.iter().map(GuestPool::held_contexts).sum()
    }

    /// Return the number of callouts in flight.
    ///
    /// A callout is counted until its response has been received or it has timed out, even if
    /// the request that sent it has already ended.
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
    pub(crate) fn start_threads(self: &Arc<Self>) -> Result<()> {
        // The threads cannot be started in `WasmRuntime::new`. When daemonizing, Pingora forks
        // after the runtime has been built, and threads do not survive a fork. They are started by
        // the `WasmPlugins` service and on the request path instead, where only the first
        // successful call does any work.
        self.lifecycle.start_threads_once(|| {
            self.root_callback_thread.start(Arc::downgrade(self))?;
            self.ticker.start(self)
        })
    }

    pub(crate) fn plugin_names(&self) -> Vec<&str> {
        self.pools.iter().map(|pool| &*pool.name).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{fixture, plugin, wat_guest, Wat};
    use crate::ERR_INVALID_CONF;
    use proxy_wasm_host::abi::v0_2_1::types::LogLevel;
    use proxy_wasm_host::abi::v0_2_1::{LogContext, LogSink};
    use proxy_wasm_host::Limits;

    #[test]
    fn new_with_services_rejects_invalid_services() {
        let callouts = |limit| WasmServices {
            max_callouts_in_flight: limit,
            ..WasmServices::default()
        };
        let cases = [
            (callouts(0), "invalid max_callouts_in_flight 0 in wasm services, must be 1 to"),
            (
                callouts(usize::MAX),
                "invalid max_callouts_in_flight 18446744073709551615 in wasm services, must be 1 to",
            ),
            (
                WasmServices {
                    shutdown_wait_limit: Duration::ZERO,
                    ..WasmServices::default()
                },
                "invalid shutdown_wait_limit 0s in wasm services, must be greater than zero",
            ),
            (
                WasmServices {
                    threads: 0,
                    ..WasmServices::default()
                },
                "invalid threads 0 in wasm services, must be at least 1",
            ),
        ];

        for (services, message) in cases {
            let plugins = vec![plugin("a", fixture("add-request-header"), 1)];

            let err = WasmRuntime::new_with_services(plugins, services)
                .err()
                .unwrap();

            assert_eq!(err.etype(), &ERR_INVALID_CONF);
            assert!(err.to_string().contains(message), "{err}");
        }
    }

    #[test]
    fn slots_follow_services_threads_unless_set() {
        let cases = [(None, 3), (Some(2), 2)];

        for (slots, want) in cases {
            let mut conf = plugin("a", fixture("add-request-header"), 1);
            conf.slots = slots;
            let services = WasmServices {
                threads: 3,
                ..WasmServices::default()
            };

            let runtime = WasmRuntime::new_with_services(vec![conf], services).unwrap();

            assert_eq!(runtime.inner.pools[0].slot_count(), want, "{slots:?}");
        }
    }

    #[test]
    fn new_rejects_invalid_input() {
        let text = std::env::temp_dir().join(format!("pingora-wasm-text-{}", std::process::id()));
        std::fs::write(&text, "not wasm").unwrap();
        let mut fuel = plugin("fuel", fixture("add-request-header"), 1);
        fuel.limits = Limits::default().with_fuel(10);
        let refuses_configuration = Wat {
            configure: "i32.const 0",
            ..Wat::default()
        };
        let path = wat_guest("refused-open", refuses_configuration);
        let mut refused_under_open = plugin("refused-open", path, 1);
        refused_under_open.fail_policy = FailPolicy::Open;
        let cases = [
            (vec![], "wasm runtime needs at least one plugin"),
            (
                vec![
                    plugin("a", fixture("add-request-header"), 1),
                    plugin("a", fixture("add-request-header"), 1),
                ],
                "wasm plugin a: duplicate plugin name",
            ),
            (
                vec![plugin("zero", fixture("add-request-header"), 0)],
                "zero: slots must be at least 1",
            ),
            (vec![fuel], "fuel: fuel limits are not supported"),
            (
                vec![plugin("text", text, 1)],
                "failed to compile wasm plugin text",
            ),
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
                "noabi: not a supported Proxy-Wasm module",
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
                "refused: proxy_on_vm_start returned false, guest not started",
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
                "trapped: proxy_on_vm_start failed, guest not started",
            ),
            (
                vec![plugin(
                    "unconfigured",
                    wat_guest(
                        "unconfigured",
                        Wat {
                            configure: "unreachable",
                            ..Wat::default()
                        },
                    ),
                    1,
                )],
                "unconfigured: proxy_on_configure failed, guest not started",
            ),
            (
                vec![refused_under_open],
                "refused-open: proxy_on_configure returned false, guest not started",
            ),
        ];

        for (plugins, message) in cases {
            let err = WasmRuntime::new(plugins).err().unwrap();

            assert_eq!(err.etype(), &ERR_INVALID_CONF, "{message}");
            let err = err.to_string();
            assert!(err.contains(message), "{message} not found in {err}");
        }
    }

    #[test]
    fn new_returns_read_error_for_unreadable_plugin_file() {
        let plugins = vec![plugin("gone", "/no/such/file.wasm".into(), 1)];

        let err = WasmRuntime::new(plugins).err().unwrap();

        assert_eq!(err.etype(), &ErrorType::ReadError);
        let message = "failed to read wasm plugin gone from /no/such/file.wasm";
        assert!(err.to_string().contains(message), "{err}");
    }

    #[test]
    fn chain_rejects_invalid_names() {
        let runtime =
            WasmRuntime::new(vec![plugin("a", fixture("add-request-header"), 1)]).unwrap();

        let errors: Vec<_> = [&[][..], &["missing"][..], &["a", "a"][..]]
            .iter()
            .map(|names| runtime.chain(names).err().unwrap())
            .collect();

        assert!(errors.iter().all(|e| e.etype() == &ERR_INVALID_CONF));
        let errors: Vec<_> = errors.iter().map(ToString::to_string).collect();
        assert!(errors[0].contains("wasm chain needs at least one plugin"));
        assert!(errors[1].contains("wasm plugin missing: not in the runtime"));
        assert!(errors[2].contains("wasm plugin a: listed twice in the chain"));
    }

    #[test]
    fn all_guests_share_one_store() {
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
    fn guest_log_lines_go_to_configured_sink() {
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
    fn public_types_are_send_and_sync() {
        fn send_sync<T: Send + Sync>() {}

        send_sync::<WasmRuntime>();
        send_sync::<WasmChain>();
        send_sync::<crate::WasmCtx>();
    }
}
