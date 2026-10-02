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
//!
//! A [WasmRuntime] holds the compiled plugins of a proxy and their guest pools. It is built once
//! and shared by every request.

mod build;
use build::{build_pool, checked_plugin_indexes, new_shared_store, PoolInputs};
mod fail_policy;
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
use once_cell::sync::OnceCell;
use pingora_core::connectors::http::Connector;
use pingora_error::{ErrorType, OrErr, Result};
use pool::GuestPool;
use proxy_wasm_host::abi::v0_2_1::Host;
use proxy_wasm_host::{Engine, EngineConfig};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use ticker::{with_ticker, Ticker};

/// The compiled plugins of a proxy and the guests that run them.
///
/// Build one before the server starts and clone it wherever you need it. All clones refer to
/// the same plugins and guests. The plugins of a runtime share one store for shared data,
/// queues, and metrics, in which plugins with the same VM id see the same data.
///
/// To reload plugins, build a new runtime and switch new requests over to it. Requests that
/// started on the old runtime will finish on it.
#[derive(Clone)]
pub struct WasmRuntime {
    pub(crate) inner: Arc<RuntimeInner>,
}

pub(crate) struct RuntimeInner {
    engine: Engine,
    ticker: Ticker,
    threads_started: OnceCell<()>,
    pub(crate) pools: Vec<GuestPool>,
    pub(crate) callout_launcher: CalloutLauncher,
    pub(crate) root_callback_thread: RootCallbackThread,
    pub(crate) fixed_properties: Arc<WasmProperties>,
    pub(crate) metric_sink: Arc<dyn WasmMetricSink>,
    names: HashMap<String, usize>,
}

impl WasmRuntime {
    /// Compile the plugins and start their guests.
    ///
    /// Guest log lines are written to the `log` crate under the target `pingora_wasm::guest`,
    /// and plugins cannot send callouts. Use [WasmRuntime::new_with_services] to change either.
    ///
    /// # Errors
    ///
    /// Returns [ERR_INVALID_CONF](crate::ERR_INVALID_CONF) if `plugins` is empty or two plugins
    /// have the same name. The same error, with the plugin's name in its message, is returned
    /// if a plugin's file is not a supported Proxy-Wasm module, if the plugin
    /// traps or otherwise fails during startup, if its `proxy_on_vm_start` or
    /// `proxy_on_configure` returns `false`, or if its configuration is invalid. A configuration
    /// is invalid when [slots](WasmPluginConf::slots) is zero, when
    /// [limits](WasmPluginConf::limits) sets a fuel limit, when one of the body or callout limits
    /// is zero, or when [callout_wait_limit](WasmPluginConf::callout_wait_limit) is not greater
    /// than [callout_timeout_limit](WasmPluginConf::callout_timeout_limit). A plugin's
    /// [fail_policy](WasmPluginConf::fail_policy) has no effect on these errors.
    ///
    /// Returns `ReadError` if a plugin's file cannot be read, and `InternalError` if the wasm
    /// engine cannot be built.
    pub fn new(plugins: Vec<WasmPluginConf>) -> Result<Self> {
        Self::new_with_services(plugins, WasmServices::default())
    }

    /// Compile the plugins and start their guests, using your proxy's services.
    ///
    /// Use this when your plugins send callouts, define metrics, or read fixed properties, or
    /// when guest log lines should go to your own logger, e.g. to keep them in the request's
    /// `tracing` span. With [WasmServices::default] this is the same as [WasmRuntime::new].
    ///
    /// # Errors
    ///
    /// Returns the same errors as [WasmRuntime::new]. Also returns
    /// [ERR_INVALID_CONF](crate::ERR_INVALID_CONF) if
    /// [max_callouts_in_flight](WasmServices::max_callouts_in_flight) is zero or greater than
    /// `tokio::sync::Semaphore::MAX_PERMITS`.
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
        };
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
                threads_started: OnceCell::new(),
                pools,
                callout_launcher,
                root_callback_thread,
                fixed_properties,
                metric_sink,
                names,
            }),
        })
    }

    /// Build a chain from the plugins listed in `names`.
    ///
    /// Plugins run in the given order on the request and in reverse order on the response. A
    /// plugin may be part of several chains, which then share its guests.
    ///
    /// # Errors
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
    /// Start the root callback thread and the epoch ticker if they are not running yet.
    ///
    /// The threads cannot be started in [WasmRuntime::new]. When daemonizing, Pingora forks after
    /// the runtime has been built, and threads do not survive a fork. This is called on the
    /// request path instead, where only the first successful call does any work.
    ///
    /// # Errors
    ///
    /// Returns [ERR_PLUGIN_FAILED](crate::ERR_PLUGIN_FAILED) if a thread cannot be started. The
    /// next call will try again.
    pub(crate) fn start_threads(self: &Arc<Self>) -> Result<()> {
        self.threads_started.get_or_try_init(|| {
            self.root_callback_thread.start(Arc::downgrade(self))?;
            self.ticker.start(self)
        })?;
        Ok(())
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
    fn new_with_services_rejects_invalid_callout_limit() {
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

            assert_eq!(err.etype(), &ERR_INVALID_CONF);
            let message =
                format!("invalid max_callouts_in_flight {limit} in wasm services, must be 1 to");
            assert!(err.to_string().contains(&message), "{err}");
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
