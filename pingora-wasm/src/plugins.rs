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

//! The plugins of a server

use crate::chain::{WasmChain, WasmCtx};
use crate::invalid_conf;
use crate::runtime::WasmRuntime;
use arc_swap::ArcSwap;
use async_trait::async_trait;
use futures::stream::{FuturesUnordered, StreamExt};
use log::error;
use parking_lot::Mutex;
use pingora_core::server::ShutdownWatch;
use pingora_core::services::background::BackgroundService;
use pingora_error::Result;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

/// The current runtime of a server and its chains by name.
///
/// Add it to your server with `background_service`, and create each [WasmCtx] through a
/// [WasmChainHandle] from [WasmPlugins::chain]. As a background service it starts the plugins
/// after Pingora has forked, so that plugins get their ticks before the first request, and it
/// ends the plugins at a graceful shutdown. See
/// [shutdown_wait_limit](crate::WasmServices::shutdown_wait_limit).
///
/// To reload plugins, build a new runtime and pass it with the plugin names of each chain to
/// [WasmPlugins::replace]. The new runtime starts with empty shared data and queues.
///
/// ```no_run
/// # use pingora_wasm::{WasmPluginConf, WasmPlugins, WasmRuntime};
/// # use pingora_core::services::background::background_service;
/// # fn main() -> pingora_error::Result<()> {
/// let runtime = WasmRuntime::new(vec![WasmPluginConf::new("auth", "auth.wasm")])?;
/// let plugins = WasmPlugins::new(runtime, [("api", ["auth"])])?;
/// let plugins = background_service("wasm plugins", plugins);
/// let api = plugins.task().chain("api")?;
/// // Keep `api` in your proxy, call `api.new_ctx()` from its `new_ctx`, and add `plugins` to
/// // the server.
/// # Ok(())
/// # }
/// ```
pub struct WasmPlugins {
    current: Arc<ArcSwap<CurrentPlugins>>,
    names: HashMap<String, usize>,
    service: Mutex<ServiceState>,
    replaced: UnboundedSender<WasmRuntime>,
    to_end: Mutex<Option<UnboundedReceiver<WasmRuntime>>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ServiceState {
    NotStarted,
    Running,
    ShuttingDown,
}

struct CurrentPlugins {
    runtime: WasmRuntime,
    /// In the order of the indexes in `WasmPlugins::names`.
    chains: Vec<WasmChain>,
}

/// A chain of [WasmPlugins] that always uses the current runtime.
#[derive(Clone)]
pub struct WasmChainHandle {
    current: Arc<ArcSwap<CurrentPlugins>>,
    index: usize,
    name: Arc<str>,
}

impl WasmPlugins {
    /// Create the plugins of a server from a runtime and the plugin names of each chain.
    ///
    /// Each chain is built with [WasmRuntime::chain]. `chains` can be a list such as
    /// `[("api", ["auth", "stats"])]`, or [WasmConf::chains](crate::WasmConf::chains).
    ///
    /// Returns [ERR_INVALID_CONF](crate::ERR_INVALID_CONF) if `chains` is empty, if a chain name
    /// is listed twice, or for any reason [WasmRuntime::chain] does.
    pub fn new<N, P, S>(
        runtime: WasmRuntime,
        chains: impl IntoIterator<Item = (N, P)>,
    ) -> Result<Self>
    where
        N: Into<String>,
        P: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        if runtime.inner.lifecycle.is_ending() {
            return Err(invalid_conf("wasm runtime: already ended"));
        }
        let mut names = HashMap::new();
        let mut ordered = Vec::new();
        for (name, plugins) in chains {
            let name = name.into();
            if names.insert(name.clone(), ordered.len()).is_some() {
                return Err(invalid_conf(format!("wasm chain {name}: listed twice")));
            }
            ordered.push(build_chain(&runtime, plugins)?);
        }
        if ordered.is_empty() {
            return Err(invalid_conf("wasm plugins need at least one chain"));
        }
        let current = CurrentPlugins {
            runtime,
            chains: ordered,
        };
        let (replaced, to_end) = unbounded_channel();
        Ok(WasmPlugins {
            current: Arc::new(ArcSwap::from_pointee(current)),
            names,
            service: Mutex::new(ServiceState::NotStarted),
            replaced,
            to_end: Mutex::new(Some(to_end)),
        })
    }

    /// Return the handle of the chain `name`.
    ///
    /// Returns [ERR_INVALID_CONF](crate::ERR_INVALID_CONF) if there is no chain with that name.
    pub fn chain(&self, name: &str) -> Result<WasmChainHandle> {
        let Some(index) = self.names.get(name) else {
            return Err(invalid_conf(format!(
                "wasm chain {name}: not given to WasmPlugins"
            )));
        };
        Ok(WasmChainHandle {
            current: self.current.clone(),
            index: *index,
            name: name.into(),
        })
    }

    /// Return the current runtime.
    pub fn runtime(&self) -> WasmRuntime {
        self.current.load().runtime.clone()
    }

    /// Replace the runtime and its chains, e.g. to reload plugins.
    ///
    /// Pass the plugin names of each chain given to [WasmPlugins::new], in the same form. New
    /// requests use the new runtime once this returns. The service ends the old runtime after its
    /// requests have finished and its TCP connections have drained, as it would at a shutdown.
    ///
    /// If this returns an error, the old runtime stays in use. Returns
    /// [ERR_INVALID_CONF](crate::ERR_INVALID_CONF) if the chain names are not the same, if
    /// `runtime` is the current runtime or has ended, if the server is shutting down, or for
    /// any reason [WasmRuntime::chain] does. Returns
    /// [ERR_PLUGIN_FAILED](crate::ERR_PLUGIN_FAILED) if the threads of `runtime` cannot start.
    pub fn replace<N, P, S>(
        &self,
        runtime: WasmRuntime,
        chains: impl IntoIterator<Item = (N, P)>,
    ) -> Result<()>
    where
        N: Into<String>,
        P: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        if Arc::ptr_eq(&runtime.inner, &self.current.load().runtime.inner) {
            return Err(invalid_conf("wasm runtime: already in use by WasmPlugins"));
        }
        if runtime.inner.lifecycle.is_ending() {
            return Err(invalid_conf("wasm runtime: already ended"));
        }
        let mut ordered: Vec<Option<WasmChain>> = vec![None; self.names.len()];
        for (name, plugins) in chains {
            let name = name.into();
            let Some(index) = self.names.get(&name) else {
                return Err(invalid_conf(format!(
                    "wasm chain {name}: not given to WasmPlugins, a replace cannot add a chain"
                )));
            };
            if ordered[*index].is_some() {
                return Err(invalid_conf(format!("wasm chain {name}: listed twice")));
            }
            ordered[*index] = Some(build_chain(&runtime, plugins)?);
        }
        let mut chains = Vec::with_capacity(ordered.len());
        for (name, index) in &self.names {
            let Some(chain) = ordered[*index].take() else {
                return Err(invalid_conf(format!(
                    "wasm chain {name}: missing, a replace cannot remove a chain"
                )));
            };
            chains.push((*index, chain));
        }
        chains.sort_by_key(|(index, _)| *index);
        let service = self.service.lock();
        match *service {
            ServiceState::ShuttingDown => {
                return Err(invalid_conf(
                    "wasm plugins cannot be replaced while the server shuts down",
                ))
            }
            ServiceState::Running => runtime.inner.start_threads()?,
            ServiceState::NotStarted => {}
        }
        let new = CurrentPlugins {
            runtime,
            chains: chains.into_iter().map(|(_, chain)| chain).collect(),
        };
        let old = self.current.swap(Arc::new(new));
        // The receiver lives as long as `self`, so the send cannot fail
        let _ = self.replaced.send(old.runtime.clone());
        Ok(())
    }

    fn set_service_state(&self, state: ServiceState) -> Arc<CurrentPlugins> {
        let mut service = self.service.lock();
        *service = state;
        self.current.load_full()
    }
}

#[async_trait]
impl BackgroundService for WasmPlugins {
    async fn start(&self, mut shutdown: ShutdownWatch) {
        let current = self.set_service_state(ServiceState::Running);
        if let Err(e) = current.runtime.inner.start_threads() {
            error!("failed to start the wasm plugins: {e}");
        }
        drop(current);
        let Some(mut to_end) = self.to_end.lock().take() else {
            return;
        };
        let mut ending = FuturesUnordered::new();
        loop {
            tokio::select! {
                Some(old) = to_end.recv() => {
                    ending.push(end_runtime(old));
                }
                Some(()) = ending.next() => {}
                _ = shutdown.changed() => break,
            }
        }
        let current = self.set_service_state(ServiceState::ShuttingDown);
        while let Ok(old) = to_end.try_recv() {
            ending.push(end_runtime(old));
        }
        let current = current.runtime.clone();
        futures::join!(end_runtime(current), ending.collect::<Vec<()>>());
    }
}

impl WasmChainHandle {
    /// Create the per-request state for this chain on the current runtime.
    ///
    /// Call this from your proxy's `new_ctx`. See [WasmChain::new_ctx].
    pub fn new_ctx(&self) -> WasmCtx {
        loop {
            let current = self.current.load_full();
            let ctx = current.chains[self.index].new_ctx();
            if !current.runtime.inner.lifecycle.is_ending() {
                return ctx;
            }
            // At a shutdown no newer runtime has replaced this one, so the request runs on it
            if Arc::ptr_eq(&current, &self.current.load()) {
                return ctx;
            }
        }
    }
}

async fn end_runtime(runtime: WasmRuntime) {
    runtime.inner.end().await;
}

fn build_chain<P, S>(runtime: &WasmRuntime, plugins: P) -> Result<WasmChain>
where
    P: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let plugins: Vec<S> = plugins.into_iter().collect();
    let names: Vec<&str> = plugins.iter().map(AsRef::as_ref).collect();
    runtime.chain(&names)
}

impl fmt::Debug for WasmPlugins {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut chains: Vec<_> = self.names.keys().collect();
        chains.sort();
        f.debug_struct("WasmPlugins")
            .field("runtime", &self.current.load().runtime)
            .field("chains", &chains)
            .finish()
    }
}

impl fmt::Debug for WasmChainHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WasmChainHandle")
            .field("name", &self.name)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        eventually, plugin, session, wat_guest, RecordedGuestLogs, Wat, GET,
    };
    use crate::{WasmServices, ERR_INVALID_CONF};
    use tokio::sync::watch;
    use tokio::task::JoinHandle;

    type Chains<'a> = &'a [(&'a str, &'a [&'a str])];

    fn logs_root_done() -> Wat {
        Wat {
            data_segments: r#"(data (i32.const 700) "root done")"#,
            configure: "(i32.store (i32.const 600) (local.get 0)) i32.const 1",
            done: "(if (i32.eq (local.get 0) (i32.load (i32.const 600)))
            (then (drop (call $log (i32.const 2) (i32.const 700) (i32.const 9)))))
            i32.const 1",
            ..Wat::default()
        }
    }

    fn runtime(names: &[&str]) -> (WasmRuntime, Arc<RecordedGuestLogs>) {
        let logs = Arc::new(RecordedGuestLogs::default());
        let services = WasmServices {
            log_sink: logs.clone(),
            ..WasmServices::default()
        };
        let plugins = names
            .iter()
            .map(|name| plugin(name, wat_guest(name, logs_root_done()), 1))
            .collect();
        let runtime = WasmRuntime::new_with_services(plugins, services).unwrap();
        (runtime, logs)
    }

    fn plugins_with_api_chain() -> (Arc<WasmPlugins>, WasmRuntime, Arc<RecordedGuestLogs>) {
        let (runtime, logs) = runtime(&["a"]);
        let plugins = WasmPlugins::new(runtime.clone(), [("api", ["a"])]).unwrap();
        (Arc::new(plugins), runtime, logs)
    }

    /// Run the background service of `plugins` until the returned sender signals a shutdown.
    fn run_service(plugins: &Arc<WasmPlugins>) -> (watch::Sender<bool>, JoinHandle<()>) {
        let (shutdown, watch) = watch::channel(false);
        let plugins = plugins.clone();
        let service = tokio::spawn(async move { plugins.start(watch).await });
        (shutdown, service)
    }

    fn has_root_done(logs: &RecordedGuestLogs) -> bool {
        logs.0.lock().iter().any(|line| line == "root done")
    }

    #[tokio::test]
    async fn new_ctx_while_replaced_runtime_ends_uses_new_runtime() {
        let (plugins, old, old_logs) = plugins_with_api_chain();
        let _service = run_service(&plugins);
        let api = plugins.chain("api").unwrap();
        let mut old_ctx = api.new_ctx();
        let (mut session, _client) = session(GET).await;
        old_ctx.request_filter(&mut session).await.unwrap();
        let (new, _new_logs) = runtime(&["a"]);
        plugins.replace(new.clone(), [("api", ["a"])]).unwrap();
        assert!(eventually(|| old.inner.lifecycle.is_ending()).await);

        let ctx = api.new_ctx();

        assert!(Arc::ptr_eq(&ctx.chain.runtime, &new.inner));
        assert!(Arc::ptr_eq(&plugins.runtime().inner, &new.inner));
        assert!(!has_root_done(&old_logs));
        drop(old_ctx);
        assert!(eventually(|| has_root_done(&old_logs)).await);
    }

    #[test]
    fn replace_without_tokio_runtime_leaves_end_to_service() {
        let (plugins, old, old_logs) = plugins_with_api_chain();
        let tokio_runtime = tokio::runtime::Runtime::new().unwrap();
        let _service = tokio_runtime.block_on(async { run_service(&plugins) });
        old.inner.start_threads().unwrap();
        let (new, _new_logs) = runtime(&["a"]);

        let replaced = std::thread::scope(|scope| {
            scope
                .spawn(|| plugins.replace(new, [("api", ["a"])]))
                .join()
                .unwrap()
        });

        assert!(replaced.is_ok());
        assert!(tokio_runtime.block_on(eventually(|| has_root_done(&old_logs))));
    }

    #[tokio::test]
    async fn replace_refuses_other_chains_ended_runtime_and_shutdown() {
        let (ended, _) = runtime(&["a"]);
        ended.inner.end().await;
        let cases: [(&str, Option<&WasmRuntime>, Chains, &str); 5] = [
            ("missing", None, &[], "wasm chain api: missing"),
            (
                "added",
                None,
                &[("api", &["a"]), ("other", &["a"])],
                "wasm chain other: not given to WasmPlugins",
            ),
            (
                "twice",
                None,
                &[("api", &["a"]), ("api", &["a"])],
                "wasm chain api: listed twice",
            ),
            (
                "ended",
                Some(&ended),
                &[("api", &["a"])],
                "wasm runtime: already ended",
            ),
            (
                "shutdown",
                None,
                &[("api", &["a"])],
                "while the server shuts down",
            ),
        ];

        for (name, runtime_given, chains, message) in cases {
            let (plugins, old, _logs) = plugins_with_api_chain();
            if name == "shutdown" {
                let (shutdown, service) = run_service(&plugins);
                shutdown.send(true).unwrap();
                service.await.unwrap();
            }
            let (fresh, _fresh_logs) = runtime(&["a"]);
            let runtime_given = runtime_given.cloned().unwrap_or(fresh.clone());
            let chains = chains
                .iter()
                .map(|(chain, plugins)| (*chain, plugins.iter()));

            let err = plugins.replace(runtime_given, chains).unwrap_err();

            assert_eq!(err.etype(), &ERR_INVALID_CONF, "{name}");
            assert!(err.to_string().contains(message), "{name}: {err}");
            assert!(Arc::ptr_eq(&plugins.runtime().inner, &old.inner), "{name}");
            assert!(!fresh.inner.lifecycle.is_ending(), "{name}");
        }
    }

    #[test]
    fn replace_refuses_current_runtime() {
        let (plugins, old, _logs) = plugins_with_api_chain();

        let err = plugins.replace(old.clone(), [("api", ["a"])]).unwrap_err();

        assert_eq!(err.etype(), &ERR_INVALID_CONF);
        assert!(err
            .to_string()
            .contains("wasm runtime: already in use by WasmPlugins"));
        assert!(!old.inner.lifecycle.is_ending());
    }

    #[tokio::test]
    async fn new_and_chain_refuse_invalid_names() {
        let (ended, _) = runtime(&["a"]);
        ended.inner.end().await;
        let new = |runtime: &WasmRuntime, chains: Chains| {
            let chains = chains
                .iter()
                .map(|(chain, plugins)| (*chain, plugins.iter()));
            WasmPlugins::new(runtime.clone(), chains).map(|_| ())
        };
        let (fresh, _logs) = runtime(&["a"]);
        let (plugins, _, _) = plugins_with_api_chain();
        let cases = [
            (
                "empty",
                new(&fresh, &[]),
                "wasm plugins need at least one chain",
            ),
            (
                "twice",
                new(&fresh, &[("api", &["a"]), ("api", &["a"])]),
                "wasm chain api: listed twice",
            ),
            (
                "unknown plugin",
                new(&fresh, &[("api", &["b"])]),
                "wasm plugin b: not in the runtime",
            ),
            (
                "ended",
                new(&ended, &[("api", &["a"])]),
                "wasm runtime: already ended",
            ),
            (
                "unknown chain",
                plugins.chain("other").map(|_| ()),
                "wasm chain other: not given to WasmPlugins",
            ),
        ];

        for (name, result, message) in cases {
            let err = result.unwrap_err();

            assert_eq!(err.etype(), &ERR_INVALID_CONF, "{name}");
            assert!(err.to_string().contains(message), "{name}: {err}");
        }
    }
}
