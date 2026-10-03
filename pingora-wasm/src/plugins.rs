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
use log::error;
use pingora_core::server::ShutdownWatch;
use pingora_core::services::background::BackgroundService;
use pingora_error::{Error, ErrorType, Result};
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::runtime::Handle;

/// The current runtime of a server and its chains by name.
///
/// Add it to your server with `background_service`, and create each [WasmCtx] through a
/// [WasmChainHandle] from [WasmPlugins::chain]. As a background service it starts the plugins
/// after Pingora has forked, so that plugins get their ticks before the first request, and it
/// ends the plugins at a graceful shutdown. See
/// [shutdown_wait_limit](crate::WasmServices::shutdown_wait_limit).
///
/// To reload plugins, build a new runtime and pass it with its chains to
/// [WasmPlugins::replace]. The new runtime starts with empty shared data and queues.
///
/// ```no_run
/// # use pingora_wasm::{WasmPluginConf, WasmPlugins, WasmRuntime};
/// # use pingora_core::services::background::background_service;
/// # fn main() -> pingora_error::Result<()> {
/// let runtime = WasmRuntime::new(vec![WasmPluginConf::new("auth", "auth.wasm")])?;
/// let api = runtime.chain(&["auth"])?;
/// let plugins = background_service("wasm plugins", WasmPlugins::new(runtime, [("api", api)])?);
/// let api = plugins.task().chain("api")?;
/// // Keep `api` in your proxy, call `api.new_ctx()` from its `new_ctx`, and add `plugins` to
/// // the server.
/// # Ok(())
/// # }
/// ```
pub struct WasmPlugins {
    current: Arc<ArcSwap<CurrentPlugins>>,
    names: HashMap<String, usize>,
    service_started: AtomicBool,
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
    /// Create the plugins of a server from a runtime and its chains by name.
    ///
    /// Returns [ERR_INVALID_CONF](crate::ERR_INVALID_CONF) if `chains` is empty, if a name is
    /// given twice, or if a chain was built from a different runtime.
    pub fn new<N: Into<String>>(
        runtime: WasmRuntime,
        chains: impl IntoIterator<Item = (N, WasmChain)>,
    ) -> Result<Self> {
        let mut names = HashMap::new();
        let mut ordered = Vec::new();
        for (name, chain) in chains {
            let name = name.into();
            check_runtime(&runtime, &name, &chain)?;
            if names.insert(name.clone(), ordered.len()).is_some() {
                return Err(invalid_conf(format!("wasm chain {name}: given twice")));
            }
            ordered.push(chain);
        }
        if ordered.is_empty() {
            return Err(invalid_conf("wasm plugins need at least one chain"));
        }
        let current = CurrentPlugins {
            runtime,
            chains: ordered,
        };
        Ok(WasmPlugins {
            current: Arc::new(ArcSwap::from_pointee(current)),
            names,
            service_started: AtomicBool::new(false),
        })
    }

    /// Return the handle of the chain `name`.
    ///
    /// Returns [ERR_INVALID_CONF](crate::ERR_INVALID_CONF) if there is no chain with that name.
    pub fn chain(&self, name: &str) -> Result<WasmChainHandle> {
        let Some(index) = self.names.get(name) else {
            return Err(invalid_conf(format!(
                "wasm chain {name}: not in the plugins"
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
    /// Pass one chain for each name given to [WasmPlugins::new], each built from `runtime`. New
    /// requests use the new runtime once this returns. The old runtime ends in the background
    /// after its requests have finished, as it would at a shutdown. Call this from inside a tokio
    /// runtime, such as a Pingora service.
    ///
    /// If this returns an error, the old runtime stays in use. Returns
    /// [ERR_INVALID_CONF](crate::ERR_INVALID_CONF) if the names are not the same, or for any
    /// reason [WasmPlugins::new] does.
    pub fn replace<N: Into<String>>(
        &self,
        runtime: WasmRuntime,
        chains: impl IntoIterator<Item = (N, WasmChain)>,
    ) -> Result<()> {
        let mut ordered: Vec<Option<WasmChain>> = vec![None; self.names.len()];
        for (name, chain) in chains {
            let name = name.into();
            check_runtime(&runtime, &name, &chain)?;
            let Some(index) = self.names.get(&name) else {
                return Err(invalid_conf(format!(
                    "wasm chain {name}: not in the plugins, a replace cannot add a chain"
                )));
            };
            if ordered[*index].replace(chain).is_some() {
                return Err(invalid_conf(format!("wasm chain {name}: given twice")));
            }
        }
        let mut chains = Vec::with_capacity(ordered.len());
        for (name, index) in &self.names {
            match ordered[*index].take() {
                Some(chain) => chains.push((*index, chain)),
                None => {
                    return Err(invalid_conf(format!(
                        "wasm chain {name}: missing, a replace cannot remove a chain"
                    )))
                }
            }
        }
        chains.sort_by_key(|(index, _)| *index);
        let tokio_runtime = Handle::try_current().map_err(|_| {
            Error::explain(
                ErrorType::InternalError,
                "wasm plugins can only be replaced from inside a tokio runtime",
            )
        })?;
        if self.service_started.load(Ordering::SeqCst) {
            runtime.inner.start_threads()?;
        }
        let new = CurrentPlugins {
            runtime,
            chains: chains.into_iter().map(|(_, chain)| chain).collect(),
        };
        let old = self.current.swap(Arc::new(new));
        tokio_runtime.spawn(async move { old.runtime.inner.end().await });
        Ok(())
    }
}

#[async_trait]
impl BackgroundService for WasmPlugins {
    async fn start(&self, mut shutdown: ShutdownWatch) {
        self.service_started.store(true, Ordering::SeqCst);
        if let Err(e) = self.current.load().runtime.inner.start_threads() {
            error!("wasm plugins: failed to start: {e}");
        }
        let _ = shutdown.changed().await;
        let current = self.current.load_full();
        current.runtime.inner.end().await;
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

fn check_runtime(runtime: &WasmRuntime, name: &str, chain: &WasmChain) -> Result<()> {
    if Arc::ptr_eq(&runtime.inner, &chain.runtime) {
        return Ok(());
    }
    Err(invalid_conf(format!(
        "wasm chain {name}: built from a different runtime"
    )))
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

    fn plugins_with_api_chain() -> (WasmPlugins, WasmRuntime, Arc<RecordedGuestLogs>) {
        let (runtime, logs) = runtime(&["a"]);
        let api = runtime.chain(&["a"]).unwrap();
        let plugins = WasmPlugins::new(runtime.clone(), [("api", api)]).unwrap();
        (plugins, runtime, logs)
    }

    #[tokio::test]
    async fn new_ctx_while_replaced_runtime_ends_uses_new_runtime() {
        let (plugins, old, old_logs) = plugins_with_api_chain();
        let api = plugins.chain("api").unwrap();
        let mut old_ctx = api.new_ctx();
        let (mut session, _client) = session(GET).await;
        old_ctx.request_filter(&mut session).await.unwrap();
        let (new, _new_logs) = runtime(&["a"]);
        let new_api = new.chain(&["a"]).unwrap();
        plugins.replace(new.clone(), [("api", new_api)]).unwrap();
        assert!(eventually(|| old.inner.lifecycle.is_ending()).await);

        let ctx = api.new_ctx();

        assert!(Arc::ptr_eq(&ctx.chain.runtime, &new.inner));
        assert!(Arc::ptr_eq(&plugins.runtime().inner, &new.inner));
        assert!(old_logs.0.lock().is_empty());
        drop(old_ctx);
        let old_ended = || old_logs.0.lock().iter().any(|line| line == "root done");
        assert!(eventually(old_ended).await);
    }

    #[tokio::test]
    async fn replace_with_other_chain_names_fails_and_keeps_old_runtime() {
        let cases: [(&str, &[&str], &str); 2] = [
            ("missing", &[], "wasm chain api: missing"),
            (
                "added",
                &["api", "other"],
                "wasm chain other: not in the plugins",
            ),
        ];
        for (name, chain_names, message) in cases {
            let (plugins, old, _logs) = plugins_with_api_chain();
            let (new, _new_logs) = runtime(&["a"]);
            let chains = chain_names
                .iter()
                .map(|chain_name| (*chain_name, new.chain(&["a"]).unwrap()));

            let result = plugins.replace(new.clone(), chains);

            let err = result.unwrap_err();
            assert_eq!(err.etype(), &ERR_INVALID_CONF, "{name}");
            assert!(err.to_string().contains(message), "{name}: {err}");
            assert!(Arc::ptr_eq(&plugins.runtime().inner, &old.inner), "{name}");
            assert!(!new.inner.lifecycle.is_ending(), "{name}");
        }
    }
}
