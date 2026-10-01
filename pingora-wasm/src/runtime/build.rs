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

//! The steps that build a runtime, which are the check of the plugin names, the shared store,
//! and the pool of each plugin.

use super::plugin::WasmPluginConf;
use super::pool::events::RootCallbackEvent;
use super::pool::{GuestPool, GuestPoolConf};
use super::shared_store::SharedStore;
use crate::callout::CalloutUpstreams;
use crate::metrics::WasmMetricSink;
use crate::properties::WasmProperties;
use crate::root_callbacks::RootCallbackThread;
use crate::stream::RootCallbackPluginState;
use pingora_error::{Error, ErrorType, OrErr, Result};
use proxy_wasm_host::abi::v0_2_1::{
    GuestSpec, Host, InMemoryStoreLimits, LogSink, QueueEnqueued, SharedServices,
};
use proxy_wasm_host::{Engine, Module};
use std::collections::HashMap;
use std::sync::Arc;

/// Check each plugin, and return a map from the name of each plugin to its index.
pub(super) fn checked_plugin_indexes(plugins: &[WasmPluginConf]) -> Result<HashMap<String, usize>> {
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
    Ok(names)
}

/// Create the store of shared data, queues, and metrics.
///
/// The store sends an event to the root callback thread for each item that a plugin enqueues.
pub(super) fn new_shared_store(
    root_callback_thread: &RootCallbackThread,
    metric_sink: Arc<dyn WasmMetricSink>,
) -> Arc<dyn SharedServices> {
    let root_callback_sender = root_callback_thread.sender();
    let enqueue_observer = Arc::new(move |item: QueueEnqueued<'_>| {
        let _ = root_callback_sender.send(RootCallbackEvent::QueueItem(item.queue));
    });
    let limits = InMemoryStoreLimits::default();
    Arc::new(SharedStore::new(limits, enqueue_observer, metric_sink))
}

/// The inputs that every pool of a runtime shares.
pub(super) struct PoolInputs<'a> {
    pub(super) engine: &'a Engine,
    pub(super) host: &'a Host,
    pub(super) log_sink: Arc<dyn LogSink>,
    pub(super) shared_store: Arc<dyn SharedServices>,
    pub(super) upstreams: Arc<dyn CalloutUpstreams>,
    pub(super) fixed_properties: Arc<WasmProperties>,
    pub(super) root_callback_thread: &'a RootCallbackThread,
}

/// Compile `plugin` and start the guests of its pool.
pub(super) fn build_pool(
    pool_index: usize,
    plugin: &WasmPluginConf,
    inputs: &PoolInputs<'_>,
) -> Result<GuestPool> {
    let bytes = std::fs::read(&plugin.path).or_err_with(ErrorType::ReadError, || {
        format!(
            "failed to read wasm plugin {} from {}",
            plugin.name,
            plugin.path.display()
        )
    })?;
    let module = Module::new(inputs.engine, &bytes)
        .or_err_with(ErrorType::InternalError, || {
            format!("wasm plugin {} does not compile", plugin.name)
        })?;
    let services = plugin.services(inputs.log_sink.clone(), inputs.shared_store.clone());
    let spec = GuestSpec::new(inputs.host, &module, services, &plugin.limits)
        .or_err_with(ErrorType::InternalError, || {
            format!("wasm plugin {} is not a supported module", plugin.name)
        })?;
    let root_callback_plugin =
        RootCallbackPluginState::new(&plugin.name, inputs.fixed_properties.clone());
    GuestPool::new(GuestPoolConf {
        pool_index,
        name: plugin.name.clone(),
        spec,
        plugin_config: plugin.plugin_config(),
        slot_count: plugin.slots,
        phases: plugin.phase_conf(),
        callout_conf: plugin.callout_conf(inputs.upstreams.clone()),
        root_callback_plugin: Arc::new(root_callback_plugin),
        root_callback_sender: inputs.root_callback_thread.sender(),
    })
}
