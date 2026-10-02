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

//! Runtime construction
//!
//! Building a runtime validates the plugin configurations, creates the shared store, and then
//! builds a guest pool for each plugin.

use super::plugin::WasmPluginConf;
use super::pool::events::RootCallbackEvent;
use super::pool::{GuestPool, GuestPoolConf};
use super::shared_store::SharedStore;
use crate::callout::CalloutUpstreams;
use crate::observability::WasmMetricSink;
use crate::properties::WasmProperties;
use crate::root_callbacks::RootCallbackPluginState;
use crate::root_callbacks::RootCallbackThread;
use pingora_error::{Error, ErrorType, OrErr, Result};
use proxy_wasm_host::abi::v0_2_1::{
    GuestSpec, Host, InMemoryStoreLimits, LogSink, QueueEnqueued, SharedServices,
};
use proxy_wasm_host::{Engine, Module};
use std::collections::HashMap;
use std::sync::Arc;

/// Validate the plugin configurations and map each plugin name to its index.
///
/// # Errors
///
/// Returns an error if `plugins` is empty, if a configuration is invalid, or if two plugins have
/// the same name.
pub(super) fn checked_plugin_indexes(plugins: &[WasmPluginConf]) -> Result<HashMap<String, usize>> {
    if plugins.is_empty() {
        return Error::e_explain(
            ErrorType::InternalError,
            "wasm runtime needs at least one plugin",
        );
    }
    let mut names = HashMap::with_capacity(plugins.len());
    for (index, plugin) in plugins.iter().enumerate() {
        plugin.check()?;
        if names.insert(plugin.name.clone(), index).is_some() {
            return Error::e_explain(
                ErrorType::InternalError,
                format!("wasm plugin {}: duplicate plugin name", plugin.name),
            );
        }
    }
    Ok(names)
}

/// Create the store for shared data, queues, and metrics.
///
/// Every item enqueued on a shared queue is reported to the root callback thread.
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

/// Inputs shared by every pool of a runtime.
pub(super) struct PoolInputs<'a> {
    pub(super) engine: &'a Engine,
    pub(super) host: &'a Host,
    pub(super) log_sink: Arc<dyn LogSink>,
    pub(super) shared_store: Arc<dyn SharedServices>,
    pub(super) upstreams: Arc<dyn CalloutUpstreams>,
    pub(super) fixed_properties: Arc<WasmProperties>,
    pub(super) root_callback_thread: &'a RootCallbackThread,
}

/// Compile `plugin` and build its pool, starting one guest per slot.
///
/// # Errors
///
/// Returns an error if the file cannot be read, does not compile, or is not a supported
/// Proxy-Wasm module, or if a guest fails to start.
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
            format!("failed to compile wasm plugin {}", plugin.name)
        })?;
    let services = plugin.services(inputs.log_sink.clone(), inputs.shared_store.clone());
    let spec = GuestSpec::new(inputs.host, &module, services, &plugin.limits).or_err_with(
        ErrorType::InternalError,
        || {
            format!(
                "wasm plugin {}: not a supported Proxy-Wasm module",
                plugin.name
            )
        },
    )?;
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
