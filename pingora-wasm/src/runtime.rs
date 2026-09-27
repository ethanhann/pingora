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

use crate::chain::WasmChain;
use crate::logging::LogCrateSink;
use crate::plugin::WasmPluginConf;
use crate::pool::GuestPool;
use parking_lot::Mutex;
use pingora_error::{Error, ErrorType, OrErr, Result};
use proxy_wasm_host::abi::v0_2_1::{GuestSpec, Host, InMemoryStore, LogSink, SharedServices};
use proxy_wasm_host::{Engine, EngineConfig, Module};
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::thread;

/// The engine, the shared store, and the guests of a set of plugins.
///
/// Build it before the server starts. A proxy can build a new runtime to reload its plugins,
/// and requests that started on the old runtime finish on it.
#[derive(Clone)]
pub struct WasmRuntime {
    pub(crate) inner: Arc<RuntimeInner>,
}

pub(crate) struct RuntimeInner {
    engine: Engine,
    ticker_started: AtomicBool,
    ticker_lock: Mutex<()>,
    #[cfg(test)]
    ticking: Arc<AtomicBool>,
    pub(crate) pools: Vec<GuestPool>,
    names: HashMap<String, usize>,
}

impl fmt::Debug for WasmRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WasmRuntime")
            .field("plugins", &self.inner.plugin_names())
            .finish()
    }
}

impl WasmRuntime {
    /// Compiles and starts the plugins, with guest logs sent to the `log` crate.
    pub fn new(plugins: Vec<WasmPluginConf>) -> Result<Self> {
        Self::new_with_log_sink(plugins, Arc::new(LogCrateSink))
    }

    /// Compiles and starts the plugins, with guest logs sent to `sink`.
    pub fn new_with_log_sink(plugins: Vec<WasmPluginConf>, sink: Arc<dyn LogSink>) -> Result<Self> {
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
                    )
                })
                .collect::<Result<Vec<_>>>()
        })?;
        Ok(WasmRuntime {
            inner: Arc::new(RuntimeInner {
                engine,
                ticker_started: AtomicBool::new(false),
                ticker_lock: Mutex::new(()),
                #[cfg(test)]
                ticking: Arc::new(AtomicBool::new(false)),
                pools,
                names,
            }),
        })
    }

    /// A chain of the named plugins, in request order.
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

    /// The number of stream contexts that are open in every guest.
    pub fn open_contexts(&self) -> usize {
        self.inner.pools.iter().map(GuestPool::open_contexts).sum()
    }

    /// The number of stream contexts that a guest holds after the request ended.
    pub fn held_contexts(&self) -> usize {
        self.inner.pools.iter().map(GuestPool::held_contexts).sum()
    }
}

impl RuntimeInner {
    /// Starts the epoch ticker on the first call.
    ///
    /// It starts here and not in [WasmRuntime::new], because a daemon fork after `new` would
    /// lose the thread. The thread stops when the runtime is dropped. Without the ticker no guest
    /// has a CPU time limit, so a failed start is an error and the next call tries again.
    pub(crate) fn start_ticker(self: &Arc<Self>) -> Result<()> {
        if self.ticker_started.load(Ordering::Acquire) {
            return Ok(());
        }
        let _lock = self.ticker_lock.lock();
        if self.ticker_started.load(Ordering::Acquire) {
            return Ok(());
        }
        let runtime = Arc::downgrade(self);
        let period = self.engine.epoch_period();
        #[cfg(test)]
        let ticking = self.ticking.clone();
        #[cfg(test)]
        ticking.store(true, Ordering::Relaxed);
        thread::Builder::new()
            .name("pingora-wasm-epoch".to_string())
            .spawn(move || {
                tick(&runtime, period);
                #[cfg(test)]
                ticking.store(false, Ordering::Relaxed);
            })
            .or_err(
                ErrorType::HTTPStatus(503),
                "failed to start the wasm epoch ticker",
            )?;
        self.ticker_started.store(true, Ordering::Release);
        Ok(())
    }

    pub(crate) fn plugin_names(&self) -> Vec<&str> {
        self.pools.iter().map(|pool| pool.name.as_str()).collect()
    }

    #[cfg(test)]
    pub(crate) fn ticking(&self) -> Arc<AtomicBool> {
        self.ticking.clone()
    }
}

fn tick(runtime: &Weak<RuntimeInner>, period: std::time::Duration) {
    loop {
        thread::sleep(period);
        match runtime.upgrade() {
            Some(runtime) => runtime.engine.increment_epoch(),
            None => return,
        }
    }
}

/// Runs `f` with a ticker that stops when `f` returns, so a guest start that loops forever
/// reaches its time limit.
fn with_ticker<R>(engine: &Engine, f: impl FnOnce() -> R) -> R {
    struct Done<'a>(&'a AtomicBool);

    impl Drop for Done<'_> {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    let done = AtomicBool::new(false);
    let period = engine.epoch_period();
    thread::scope(|scope| {
        scope.spawn(|| {
            while !done.load(Ordering::Relaxed) {
                thread::sleep(period);
                engine.increment_epoch();
            }
        });
        let _done = Done(&done);
        f()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ctx::PluginRecord;
    use crate::test_support::{fixture, plugin, wat_guest, Wat};
    use proxy_wasm_host::abi::v0_2_1::types::LogLevel;
    use proxy_wasm_host::abi::v0_2_1::LogContext;
    use proxy_wasm_host::Limits;
    use std::time::Duration;

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

    #[test]
    fn the_ticker_starts_on_the_first_call() {
        let runtime =
            WasmRuntime::new(vec![plugin("a", fixture("add-request-header"), 1)]).unwrap();
        let ticking = runtime.inner.ticking();
        let before = ticking.load(Ordering::Relaxed);

        runtime.inner.start_ticker().unwrap();

        assert!(!before);
        assert!(ticking.load(Ordering::Relaxed));
    }

    #[test]
    fn the_ticker_stops_when_the_runtime_drops() {
        let runtime =
            WasmRuntime::new(vec![plugin("a", fixture("add-request-header"), 1)]).unwrap();
        runtime.inner.start_ticker().unwrap();
        let ticking = runtime.inner.ticking();

        drop(runtime);

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while ticking.load(Ordering::Relaxed) && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        assert!(!ticking.load(Ordering::Relaxed));
    }

    #[test]
    fn with_ticker_returns_when_the_closure_panics() {
        let engine = EngineConfig::new()
            .with_external_ticks(true)
            .build()
            .unwrap();
        let (sender, receiver) = std::sync::mpsc::channel();

        thread::spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                with_ticker(&engine, || panic!("the guest start failed"))
            }));
            let _ = sender.send(result.is_err());
        });

        let panicked = receiver.recv_timeout(Duration::from_secs(5));
        assert_eq!(panicked, Ok(true));
    }

    #[test]
    fn pick_skips_a_locked_slot_and_a_slot_with_no_guest() {
        let runtime =
            WasmRuntime::new(vec![plugin("a", fixture("add-request-header"), 3)]).unwrap();
        let pool = &runtime.inner.pools[0];
        pool.fail_slot(1);
        let held = pool.lock_slot(0);

        let picked: Vec<_> = (0..4).map(|_| pool.pick().unwrap().0).collect();

        drop(held);
        assert_eq!(picked, [2, 2, 2, 2]);
    }

    #[test]
    fn a_recent_failed_rebuild_waits_for_its_backoff() {
        let runtime =
            WasmRuntime::new(vec![plugin("a", fixture("add-request-header"), 2)]).unwrap();
        let pool = &runtime.inner.pools[0];
        pool.fail_slot(0);
        let busy = pool.lock_slot(1);

        let picked = thread::scope(|scope| {
            let picker = scope.spawn(|| pool.pick().map(|(slot, _)| slot).ok());
            thread::sleep(Duration::from_millis(50));
            drop(busy);
            picker.join().unwrap()
        });

        assert_eq!(picked, Some(1));
        assert!(pool.lock_slot(0).is_none());
    }

    fn open_context(runtime: &WasmRuntime, ctx: &mut crate::WasmCtx) {
        let pool = &runtime.inner.pools[0];
        let (slot, mut guard) = pool.pick().unwrap();
        let loaded = guard.as_mut().unwrap();
        let root = loaded.root;
        let guest = loaded.guest.id();
        let context = ctx
            .run(&mut loaded.guest, |scope| {
                scope.on_context_create(Some(root))
            })
            .unwrap();
        pool.opened(slot);
        ctx.records[0] = Some(PluginRecord {
            slot,
            guest,
            context,
        });
    }

    #[test]
    fn dropping_a_ctx_deletes_its_open_context() {
        let runtime =
            WasmRuntime::new(vec![plugin("a", fixture("add-request-header"), 1)]).unwrap();
        let mut ctx = runtime.chain(&["a"]).unwrap().new_ctx();
        open_context(&runtime, &mut ctx);
        let open = runtime.open_contexts();

        drop(ctx);

        assert_eq!(open, 1);
        assert_eq!(runtime.open_contexts(), 0);
        assert_eq!(runtime.held_contexts(), 0);
    }

    #[test]
    fn a_held_context_moves_to_the_held_count() {
        let runtime = WasmRuntime::new(vec![plugin(
            "held",
            wat_guest(
                "held-unit",
                Wat {
                    done: "i32.const 0",
                    ..Wat::default()
                },
            ),
            1,
        )])
        .unwrap();
        let mut ctx = runtime.chain(&["held"]).unwrap().new_ctx();
        open_context(&runtime, &mut ctx);

        drop(ctx);

        assert_eq!(runtime.open_contexts(), 0);
        assert_eq!(runtime.held_contexts(), 1);
    }

    #[test]
    fn a_swap_returns_the_session_header() {
        let runtime =
            WasmRuntime::new(vec![plugin("a", fixture("add-request-header"), 1)]).unwrap();
        let mut ctx = runtime.chain(&["a"]).unwrap().new_ctx();
        let mut session_header =
            pingora_http::RequestHeader::build("POST", b"/original", None).unwrap();

        ctx.request_in(&mut session_header);
        let during = session_header.raw_path().to_vec();
        ctx.request_out(&mut session_header);

        assert_eq!(during, b"/");
        assert_eq!(session_header.method, http::Method::POST);
        assert_eq!(session_header.raw_path(), b"/original");
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
    fn new_with_log_sink_sends_guest_lines_to_the_sink() {
        let sink = Arc::new(Recording::default());

        WasmRuntime::new_with_log_sink(vec![plugin("b", fixture("http-example"), 1)], sink.clone())
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
