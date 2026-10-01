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

//! The epoch ticker.
//!
//! Wasmtime measures the CPU time of a guest in epochs, and the ticker advances the epoch.
//! Without it, a guest has no CPU time limit.

use super::RuntimeInner;
use crate::ERR_PLUGIN_FAILED;
use pingora_error::{OrErr, Result};
use proxy_wasm_host::Engine;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::thread;
use std::time::Duration;

// Linux keeps 15 bytes of a thread name
const THREAD_NAME: &str = "wasm-epoch";

pub(crate) struct Ticker {
    #[cfg(test)]
    pub(crate) ticking: Arc<AtomicBool>,
}

impl Ticker {
    pub(crate) fn new() -> Self {
        Ticker {
            #[cfg(test)]
            ticking: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Start the ticker thread. It stops when the runtime is dropped.
    pub(crate) fn start(&self, runtime: &Arc<RuntimeInner>) -> Result<()> {
        let weak = Arc::downgrade(runtime);
        let period = runtime.engine.epoch_period();
        #[cfg(test)]
        let ticking = self.ticking.clone();
        #[cfg(test)]
        ticking.store(true, Ordering::Relaxed);
        thread::Builder::new()
            .name(THREAD_NAME.to_string())
            .spawn(move || {
                tick(&weak, period);
                #[cfg(test)]
                ticking.store(false, Ordering::Relaxed);
            })
            .or_err(ERR_PLUGIN_FAILED, "failed to start the wasm epoch ticker")?;
        Ok(())
    }
}

fn tick(runtime: &Weak<RuntimeInner>, period: Duration) {
    loop {
        thread::sleep(period);
        match runtime.upgrade() {
            Some(runtime) => runtime.engine.increment_epoch(),
            None => return,
        }
    }
}

/// Run `f` with a ticker that stops when `f` returns, so a guest that loops forever while it
/// starts reaches its time limit.
pub(super) fn with_ticker<R>(engine: &Engine, f: impl FnOnce() -> R) -> R {
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
    use crate::test_support::{fixture, plugin};
    use crate::WasmRuntime;
    use proxy_wasm_host::EngineConfig;

    #[test]
    fn the_ticker_starts_on_the_first_call() {
        let runtime =
            WasmRuntime::new(vec![plugin("a", fixture("add-request-header"), 1)]).unwrap();
        let ticking = runtime.inner.ticker.ticking.clone();
        let before = ticking.load(Ordering::Relaxed);

        runtime.inner.start_threads().unwrap();

        assert!(!before);
        assert!(ticking.load(Ordering::Relaxed));
    }

    #[test]
    fn the_ticker_stops_when_the_runtime_drops() {
        let runtime =
            WasmRuntime::new(vec![plugin("a", fixture("add-request-header"), 1)]).unwrap();
        runtime.inner.start_threads().unwrap();
        let ticking = runtime.inner.ticker.ticking.clone();

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
}
