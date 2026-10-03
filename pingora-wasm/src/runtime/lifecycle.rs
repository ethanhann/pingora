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

//! Runtime lifecycle

use super::RuntimeInner;
use log::warn;
use once_cell::sync::OnceCell;
use pingora_error::Result;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::{Notify, OnceCell as AsyncOnceCell};
use tokio::time::timeout;

#[derive(Default)]
pub(crate) struct Lifecycle {
    /// Whether the threads were started, set once by the first start or by the end.
    threads_started: OnceCell<bool>,
    ending: AtomicBool,
    live_ctxs: AtomicUsize,
    last_ctx_dropped: Notify,
    ended: AsyncOnceCell<()>,
}

impl Lifecycle {
    pub(crate) fn start_threads_once(&self, start: impl FnOnce() -> Result<()>) -> Result<()> {
        self.threads_started.get_or_try_init(|| -> Result<bool> {
            if self.ending.load(Ordering::SeqCst) {
                return Ok(false);
            }
            start()?;
            Ok(true)
        })?;
        Ok(())
    }

    /// Mark the runtime as ending, and return whether its threads were started.
    ///
    /// A start that is in progress finishes first. Once this returns, no filter starts the
    /// threads.
    pub(crate) fn begin_end(&self) -> bool {
        self.ending.store(true, Ordering::SeqCst);
        *self.threads_started.get_or_init(|| false)
    }

    pub(crate) fn is_ending(&self) -> bool {
        self.ending.load(Ordering::SeqCst)
    }

    pub(crate) fn ctx_created(&self) {
        self.live_ctxs.fetch_add(1, Ordering::SeqCst);
    }

    pub(crate) fn ctx_dropped(&self) {
        if self.live_ctxs.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.last_ctx_dropped.notify_waiters();
        }
    }

    pub(crate) async fn wait_for_no_live_ctx(&self) {
        loop {
            let mut dropped = pin!(self.last_ctx_dropped.notified());
            dropped.as_mut().enable();
            if self.live_ctxs.load(Ordering::SeqCst) == 0 {
                return;
            }
            dropped.await;
        }
    }
}

impl RuntimeInner {
    /// End the runtime: wait for its requests to finish, then end its plugins.
    ///
    /// A second call waits for the first one to return.
    pub(crate) async fn end(&self) {
        self.lifecycle.ended.get_or_init(|| self.end_once()).await;
    }

    async fn end_once(&self) {
        for pool in &self.pools {
            pool.stop_rebuilds();
        }
        if !self.lifecycle.begin_end() {
            return;
        }
        self.lifecycle.wait_for_no_live_ctx().await;
        let plugin_names = self.pools.iter().map(|pool| pool.name.clone()).collect();
        let mut progress = self.root_callback_thread.send_end(plugin_names);
        let finished = async {
            // An error means the root callback thread has stopped, so no plugin is left
            let _ = progress.wait_for(|progress| progress.finished).await;
        };
        if timeout(self.shutdown_wait_limit, finished).await.is_ok() {
            return;
        }
        let limit = self.shutdown_wait_limit;
        let names = progress.borrow().waiting_for.join(", ");
        warn!("wasm runtime ended after shutdown_wait_limit {limit:?} with plugins still running: {names}");
    }
}

#[cfg(test)]
mod tests {
    use crate::test_support::callouts::{authz_services, FixedSender};
    use crate::test_support::{
        crate_log_lines_with, eventually, plugin, record_crate_logs, session, wat_guest,
        RecordedGuestLogs, Wat, GET,
    };
    use crate::WasmRuntime;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tokio::sync::Notify;
    use tokio::time::timeout;

    const DATA: &str = r#"(data (i32.const 700) "root done") (data (i32.const 710) "root deleted")
        (data (i32.const 730) "stream deleted")"#;
    const RECORD_ROOT: &str = "(i32.store (i32.const 600) (local.get 0)) i32.const 1";
    const LOG_DELETED: &str = "(if (i32.eq (local.get 0) (i32.load (i32.const 600)))
        (then (drop (call $log (i32.const 2) (i32.const 710) (i32.const 12))))
        (else (drop (call $log (i32.const 2) (i32.const 730) (i32.const 14)))))";
    const LOG_RESULT_THEN_DONE: &str = "(call $log_result (local.get 2)) (drop (call $proxy_done))";

    /// A guest whose root sends a callout from `proxy_on_done` and calls `proxy_done` when the
    /// result arrives.
    const ROOT_SENDS_ON_DONE: Wat = Wat {
        data_segments: DATA,
        configure: RECORD_ROOT,
        done: "(if (result i32) (i32.eq (local.get 0) (i32.load (i32.const 600)))
            (then (drop (call $log (i32.const 2) (i32.const 700) (i32.const 9)))
                (call $call_and_log_status) (i32.const 0))
            (else (i32.const 1)))",
        http_call_response: Some(LOG_RESULT_THEN_DONE),
        delete: LOG_DELETED,
        ..ROOT_DONE_AT_ONCE
    };

    /// A guest whose request context sends a callout from `proxy_on_done` and is held until the
    /// result arrives.
    const HOLDS_REQUEST_CONTEXT: Wat = Wat {
        done: "(if (result i32) (i32.eq (local.get 0) (i32.load (i32.const 600)))
            (then (drop (call $log (i32.const 2) (i32.const 700) (i32.const 9))) (i32.const 1))
            (else (i32.store (i32.const 604) (local.get 0)) (call $call_and_log_status)
                (i32.const 0)))",
        http_call_response: Some(
            "(call $log_result (local.get 2))
            (drop (call $set_effective_context (i32.load (i32.const 604))))
            (drop (call $proxy_done))",
        ),
        ..ROOT_DONE_AT_ONCE
    };

    /// A guest whose root returns `false` from `proxy_on_done` and never calls `proxy_done`.
    const ROOT_NEVER_DONE: Wat = Wat {
        done: "(if (result i32) (i32.eq (local.get 0) (i32.load (i32.const 600)))
            (then (drop (call $log (i32.const 2) (i32.const 700) (i32.const 9))) (i32.const 0))
            (else (i32.const 1)))",
        ..ROOT_DONE_AT_ONCE
    };

    const ROOT_DONE_AT_ONCE: Wat = Wat {
        abi: true,
        vm_start: "i32.const 1",
        configure: RECORD_ROOT,
        request_headers: "i32.const 0",
        done: "i32.const 1",
        request_body: None,
        response_headers: None,
        response_body: None,
        response_trailers: None,
        http_call_response: None,
        log: None,
        tick: None,
        queue_ready: None,
        delete: LOG_DELETED,
        data_segments: DATA,
    };

    fn runtime_with(
        label: &str,
        wat: Wat,
        sender: Arc<FixedSender>,
        shutdown_wait_limit: Duration,
    ) -> (WasmRuntime, Arc<RecordedGuestLogs>) {
        let logs = Arc::new(RecordedGuestLogs::default());
        let mut services = authz_services();
        services.log_sink = logs.clone();
        services.shutdown_wait_limit = shutdown_wait_limit;
        let plugins = vec![plugin(label, wat_guest(label, wat), 1)];
        let runtime = WasmRuntime::new_with_callout_sender(plugins, services, sender).unwrap();
        (runtime, logs)
    }

    fn lines(logs: &RecordedGuestLogs) -> Vec<String> {
        logs.0.lock().clone()
    }

    #[tokio::test]
    async fn end_waits_for_live_ctx_then_for_root_to_call_proxy_done() {
        let gate = Arc::new(Notify::new());
        let sender = FixedSender::responds_after("ok", gate.clone());
        let limit = Duration::from_secs(5);
        let (runtime, logs) = runtime_with("a", ROOT_SENDS_ON_DONE, sender.clone(), limit);
        let mut ctx = runtime.chain(&["a"]).unwrap().new_ctx();
        let (mut session, _client) = session(GET).await;
        ctx.request_filter(&mut session).await.unwrap();
        let inner = runtime.inner.clone();
        let end = tokio::spawn(async move { inner.end().await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        let lines_while_ctx_alive = lines(&logs);
        drop(ctx);
        assert!(eventually(|| sender.sent_count() == 1).await);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let ended_before_proxy_done = end.is_finished();
        gate.notify_one();

        let ended = timeout(Duration::from_secs(5), end).await;

        assert!(ended.is_ok());
        assert!(
            lines_while_ctx_alive.is_empty(),
            "{lines_while_ctx_alive:?}"
        );
        assert!(!ended_before_proxy_done);
        let want = [
            "stream deleted",
            "root done",
            "accepted",
            "response",
            "root deleted",
        ];
        assert_eq!(lines(&logs), want);
    }

    #[tokio::test]
    async fn end_waits_for_held_context_to_call_proxy_done() {
        let gate = Arc::new(Notify::new());
        let sender = FixedSender::responds_after("ok", gate.clone());
        let limit = Duration::from_secs(5);
        let (runtime, logs) = runtime_with("a", HOLDS_REQUEST_CONTEXT, sender.clone(), limit);
        let mut ctx = runtime.chain(&["a"]).unwrap().new_ctx();
        let (mut session, _client) = session(GET).await;
        ctx.request_filter(&mut session).await.unwrap();
        drop(ctx);
        let inner = runtime.inner.clone();
        let end = tokio::spawn(async move { inner.end().await });
        let has_root_done = || lines(&logs).iter().any(|line| line == "root done");
        assert!(eventually(has_root_done).await);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let ended_while_held = end.is_finished();
        let held_before_result = runtime.held_contexts();
        gate.notify_one();

        let ended = timeout(Duration::from_secs(5), end).await;

        assert!(ended.is_ok());
        assert!(!ended_while_held);
        assert_eq!(held_before_result, 1);
        assert_eq!(runtime.held_contexts(), 0);
        let want = [
            "accepted",
            "root done",
            "response",
            "stream deleted",
            "root deleted",
        ];
        assert_eq!(lines(&logs), want);
    }

    #[tokio::test]
    async fn end_returns_at_limit_with_one_warning_when_root_never_calls_proxy_done() {
        record_crate_logs();
        let sender = FixedSender::responds("ok");
        let limit = Duration::from_millis(100);
        let (runtime, logs) = runtime_with("never-done", ROOT_NEVER_DONE, sender, limit);
        runtime.inner.start_threads().unwrap();
        let started = Instant::now();

        let ended = timeout(Duration::from_secs(5), runtime.inner.end()).await;

        assert!(ended.is_ok());
        assert!(started.elapsed() >= limit);
        let warning = "shutdown_wait_limit 100ms with plugins still running: never-done";
        assert_eq!(crate_log_lines_with(warning).len(), 1);
        assert_eq!(lines(&logs), ["root done"]);
    }

    #[tokio::test]
    async fn end_returns_at_once_when_threads_never_started_or_runtime_already_ended() {
        let cases = [
            ("never started", false, 0, Vec::<&str>::new()),
            ("already ended", true, 1, vec!["root done"]),
        ];
        for (name, start_threads, ends_before, want_lines) in cases {
            let sender = FixedSender::responds("ok");
            let limit = Duration::from_millis(200);
            let (runtime, logs) = runtime_with("a", ROOT_NEVER_DONE, sender, limit);
            if start_threads {
                runtime.inner.start_threads().unwrap();
            }
            for _ in 0..ends_before {
                runtime.inner.end().await;
            }

            let ended = timeout(Duration::from_millis(100), runtime.inner.end()).await;

            assert!(ended.is_ok(), "{name}");
            assert_eq!(lines(&logs), want_lines, "{name}");
        }
    }
}
