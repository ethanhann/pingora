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

//! Fail policy tests
//!
//! Covers what a plugin failure does to its request under each fail policy.

use super::{all, get, header};
use crate::utils::raw::send_chunked_request;
use crate::utils::{
    callout_origin, echo_origin, eventually, guest_messages, init, metrics_text, runtime,
};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

#[tokio::test]
async fn trap_under_closed_responds_with_503_and_is_counted() {
    init().await;
    let (origin, _) = echo_origin().await;

    let trapped = get(6414, "/", origin.addr().port(), &[("x-trap", "1")]).await;

    assert_eq!(trapped.status(), 503);
    let next = get(6414, "/", origin.addr().port(), &[]).await;
    assert_eq!(next.status(), 200);
    let metrics = metrics_text();
    let failed = "wasm_plugin_failures_total\
        {failure=\"guest_error\",outcome=\"failed\",plugin=\"trap-closed\"} 1";
    assert!(metrics.contains(failed), "{metrics}");
    let replaced = "wasm_guests_replaced_total{plugin=\"trap-closed\"} 1";
    assert!(metrics.contains(replaced), "{metrics}");
}

#[tokio::test]
async fn trap_under_open_continues_without_plugin() {
    init().await;
    let (origin, count) = echo_origin().await;

    let res = get(6415, "/", origin.addr().port(), &[]).await;

    assert_eq!(res.status(), 200);
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(all(&res, "x-echo-x-order"), ["next"]);
    assert_eq!(
        header(&res, "x-echo-x-test-skipped").as_deref(),
        Some("trap-open")
    );
    let metrics = metrics_text();
    let skipped = "wasm_plugin_failures_total\
        {failure=\"guest_error\",outcome=\"skipped\",plugin=\"trap-open\"} 1";
    assert!(metrics.contains(skipped), "{metrics}");
}

#[tokio::test]
async fn trap_under_open_releases_held_request_body() {
    init().await;
    let (origin, _) = echo_origin().await;
    let chunks = ["one ", "two ", "three"];

    let res = send_chunked_request(6416, origin.addr().port(), "POST", &[], &chunks).await;

    assert_eq!(res.status, 200);
    assert_eq!(res.body, "one two three");
    let metrics = metrics_text();
    let skipped = "wasm_plugin_failures_total\
        {failure=\"guest_error\",outcome=\"skipped\",plugin=\"hold-trap-open\"} 1";
    assert!(metrics.contains(skipped), "{metrics}");
}

#[tokio::test]
async fn plugin_paused_after_callout_under_open_is_skipped() {
    init().await;
    let (origin, count) = echo_origin().await;

    let res = get(6417, "/", origin.addr().port(), &[]).await;

    assert_eq!(res.status(), 200);
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(callout_origin("stay-paused").requests().len(), 1);
    assert_eq!(
        header(&res, "x-echo-x-test-skipped").as_deref(),
        Some("stay-paused")
    );
}

#[tokio::test]
async fn callout_chain_ends_at_wait_limit() {
    init().await;
    let (origin, count) = echo_origin().await;
    let started = Instant::now();

    let res = get(6418, "/", origin.addr().port(), &[]).await;

    assert_eq!(res.status(), 503);
    assert!(started.elapsed() >= Duration::from_millis(400));
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert!(callout_origin("callout-chain").requests().len() > 1);
    let metrics = metrics_text();
    let wait_limit = "wasm_plugin_failures_total\
        {failure=\"wait_limit\",outcome=\"failed\",plugin=\"callout-chain\"} 1";
    assert!(metrics.contains(wait_limit), "{metrics}");
    assert!(eventually(|| runtime(6418).open_contexts() == 0).await);
}

#[tokio::test]
async fn plugin_ahead_of_failed_plugin_reads_503_in_proxy_on_log() {
    init().await;
    let (origin, _) = echo_origin().await;

    let res = get(6419, "/", origin.addr().port(), &[]).await;

    assert_eq!(res.status(), 503);
    assert!(eventually(|| !guest_messages("code-logger").is_empty()).await);
    assert_eq!(guest_messages("code-logger"), [503_i64.to_le_bytes()]);
}

#[tokio::test]
async fn held_request_body_over_limit_under_open_responds_with_413() {
    init().await;
    let (origin, count) = echo_origin().await;
    let chunks = ["twenty bytes of body"];

    let res = send_chunked_request(6420, origin.addr().port(), "POST", &[], &chunks).await;

    assert_eq!(res.status, 413);
    assert_eq!(count.load(Ordering::SeqCst), 0);
}
