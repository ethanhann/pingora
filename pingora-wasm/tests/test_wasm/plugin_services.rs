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

//! Plugin services tests
//!
//! Covers ticks, shared queues, callouts from the root context, contexts kept after their
//! request, and metrics.

use super::get;
use crate::utils::{echo_origin, eventually, guest_lines, init, metrics_text, runtime};
use std::time::Duration;

fn count_lines_with(text: &str) -> usize {
    guest_lines()
        .iter()
        .filter(|line| line.contains(text))
        .count()
}

#[tokio::test]
async fn plugin_ticks_at_configured_period() {
    init().await;
    let (origin, _) = echo_origin().await;
    get(6403, "/", origin.addr().port(), &[]).await;
    assert!(eventually(|| count_lines_with("tick of 6403") > 0).await);
    let before = count_lines_with("tick of 6403");

    tokio::time::sleep(Duration::from_millis(600)).await;

    let ticks = count_lines_with("tick of 6403") - before;
    assert!((3..=8).contains(&ticks), "{ticks} ticks in 600 ms");
}

#[tokio::test]
async fn queue_item_from_request_reaches_one_root_context() {
    init().await;
    let (origin, _) = echo_origin().await;

    get(6404, "/queued-6404", origin.addr().port(), &[]).await;

    assert!(eventually(|| count_lines_with("path seen: /queued-6404") == 1).await);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(count_lines_with("path seen: /queued-6404"), 1);
}

#[tokio::test]
async fn plugin_counter_is_published_to_prometheus() {
    init().await;
    let (origin, _) = echo_origin().await;

    for _ in 0..3 {
        get(6405, "/", origin.addr().port(), &[]).await;
    }

    let output = metrics_text();
    assert!(
        output.contains("wasm_test_requests{vm_id=\"counter\"} 3"),
        "{output}"
    );
}

#[tokio::test]
async fn callout_from_tick_delivers_response() {
    init().await;
    let (origin, _) = echo_origin().await;

    get(6409, "/", origin.addr().port(), &[]).await;

    assert!(eventually(|| count_lines_with("root callout response of 6409") == 1).await);
}

#[tokio::test]
async fn held_context_ends_after_proxy_done() {
    init().await;
    let (origin, _) = echo_origin().await;
    let runtime = runtime(6410);

    get(6410, "/", origin.addr().port(), &[]).await;

    assert!(eventually(|| count_lines_with("held context of 6410 logged") == 1).await);
    assert!(eventually(|| runtime.held_contexts() == 0 && runtime.callouts_in_flight() == 0).await);
}

#[tokio::test]
async fn held_context_gets_failure_for_callout_of_ended_request() {
    init().await;
    let (origin, _) = echo_origin().await;
    let runtime = runtime(6412);

    get(6412, "/", origin.addr().port(), &[]).await;

    assert!(eventually(|| count_lines_with("held context of 6412 logged") == 1).await);
    assert!(eventually(|| runtime.held_contexts() == 0).await);
}

#[tokio::test]
async fn callout_timeout_counts_as_failure() {
    init().await;
    let (origin, _) = echo_origin().await;

    let res = get(6411, "/", origin.addr().port(), &[]).await;

    assert_eq!(res.status(), 418);
    let failure = "wasm_callout_failures_total{failure=\"timeout\",plugin=\"relay-metric\"} 1";
    assert!(eventually(|| metrics_text().contains(failure)).await);
}
