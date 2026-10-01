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

//! Ticks, queue wakes, root callouts, held contexts, and metrics.

use super::get;
use crate::utils::{echo_origin, eventually, guest_lines, init, metrics_output, runtime};
use std::time::Duration;

fn count_lines(text: &str) -> usize {
    guest_lines()
        .iter()
        .filter(|line| line.contains(text))
        .count()
}

#[tokio::test]
async fn a_plugin_logs_3_to_8_ticks_in_6_periods() {
    init().await;
    let (origin, _) = echo_origin().await;
    get(6403, "/", origin.addr().port(), &[]).await;
    assert!(eventually(|| count_lines("tick of 6403") > 0).await);
    let before = count_lines("tick of 6403");

    tokio::time::sleep(Duration::from_millis(600)).await;

    let ticks = count_lines("tick of 6403") - before;
    assert!((3..=8).contains(&ticks), "{ticks} ticks in 600 ms");
}

#[tokio::test]
async fn a_queue_item_from_a_request_reaches_one_root() {
    init().await;
    let (origin, _) = echo_origin().await;

    get(6404, "/queued-6404", origin.addr().port(), &[]).await;

    assert!(eventually(|| count_lines("path seen: /queued-6404") == 1).await);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(count_lines("path seen: /queued-6404"), 1);
}

#[tokio::test]
async fn a_guest_counter_shows_in_the_prometheus_output() {
    init().await;
    let (origin, _) = echo_origin().await;

    for _ in 0..3 {
        get(6405, "/", origin.addr().port(), &[]).await;
    }

    let output = metrics_output();
    assert!(
        output.contains("wasm_test_requests{vm_id=\"counter\"} 3"),
        "{output}"
    );
}

#[tokio::test]
async fn a_root_callout_from_a_tick_delivers_its_response() {
    init().await;
    let (origin, _) = echo_origin().await;

    get(6409, "/", origin.addr().port(), &[]).await;

    assert!(eventually(|| count_lines("root callout response of 6409") == 1).await);
}

#[tokio::test]
async fn a_held_context_ends_after_the_guest_calls_proxy_done() {
    init().await;
    let (origin, _) = echo_origin().await;
    let runtime = runtime(6410);

    get(6410, "/", origin.addr().port(), &[]).await;

    assert!(eventually(|| count_lines("held context of 6410 logged") == 1).await);
    assert!(eventually(|| runtime.held_contexts() == 0 && runtime.callouts_in_flight() == 0).await);
}

#[tokio::test]
async fn a_held_context_receives_a_failure_for_a_callout_of_its_request() {
    init().await;
    let (origin, _) = echo_origin().await;
    let runtime = runtime(6412);

    get(6412, "/", origin.addr().port(), &[]).await;

    assert!(eventually(|| count_lines("held context of 6412 logged") == 1).await);
    assert!(eventually(|| runtime.held_contexts() == 0).await);
}

#[tokio::test]
async fn a_callout_that_times_out_counts_as_a_failure() {
    init().await;
    let (origin, _) = echo_origin().await;

    let res = get(6411, "/", origin.addr().port(), &[]).await;

    assert_eq!(res.status(), 418);
    let failure = "wasm_callout_failures_total{failure=\"timeout\",plugin=\"relay-metric\"} 1";
    assert!(eventually(|| metrics_output().contains(failure)).await);
}
