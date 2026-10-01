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

mod utils;

#[path = "test_wasm/properties.rs"]
mod properties;
#[path = "test_wasm/root_callbacks.rs"]
mod root_callbacks;

use std::sync::atomic::Ordering;
use std::time::Duration;
use utils::raw::{
    decode_chunked_body, post_on_one_connection, send_chunked_request, send_get_without_reading,
};
use utils::{
    callout_origin, client, closing_peer, echo_origin, eventually, guest_lines, init, runtime, url,
};

fn header(response: &reqwest::Response, name: &str) -> Option<String> {
    response
        .headers()
        .get(name)
        .map(|v| v.to_str().unwrap().to_string())
}

fn all(response: &reqwest::Response, name: &str) -> Vec<String> {
    response
        .headers()
        .get_all(name)
        .iter()
        .map(|v| v.to_str().unwrap().to_string())
        .collect()
}

async fn get(port: u16, path: &str, origin: u16, extra: &[(&str, &str)]) -> reqwest::Response {
    let mut request = client()
        .get(url(port, path))
        .header("x-test-origin", origin.to_string());
    for (name, value) in extra {
        request = request.header(*name, *value);
    }
    request.send().await.unwrap()
}

#[tokio::test]
async fn a_plugin_adds_a_request_header() {
    init().await;
    let (origin, _) = echo_origin().await;

    let res = get(6380, "/", origin.addr().port(), &[]).await;

    assert_eq!(res.status(), 200);
    let context = header(&res, "x-echo-wasm-context").unwrap();
    assert!(context.parse::<u32>().is_ok(), "{context}");
}

#[tokio::test]
async fn a_chain_runs_each_plugin_on_the_request() {
    init().await;
    let (origin, _) = echo_origin().await;

    let res = get(6381, "/", origin.addr().port(), &[]).await;

    assert_eq!(res.status(), 200);
    assert!(header(&res, "x-echo-wasm-context").is_some());
    assert!(header(&res, "x-echo-x-proxy-wasm").is_some());
}

#[tokio::test]
async fn a_plugin_adds_a_response_header_from_its_configuration() {
    init().await;
    let (origin, _) = echo_origin().await;

    let res = get(6382, "/", origin.addr().port(), &[]).await;

    assert_eq!(res.status(), 200);
    assert_eq!(all(&res, "custom-header"), ["hello"]);
}

#[tokio::test]
async fn a_plugin_sends_its_own_response() {
    init().await;
    let (origin, count) = echo_origin().await;

    let res = get(6383, "/hello", origin.addr().port(), &[]).await;

    assert_eq!(res.status(), 200);
    assert_eq!(header(&res, "hello").as_deref(), Some("World"));
    assert_eq!(header(&res, "powered-by").as_deref(), Some("proxy-wasm"));
    assert_eq!(res.text().await.unwrap(), "Hello, World!\n");
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert!(
        eventually(|| guest_lines().iter().any(|l| l.contains("<- hello: World"))).await,
        "the sender did not see its own local response"
    );
}

#[tokio::test]
async fn a_denied_request_never_reaches_the_origin() {
    init().await;
    let (origin, count) = echo_origin().await;

    let res = get(6388, "/", origin.addr().port(), &[("x-deny", "1")]).await;

    assert_eq!(res.status(), 403);
    assert_eq!(header(&res, "x-denied").as_deref(), Some("yes"));
    assert_eq!(res.text().await.unwrap(), "denied");
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert!(eventually(|| runtime(6388).open_contexts() == 0).await);
}

#[tokio::test]
async fn a_trap_responds_with_503_and_the_next_request_succeeds() {
    init().await;
    let (origin, _) = echo_origin().await;

    let trapped = get(6384, "/", origin.addr().port(), &[("x-trap", "1")]).await;
    let next = get(6384, "/", origin.addr().port(), &[]).await;

    assert_eq!(trapped.status(), 503);
    assert_eq!(next.status(), 200);
    assert!(header(&next, "x-echo-x-proxy-wasm").is_some());
}

#[tokio::test]
async fn parallel_requests_share_a_pool() {
    init().await;
    let (origin, _) = echo_origin().await;
    let port = origin.addr().port();

    let responses = futures::future::join_all((0..64).map(|_| get(6385, "/", port, &[]))).await;

    assert!(responses.iter().all(|r| r.status() == 200));
    assert!(responses
        .iter()
        .all(|r| header(r, "x-echo-x-proxy-wasm").is_some()));
    assert!(eventually(|| runtime(6385).open_contexts() == 0).await);
}

#[tokio::test]
async fn a_guest_reads_authority_and_never_sees_host() {
    init().await;
    let (origin, _) = echo_origin().await;

    let res = get(6381, "/", origin.addr().port(), &[("host", "example.test")]).await;

    assert_eq!(res.status(), 200);
    assert!(
        eventually(|| guest_lines()
            .iter()
            .any(|l| l.ends_with(":authority -> example.test")))
        .await
    );
    assert!(!guest_lines().iter().any(|l| l.contains(": host -> ")));
}

#[tokio::test]
async fn the_response_phase_runs_in_reverse_order() {
    init().await;
    let (origin, _) = echo_origin().await;

    let res = get(6386, "/", origin.addr().port(), &[]).await;

    assert_eq!(all(&res, "custom-header"), ["b", "a"]);
}

#[tokio::test]
async fn a_local_response_passes_through_the_earlier_plugins() {
    init().await;
    let (origin, _) = echo_origin().await;

    let res = get(6387, "/", origin.addr().port(), &[("x-deny", "1")]).await;

    assert_eq!(res.status(), 403);
    assert_eq!(all(&res, "custom-header"), ["hello"]);
}

#[tokio::test]
async fn two_chains_share_the_guest_of_one_runtime() {
    init().await;
    let (origin, _) = echo_origin().await;
    let port = origin.addr().port();

    let first = get(6389, "/", port, &[]).await;
    let second = get(6390, "/", port, &[]).await;

    let first: u32 = header(&first, "x-echo-wasm-context")
        .unwrap()
        .parse()
        .unwrap();
    let second: u32 = header(&second, "x-echo-wasm-context")
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(second, first + 1);
}

#[tokio::test]
async fn request_bodies_survive_on_a_keep_alive_connection() {
    init().await;
    let (origin, count) = echo_origin().await;
    let client = client();
    let post = |body: &'static str| {
        client
            .post(url(6380, "/"))
            .header("x-test-origin", origin.addr().port().to_string())
            .body(body)
            .send()
    };

    let first = post("hello world").await.unwrap();
    let second = post("hello, world!").await.unwrap();

    assert_eq!(first.status(), 200);
    assert_eq!(second.status(), 200);
    assert_eq!(header(&first, "x-echo-body-len").as_deref(), Some("11"));
    assert_eq!(header(&second, "x-echo-body-len").as_deref(), Some("13"));
    assert_eq!(count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_plugin_replaces_a_response_body_and_the_connection_stays_open() {
    init().await;
    let (origin, _) = echo_origin().await;
    let bodies = ["a secret", "public"];

    let responses = post_on_one_connection(6391, origin.addr().port(), &bodies).await;

    let [secret, public] = &responses[..] else {
        panic!("{} responses", responses.len());
    };
    assert_eq!(secret.status, 200);
    assert!(secret.head.contains("\r\ntransfer-encoding: chunked"));
    assert!(!secret.head.contains("\r\ncontent-length:"));
    assert_eq!(
        decode_chunked_body(&secret.body),
        "Original message body (8 bytes) redacted.\n"
    );
    assert_eq!(public.status, 200);
    assert_eq!(decode_chunked_body(&public.body), "public");
}

#[tokio::test]
async fn a_plugin_holds_a_request_body_until_its_end() {
    init().await;
    let (origin, _) = echo_origin().await;
    let chunks = ["one ", "two ", "three"];

    let res = send_chunked_request(6392, origin.addr().port(), "POST", &[], &chunks).await;

    assert_eq!(res.status, 200);
    assert_eq!(res.body, "aone two three");
}

#[tokio::test]
async fn a_held_request_body_over_its_limit_responds_with_413() {
    init().await;
    let (origin, count) = echo_origin().await;
    let chunks = ["twenty bytes of body"];

    let res = send_chunked_request(6393, origin.addr().port(), "POST", &[], &chunks).await;

    assert_eq!(res.status, 413);
    assert_eq!(count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_plugin_responds_in_place_of_the_upstream_response() {
    init().await;
    let (origin, count) = echo_origin().await;

    let res = get(6394, "/", origin.addr().port(), &[]).await;

    assert_eq!(res.status(), 418);
    assert_eq!(res.text().await.unwrap(), "teapot");
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_plugin_responds_to_a_request_body() {
    init().await;
    let (origin, _) = echo_origin().await;

    let res = send_chunked_request(6395, origin.addr().port(), "POST", &[], &["attack"]).await;

    assert_eq!(res.status, 418);
    assert_eq!(res.body, "teapot");
    assert!(eventually(|| runtime(6395).open_contexts() == 0).await);
}

#[tokio::test]
async fn a_retry_sends_the_output_of_the_plugins() {
    init().await;
    let (origin, count) = echo_origin().await;
    let (first, peer) = closing_peer().await;
    let first = first.to_string();
    let headers = [("x-test-first-origin", first.as_str())];

    let res = send_chunked_request(6396, origin.addr().port(), "PUT", &headers, &["x", "y"]).await;

    // The plugin marks the two chunks and the empty call that ends the body. If it ran on the
    // bytes of the retry, it would mark them once.
    peer.await.unwrap();
    assert_eq!(res.status, 200);
    assert_eq!(res.body, "axaya");
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_plugin_response_with_no_body_ends_an_h2_stream() {
    init().await;
    let (origin, _) = echo_origin().await;
    let client = reqwest::Client::builder()
        .no_proxy()
        .http2_prior_knowledge()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    let res = client
        .head(url(6394, "/"))
        .header("x-test-origin", origin.addr().port().to_string())
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 418);
    assert_eq!(res.version(), reqwest::Version::HTTP_2);
    assert_eq!(res.bytes().await.unwrap().len(), 0);
}

#[tokio::test]
async fn a_plugin_allows_a_request_after_its_callout() {
    init().await;
    let (origin, _) = echo_origin().await;

    let res = get(6397, "/", origin.addr().port(), &[]).await;

    assert_eq!(res.status(), 200);
    assert_eq!(all(&res, "powered-by"), ["proxy-wasm"]);
    let callouts = callout_origin("auth-even").requests();
    assert_eq!(
        callouts,
        [("/bytes/1".to_string(), "httpbin.org".to_string())]
    );
}

#[tokio::test]
async fn a_plugin_denies_a_request_after_its_callout() {
    init().await;
    let (origin, count) = echo_origin().await;

    let res = get(6398, "/", origin.addr().port(), &[]).await;

    assert_eq!(res.status(), 403);
    assert_eq!(all(&res, "powered-by"), ["proxy-wasm"]);
    assert_eq!(res.text().await.unwrap(), "Access forbidden.\n");
    assert_eq!(count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_callout_ends_at_the_timeout_of_its_plugin() {
    init().await;
    let (origin, count) = echo_origin().await;
    // The plugin passes 1 second, and the limit of its callouts is 10 seconds
    let before_the_limit = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let request = before_the_limit
        .get(url(6399, "/"))
        .header("x-test-origin", origin.addr().port().to_string());

    let res = request.send().await.unwrap();

    assert_eq!(res.status(), 403);
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert_eq!(callout_origin("auth-no-response").requests().len(), 1);
}

#[tokio::test]
async fn a_callout_ends_at_the_limit_with_a_response_for_its_plugin() {
    init().await;
    let (origin, _) = echo_origin().await;

    let res = get(6400, "/", origin.addr().port(), &[]).await;

    assert_eq!(res.status(), 418);
    assert_eq!(res.text().await.unwrap(), "upstream request timeout");
}

#[tokio::test]
async fn a_request_whose_h1_client_left_ends_with_its_callout() {
    init().await;
    let (origin, _) = echo_origin().await;
    let connection = send_get_without_reading(6401, origin.addr().port()).await;
    callout_origin("relay-h1-close").wait_for_a_request().await;

    drop(connection);

    let runtime = runtime(6401);
    assert!(eventually(|| runtime.open_contexts() == 0).await);
    assert!(eventually(|| runtime.callouts_in_flight() == 0).await);
}

#[tokio::test]
async fn a_request_whose_h2_client_left_ends_before_its_callout() {
    init().await;
    let (origin, _) = echo_origin().await;
    let h2 = reqwest::Client::builder()
        .no_proxy()
        .http2_prior_knowledge()
        .build()
        .unwrap();
    let request = h2
        .get(url(6402, "/"))
        .header("x-test-origin", origin.addr().port().to_string())
        .send();
    let request = tokio::spawn(request);
    callout_origin("relay-h2-close").wait_for_a_request().await;

    request.abort();

    // The origin never responds, so the callout is in flight until its limit of 10 seconds
    assert!(eventually(|| runtime(6402).open_contexts() == 0).await);
}
