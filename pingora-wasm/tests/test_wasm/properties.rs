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

//! The properties that plugins read.

use super::{get, header};
use crate::utils::{echo_origin, eventually, guest_lines, init};

#[tokio::test]
async fn a_plugin_reads_the_request_properties() {
    init().await;
    let (origin, _) = echo_origin().await;

    let res = get(6406, "/a/b?c=d", origin.addr().port(), &[]).await;

    let echoed = |name: &str| header(&res, &format!("x-echo-{name}"));
    let cases = [
        ("x-path", "/a/b?c=d".to_string()),
        ("x-method", "GET".to_string()),
        ("x-protocol", "HTTP/1.1".to_string()),
        ("x-scheme", "http".to_string()),
        ("x-host", "127.0.0.1:6406".to_string()),
        ("x-destination", "127.0.0.1:6406".to_string()),
        ("x-route", "test-route".to_string()),
        ("x-node", "test-node".to_string()),
    ];
    for (name, expected) in cases {
        assert_eq!(echoed(name), Some(expected), "{name}");
    }
    let source = echoed("x-source").unwrap();
    assert!(source.starts_with("127.0.0.1:"), "{source}");
}

#[tokio::test]
async fn a_plugin_reads_the_upstream_address_and_the_response_code() {
    init().await;
    let (origin, _) = echo_origin().await;
    let port = origin.addr().port();

    let res = get(6407, "/", port, &[]).await;

    assert_eq!(
        header(&res, "x-upstream"),
        Some(format!("127.0.0.1:{port}"))
    );
    let code = String::from_utf8_lossy(&200_i64.to_le_bytes()).into_owned();
    let logged = |line: &String| line.starts_with("response-properties ") && line.ends_with(&code);
    assert!(eventually(|| guest_lines().iter().any(logged)).await);
}

#[tokio::test]
async fn a_plugin_reads_the_sizes_in_proxy_on_log() {
    init().await;
    let (origin, _) = echo_origin().await;

    let res = get(6408, "/", origin.addr().port(), &[]).await;
    let body_size = res.bytes().await.unwrap().len();

    let request_size = String::from_utf8_lossy(&0_i64.to_le_bytes()).into_owned();
    let response_size = String::from_utf8_lossy(&(body_size as i64).to_le_bytes()).into_owned();
    let logged = |size: &str| {
        let lines = guest_lines();
        lines
            .iter()
            .any(|line| line.starts_with("logging-properties ") && line.ends_with(size))
    };
    assert!(eventually(|| logged(&request_size) && logged(&response_size)).await);
}
