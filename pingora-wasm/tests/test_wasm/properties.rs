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

//! Property tests
//!
//! Covers the properties a plugin can read in the request, response, and logging phases.

use super::{get, header};
use crate::utils::{echo_origin, eventually, guest_messages, init};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn decode_i64(bytes: &[u8]) -> Option<i64> {
    Some(i64::from_le_bytes(bytes.try_into().ok()?))
}

#[tokio::test]
async fn plugin_reads_request_properties() {
    init().await;
    let (origin, _) = echo_origin().await;

    let res = get(6406, "/a/b?c=d", origin.addr().port(), &[]).await;

    let echoed = |name: &str| header(&res, &format!("x-echo-{name}"));
    let cases = [
        ("x-path", "/a/b?c=d"),
        ("x-url-path", "/a/b"),
        ("x-method", "GET"),
        ("x-protocol", "HTTP/1.1"),
        ("x-scheme", "http"),
        ("x-host", "127.0.0.1:6406"),
        ("x-destination", "127.0.0.1:6406"),
        ("x-route", "test-route"),
        ("x-node", "test-node"),
    ];
    for (name, expected) in cases {
        assert_eq!(echoed(name).as_deref(), Some(expected), "{name}");
    }
    let source = echoed("x-source").unwrap();
    assert!(source.starts_with("127.0.0.1:"), "{source}");
    assert!(eventually(|| guest_messages("request-properties").len() >= 2).await);
    let logged = guest_messages("request-properties");
    let time = Duration::from_nanos(
        decode_i64(&logged[logged.len() - 1])
            .unwrap()
            .try_into()
            .unwrap(),
    );
    let age = SystemTime::now().duration_since(UNIX_EPOCH).unwrap() - time;
    assert_eq!(decode_i64(&logged[logged.len() - 2]), Some(6406));
    assert!(age < Duration::from_secs(5), "{age:?}");
}

#[tokio::test]
async fn response_phase_reads_upstream_address_and_response_code() {
    init().await;
    let (origin, _) = echo_origin().await;
    let port = origin.addr().port();

    let res = get(6407, "/", port, &[]).await;

    assert_eq!(
        header(&res, "x-upstream"),
        Some(format!("127.0.0.1:{port}"))
    );
    let logged = guest_messages("response-properties");
    let logged: Vec<_> = logged.iter().map(|value| decode_i64(value)).collect();
    assert_eq!(
        logged[logged.len() - 2..],
        [Some(200), Some(i64::from(port))]
    );
}

#[tokio::test]
async fn proxy_on_log_reads_sizes_and_duration() {
    init().await;
    let (origin, _) = echo_origin().await;

    let res = get(6408, "/", origin.addr().port(), &[]).await;
    let body_size = i64::try_from(res.bytes().await.unwrap().len()).unwrap();

    assert!(eventually(|| guest_messages("logging-properties").len() >= 3).await);
    let logged = guest_messages("logging-properties");
    let logged: Vec<_> = logged.iter().map(|value| decode_i64(value)).collect();
    let [request_size, response_size, duration] = logged[logged.len() - 3..] else {
        unreachable!("a slice of three values always matches");
    };
    assert_eq!((request_size, response_size), (Some(0), Some(body_size)));
    assert!(duration.is_some_and(|nanos| nanos > 0), "{duration:?}");
}
