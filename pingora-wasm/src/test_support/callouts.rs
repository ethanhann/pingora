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

//! Callouts for tests: the callbacks of the guests that make them, and a sender that returns a
//! fixed result.

use crate::callout::{AcceptedCallout, CalloutResult, CalloutSender, StaticCalloutUpstreams};
use crate::{WasmCtx, WasmPluginConf, WasmRuntime, WasmServices};
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use pingora_core::upstreams::peer::HttpPeer;
use std::sync::Arc;
use tokio::sync::Notify;

/// A callback that makes a callout to the upstream `authz`, and pauses.
pub(crate) const CALL_AND_PAUSE: &str = "(call $call_authz_and_pause)";
pub(crate) const CALL_TWICE_AND_PAUSE: &str =
    "(drop (call $call_authz_and_pause)) (call $call_authz_and_pause)";
pub(crate) const CALL_WITHOUT_PAUSE: &str = "(call $call_without_pause)";
/// A callback that makes a callout, writes the log line `accepted` or `refused` for its status,
/// and returns 1.
pub(crate) const CALL_AND_LOG_STATUS: &str = "(call $call_and_log_status) (i32.const 1)";
/// A callback that sends 418 when the request has the header `x-asked`.
pub(crate) const TEAPOT_IF_ASKED: &str = "(call $teapot_if_asked)";
pub(crate) const CALL_AND_TRAP: &str = "(drop (call $call_authz_and_pause)) unreachable";
pub(crate) const CONTINUE_REQUEST_AND_PAUSE: &str = "(call $continue_and_pause (i32.const 0))";
pub(crate) const CONTINUE_RESPONSE_AND_PAUSE: &str = "(call $continue_and_pause (i32.const 1))";

/// Bodies of `proxy_on_http_call_response`.
pub(crate) const CONTINUE_REQUEST: &str = "(call $continue (i32.const 0))";
pub(crate) const CONTINUE_RESPONSE: &str = "(call $continue (i32.const 1))";
pub(crate) const CONTINUE_REQUEST_ON_SECOND_DELIVERY: &str =
    "(call $continue_on_second (i32.const 0))";
pub(crate) const MARK_ASKED_AND_CONTINUE: &str =
    "(call $mark_asked) (call $continue (i32.const 0))";
pub(crate) const RELAY_CALLOUT_BODY: &str =
    "(call $relay_callout_body (local.get 2) (local.get 3))";
pub(crate) const STAY_PAUSED: &str = "";
pub(crate) const LOG_RESULT: &str = "(call $log_result (local.get 2))";
pub(crate) const CALL_WITH_NO_RESULT: &str = "(drop (call $call_authz_and_pause))";

/// A sender that returns one result for every callout.
pub(crate) struct FixedSender {
    result: Option<CalloutResult>,
    /// The upstream and the path of each callout that arrived.
    pub(crate) sent: Mutex<Vec<(String, String)>>,
    /// When set, a callout waits here before it returns its result.
    gate: Option<Arc<Notify>>,
}

impl FixedSender {
    /// Create a sender whose callouts receive 200 with `body`.
    pub(crate) fn responds(body: &'static str) -> Arc<Self> {
        let result = CalloutResult::Response {
            headers: vec![(b":status".to_vec(), b"200".to_vec())],
            body: Bytes::from_static(body.as_bytes()),
            trailers: Vec::new(),
        };
        Self::with_result_and_gate(Some(result), None)
    }

    /// Create a sender whose tasks panic.
    pub(crate) fn panics() -> Arc<Self> {
        Self::with_result_and_gate(None, None)
    }

    /// Create a sender whose callouts receive 200 with `body` after `gate` opens.
    pub(crate) fn responds_after(body: &'static str, gate: Arc<Notify>) -> Arc<Self> {
        let sender = Self::responds(body);
        Self::with_result_and_gate(sender.result.clone(), Some(gate))
    }

    fn with_result_and_gate(result: Option<CalloutResult>, gate: Option<Arc<Notify>>) -> Arc<Self> {
        Arc::new(FixedSender {
            result,
            sent: Mutex::new(Vec::new()),
            gate,
        })
    }

    pub(crate) fn sent_count(&self) -> usize {
        self.sent.lock().len()
    }
}

#[async_trait]
impl CalloutSender for FixedSender {
    async fn send(&self, callout: AcceptedCallout) -> CalloutResult {
        let path = String::from_utf8_lossy(callout.request.raw_path()).into_owned();
        self.sent.lock().push((callout.upstream.clone(), path));
        if let Some(gate) = &self.gate {
            gate.notified().await;
        }
        self.result.clone().expect("the sender has no result")
    }
}

/// Return services with the upstream `authz`.
pub(crate) fn authz_services() -> WasmServices {
    let mut upstreams = StaticCalloutUpstreams::new();
    upstreams.insert("authz", HttpPeer::new("127.0.0.1:1", false, String::new()));
    WasmServices {
        callout_upstreams: Arc::new(upstreams),
        ..WasmServices::default()
    }
}

/// Build a runtime whose callouts go to `sender`, and a request context from a chain of
/// `plugins` in that order.
pub(crate) fn callout_ctx(
    plugins: Vec<WasmPluginConf>,
    sender: Arc<FixedSender>,
) -> (WasmRuntime, WasmCtx) {
    callout_ctx_with_services(plugins, sender, authz_services())
}

pub(crate) fn callout_ctx_with_services(
    plugins: Vec<WasmPluginConf>,
    sender: Arc<FixedSender>,
    services: WasmServices,
) -> (WasmRuntime, WasmCtx) {
    let names: Vec<String> = plugins.iter().map(|p| p.name.clone()).collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    let runtime = WasmRuntime::new_with_callout_sender(plugins, services, sender).unwrap();
    let ctx = runtime.chain(&names).unwrap().new_ctx();
    (runtime, ctx)
}
