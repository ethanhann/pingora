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

//! Plugin fail policy

use serde::Deserialize;
use std::fmt;

/// What happens to a request or a TCP connection when one of its plugins fails.
///
/// Each plugin has its own policy, set in
/// [WasmPluginConf::fail_policy](crate::WasmPluginConf::fail_policy). A plugin fails when it:
///
/// - traps or returns an error in a callback, including `proxy_on_http_call_response`
/// - has no guest in any of its slots when the request starts
/// - loses the guest that held the request's context, e.g. to a trap in another request
/// - pauses on request headers, response headers, response trailers, or the last chunk of a
///   body with no callout to wait for
/// - waits for callouts longer than
///   [callout_wait_limit](crate::WasmPluginConf::callout_wait_limit)
///
/// Some failures fail the request under both policies:
///
/// - A failure before the end of a body the plugin changed. The rest of the body would go out
///   without the plugin's changes.
/// - More held body bytes than
///   [request_body_limit](crate::WasmPluginConf::request_body_limit) or
///   [response_body_limit](crate::WasmPluginConf::response_body_limit) allows. Otherwise a
///   client could bypass the plugin by padding its request.
/// - A response sent after the response header. It can no longer replace the response that
///   has already started.
/// - Any filter that runs after an earlier filter of the request was cancelled during a
///   callout wait, since the plugins may have been left halfway through that filter.
///
/// A plugin on a connection of [WasmTcpProxy](crate::WasmTcpProxy) fails as in the first list,
/// and its pause with no callout to wait for is a pause of `proxy_on_new_connection`, or of its
/// data once neither side can send more bytes. Where a failure would fail a request, it closes
/// both sides of the connection instead, and a failure before the end of a direction whose data
/// the plugin changed closes the connection under both policies.
///
/// A plugin that cannot start fails [WasmRuntime::new](crate::WasmRuntime::new) under both
/// policies.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FailPolicy {
    /// The request fails, or the TCP connection is closed.
    ///
    /// The filter returns [ERR_PLUGIN_FAILED](crate::ERR_PLUGIN_FAILED), and Pingora responds
    /// with 503 or, once the response header has been sent, ends the response early.
    #[default]
    Closed,
    /// The failure is logged, and the plugin is skipped for the rest of the request or
    /// connection, which continues with the next plugin.
    Open,
}

impl FailPolicy {
    /// Return the policy as a lowercase string, `closed` or `open`.
    pub fn as_str(&self) -> &'static str {
        match self {
            FailPolicy::Closed => "closed",
            FailPolicy::Open => "open",
        }
    }
}

impl fmt::Display for FailPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
