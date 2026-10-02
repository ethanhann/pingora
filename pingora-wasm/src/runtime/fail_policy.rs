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

/// What happens to a request when one of its plugins fails.
///
/// Each plugin has its own policy, set in
/// [WasmPluginConf::fail_policy](crate::WasmPluginConf::fail_policy), which describes both
/// policies in full. A guest that can no longer be used is replaced under either policy.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FailPolicy {
    /// The request fails.
    #[default]
    Closed,
    /// The failure is logged, the plugin is skipped for the rest of the request, and the request
    /// continues without it.
    Open,
}

impl FailPolicy {
    /// Return the policy as a lowercase string, `closed` or `open`.
    ///
    /// `Display` writes the same string.
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
