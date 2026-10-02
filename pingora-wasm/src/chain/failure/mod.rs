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

//! Plugin failures and the fail policy
//!
//! A filter describes each plugin failure as a [FilterFailure]. Most failures go through
//! `skip_plugin_or_fail_request`, which applies the plugin's fail policy and either skips the
//! plugin for the rest of the request or fails the request. Failures that fail the request under
//! both policies go through `failed_request_error`.

mod policy;

pub(crate) use policy::FailPolicyRecord;

use super::body::BodyDirection;
use crate::observability::PluginFailure;
use crate::ERR_PLUGIN_FAILED;
use pingora_error::{Error, ErrorType};
use proxy_wasm_host::abi::v0_2_1::{Callback, GuestError};
use std::time::Duration;

/// A plugin failure in a filter, before the plugin's fail policy is applied.
pub(in crate::chain) struct FilterFailure {
    pub(super) kind: PluginFailure,
    pub(super) callback: Option<Callback>,
    /// The part of the error or log message after the plugin name.
    pub(super) detail: String,
    pub(super) cause: Option<GuestError>,
    error_type: ErrorType,
}

impl FilterFailure {
    fn new(kind: PluginFailure, callback: Option<Callback>, detail: impl Into<String>) -> Self {
        FilterFailure {
            kind,
            callback,
            detail: detail.into(),
            cause: None,
            error_type: ERR_PLUGIN_FAILED,
        }
    }

    pub(in crate::chain) fn guest_error(callback: Callback, cause: GuestError) -> Self {
        let detail = format!("{callback} failed");
        FilterFailure {
            cause: Some(cause),
            ..Self::new(PluginFailure::GuestError, Some(callback), detail)
        }
    }

    pub(in crate::chain) fn unavailable() -> Self {
        Self::new(PluginFailure::Unavailable, None, "no slot has a guest")
    }

    pub(in crate::chain) fn guest_lost(slot: usize, callback: Callback) -> Self {
        let detail = format!("guest in slot {slot} lost before {callback}");
        Self::new(PluginFailure::GuestLost, None, detail)
    }

    pub(in crate::chain) fn paused(callback: Callback, detail: &str) -> Self {
        Self::new(PluginFailure::PausedWithoutCallout, Some(callback), detail)
    }

    pub(in crate::chain) fn wait_limit(callback: Callback, limit: Duration) -> Self {
        let detail = format!("callout wait in {callback} exceeded callout_wait_limit {limit:?}");
        Self::new(PluginFailure::WaitLimit, Some(callback), detail)
    }

    pub(in crate::chain) fn body_limit(
        direction: BodyDirection,
        size: usize,
        limit: usize,
    ) -> Self {
        let (body, limit_name) = direction.body_and_limit_names();
        let detail = format!("{size} held {body} body bytes exceed {limit_name} {limit}");
        FilterFailure {
            error_type: direction.too_large(),
            ..Self::new(PluginFailure::BodyLimit, Some(direction.callback()), detail)
        }
    }

    pub(in crate::chain) fn cancelled_wait() -> Self {
        let detail = "request aborted, an earlier filter was cancelled during a callout wait";
        Self::new(PluginFailure::CancelledWait, None, detail)
    }

    pub(in crate::chain) fn late_response(callback: Callback) -> Self {
        let detail = "response rejected, sent after the response header";
        Self::new(PluginFailure::LateResponse, Some(callback), detail)
    }

    pub(super) fn into_error(self, plugin_name: &str) -> Box<Error> {
        let context = format!("wasm plugin {plugin_name}: {}", self.detail);
        match self.cause {
            Some(cause) => Error::because(self.error_type, context, cause),
            None => Error::explain(self.error_type, context),
        }
    }
}
