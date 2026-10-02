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

//! Guest startup
//!
//! Starting a guest creates its root context, then runs `proxy_on_vm_start` and
//! `proxy_on_configure`.

use super::events::{GuestAddress, RootCallbackLink, SlotIndex};
use super::{GuestPool, Loaded};
use crate::callout::{AcceptedCallout, GuestCalloutService};
use crate::root_callbacks::RootStream;
use crate::{invalid_conf, ERR_INVALID_CONF};
use pingora_error::{Error, Result};
use proxy_wasm_host::abi::v0_2_1::{Callback, GuestError};
use std::sync::Arc;

pub(super) struct StartedGuest {
    pub(super) loaded: Loaded,
    pub(super) root_callouts: Vec<AcceptedCallout>,
}

enum StartOutcome {
    Started,
    Refused(Callback),
}

impl GuestPool {
    fn guest_start_error(&self, detail: &str, cause: GuestError) -> Box<Error> {
        let context = format!("wasm plugin {}: {detail}", self.name);
        Error::because(ERR_INVALID_CONF, context, cause)
    }

    pub(super) fn start_guest(&self, slot: usize) -> Result<StartedGuest> {
        let mut guest = self
            .spec
            .build()
            .map_err(|e| self.guest_start_error("failed to build guest", e))?;
        // Callout ids are only unique within a guest, so every guest gets its own service
        let callout_service = Arc::new(GuestCalloutService::new(self.callout_conf.clone()));
        let services = guest
            .services()
            .clone()
            .with_callouts(callout_service.clone());
        *guest.services_mut() = services;
        // Startup runs under the root stream state, which lets the plugin read the fixed
        // properties from `proxy_on_vm_start` and `proxy_on_configure`
        let mut scope = guest.enter(RootStream::new(self.root_callback_plugin.clone()));
        let root = match scope.on_context_create(None) {
            Ok(root) => root,
            Err(e) => return Err(self.guest_start_error("failed to create root context", e)),
        };
        let plugin = self.plugin.clone();
        let (outcome, root_callouts) = callout_service.record_callouts(root, || {
            let failed_in = |callback| move |e: GuestError| (callback, e);
            if !scope
                .on_vm_start(root)
                .map_err(failed_in(Callback::VmStart))?
            {
                return Ok(StartOutcome::Refused(Callback::VmStart));
            }
            if !scope
                .on_configure(root, plugin)
                .map_err(failed_in(Callback::Configure))?
            {
                return Ok(StartOutcome::Refused(Callback::Configure));
            }
            Ok(StartOutcome::Started)
        });
        let _root_stream = scope.finish();
        match outcome {
            Ok(StartOutcome::Started) => {}
            Ok(StartOutcome::Refused(callback)) => {
                return Err(invalid_conf(format!(
                    "wasm plugin {}: {callback} returned false, guest not started",
                    self.name
                )))
            }
            Err((callback, e)) => {
                let what = format!("{callback} failed, guest not started");
                return Err(self.guest_start_error(&what, e));
            }
        }
        let address = GuestAddress {
            slot: SlotIndex {
                pool_index: self.pool_index,
                slot_index: slot,
            },
            guest: guest.id(),
        };
        let link = RootCallbackLink::new(
            address,
            self.root_callback_plugin.clone(),
            self.root_callback_sender.clone(),
        );
        let held = self.slots[slot].held.clone();
        let loaded = Loaded::new(self.name.clone(), guest, root, callout_service, link, held);
        Ok(StartedGuest {
            loaded,
            root_callouts,
        })
    }
}
