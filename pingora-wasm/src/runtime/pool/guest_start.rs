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

//! The start of a guest: its root context, `proxy_on_vm_start`, and `proxy_on_configure`.

use super::events::{GuestAddress, RootCallbackLink, SlotIndex};
use super::{GuestPool, Loaded};
use crate::callout::{AcceptedCallout, GuestCalloutService};
use crate::root_callbacks::RootStream;
use crate::{plugin_failure, plugin_unavailable};
use pingora_error::Result;
use proxy_wasm_host::abi::v0_2_1::{Callback, GuestError};
use std::sync::Arc;

/// A guest that started, with the callouts that its root sent while it started.
pub(super) struct StartedGuest {
    pub(super) loaded: Loaded,
    pub(super) root_callouts: Vec<AcceptedCallout>,
}

/// How a start ended when the guest did not fail.
enum StartOutcome {
    Started,
    Refused(Callback),
}

impl GuestPool {
    /// Build a guest for `slot` and start it.
    ///
    /// The start runs with the root stream state, so the plugin reads the fixed properties in
    /// `proxy_on_configure`.
    pub(super) fn start_guest(&self, slot: usize) -> Result<StartedGuest> {
        let mut guest = self
            .spec
            .build()
            .map_err(|e| plugin_failure(&self.name, "could not be built", e))?;
        // Callout ids are unique only within one guest, so each guest needs its own service
        let callout_service = Arc::new(GuestCalloutService::new(self.callout_conf.clone()));
        let services = guest
            .services()
            .clone()
            .with_callouts(callout_service.clone());
        *guest.services_mut() = services;
        let mut scope = guest.enter(RootStream::new(self.root_callback_plugin.clone()));
        let root = match scope.on_context_create(None) {
            Ok(root) => root,
            Err(e) => return Err(plugin_failure(&self.name, "failed to start", e)),
        };
        let plugin = self.plugin.clone();
        let (outcome, root_callouts) = callout_service.record_callouts(root, || {
            if !scope.on_vm_start(root)? {
                return Ok::<_, GuestError>(StartOutcome::Refused(Callback::VmStart));
            }
            if !scope.on_configure(root, plugin)? {
                return Ok(StartOutcome::Refused(Callback::Configure));
            }
            Ok(StartOutcome::Started)
        });
        let _root_stream = scope.finish();
        match outcome {
            Ok(StartOutcome::Started) => {}
            Ok(StartOutcome::Refused(callback)) => {
                return Err(plugin_unavailable(
                    &self.name,
                    &format!("refused to start in {callback}"),
                ))
            }
            Err(e) => return Err(plugin_failure(&self.name, "failed to start", e)),
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
        let loaded = Loaded::new(guest, root, callout_service, link, held);
        Ok(StartedGuest {
            loaded,
            root_callouts,
        })
    }
}
