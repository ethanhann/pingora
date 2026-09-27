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

mod logging;

use crate::logging::TracingSink;
use proxy_wasm_host::abi::v0_2_1::{GuestSpec, Host, VmServices};
use proxy_wasm_host::{Engine, Limits, Module};
use std::sync::Arc;

/// The plugin this example runs when no path is given.
const DEFAULT_GUEST: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/add-request-header.wasm"
);

pub fn bootstrap() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Todo:
    // Need to create the WASM host.
    // Need to hook the engine into at least one filter.
    // Need to expose a module loading mechanism

    let path = std::env::args().nth(1).unwrap_or(DEFAULT_GUEST.to_owned());
    let bytes = std::fs::read(&path)?;
    let engine = Engine::new()?;
    let module = Module::new(&engine, &bytes)?;
    let services = VmServices::new(Arc::new(TracingSink)).with_vm_id(*b"example");
    let spec = GuestSpec::new(&Host::new(&engine)?, &module, services, &Limits::default())?;
    Ok(())
}
