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

mod config;
mod engine;
mod error;

pub fn bootstrap() {
    todo!("load the engine and hook it into filters")

    // Todo:
    // Need to create the WASM engine - the engine should not be instantiated per-request, and ideally not per-filter.
    // Need to hook the engine into at least one filter.
    // Need to expose a module loading mechanism
}
