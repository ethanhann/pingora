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

#[derive(Debug, Clone)]
pub struct WasmConfig {
    /// Maximum concurrent WASM device hook executions (sizes the wasmtime pool).
    pub max_concurrent_executions: u32,
    /// Per-execution linear memory ceiling, in bytes.
    pub max_memory_bytes: usize,
}

impl Default for WasmConfig {
    fn default() -> Self {
        Self {
            max_concurrent_executions: 512,
            max_memory_bytes: 64 * 1024 * 1024, // 64 MiB
        }
    }
}
