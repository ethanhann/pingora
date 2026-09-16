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

#[derive(Debug)]
pub enum WasmError {
    Wasmtime(wasmtime::Error),
    Io(std::io::Error),
}

impl From<wasmtime::Error> for WasmError {
    fn from(err: wasmtime::Error) -> Self {
        WasmError::Wasmtime(err)
    }
}

impl From<std::io::Error> for WasmError {
    fn from(err: std::io::Error) -> Self {
        WasmError::Io(err)
    }
}

impl std::fmt::Display for WasmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WasmError::Wasmtime(err) => write!(f, "Wasmtime error: {}", err),
            WasmError::Io(err) => write!(f, "IO error: {}", err),
        }
    }
}

impl std::error::Error for WasmError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WasmError::Wasmtime(err) => Some(err.as_ref()),
            WasmError::Io(err) => Some(err),
        }
    }
}
