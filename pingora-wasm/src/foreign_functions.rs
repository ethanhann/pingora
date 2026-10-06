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

//! Foreign functions

use proxy_wasm_host::abi::v0_2_1::types::Status;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

type ForeignFunction =
    dyn Fn(&WasmForeignCall<'_>) -> Result<Vec<u8>, WasmForeignFunctionError> + Send + Sync;

/// Functions of your proxy that plugins call by name with `proxy_call_foreign_function`.
///
/// A plugin and your proxy agree on the name of a function and on the bytes of its arguments and
/// its result. A plugin that calls a name you did not insert gets `NOT_FOUND`. A function runs
/// inside the plugin's callback, so it must not block. Set the functions as
/// [WasmServices::foreign_functions](crate::WasmServices::foreign_functions).
///
/// ```
/// use pingora_wasm::{WasmForeignFunctionError, WasmForeignFunctions};
///
/// let mut functions = WasmForeignFunctions::new();
/// functions.insert("reverse", |call| {
///     if call.arguments.is_empty() {
///         return Err(WasmForeignFunctionError::BadArgument);
///     }
///     Ok(call.arguments.iter().rev().copied().collect())
/// });
/// ```
#[derive(Clone, Default)]
pub struct WasmForeignFunctions {
    functions: HashMap<String, Arc<ForeignFunction>>,
}

impl WasmForeignFunctions {
    /// Create an empty set of functions.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add `function` under `name`, and return whether it replaced a function with that name.
    pub fn insert(
        &mut self,
        name: impl Into<String>,
        function: impl Fn(&WasmForeignCall<'_>) -> Result<Vec<u8>, WasmForeignFunctionError>
            + Send
            + Sync
            + 'static,
    ) -> bool {
        self.functions
            .insert(name.into(), Arc::new(function))
            .is_some()
    }

    pub(crate) fn call(
        &self,
        plugin_name: &str,
        name: &[u8],
        arguments: &[u8],
        out: &mut Vec<u8>,
    ) -> Result<(), Status> {
        let function = std::str::from_utf8(name)
            .ok()
            .and_then(|name| self.functions.get(name))
            .ok_or(Status::NotFound)?;
        let call = WasmForeignCall::new(plugin_name, arguments);
        *out = function(&call).map_err(Status::from)?;
        Ok(())
    }
}

impl fmt::Debug for WasmForeignFunctions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut names: Vec<&String> = self.functions.keys().collect();
        names.sort();
        f.debug_struct("WasmForeignFunctions")
            .field("names", &names)
            .finish()
    }
}

/// One call of a foreign function by a plugin.
#[non_exhaustive]
#[derive(Debug)]
pub struct WasmForeignCall<'a> {
    /// The name of the plugin that made the call.
    pub plugin_name: &'a str,
    /// The arguments, in the encoding that the plugin and your proxy agree on.
    pub arguments: &'a [u8],
}

impl<'a> WasmForeignCall<'a> {
    /// Create a call, e.g. to test your own function.
    pub fn new(plugin_name: &'a str, arguments: &'a [u8]) -> Self {
        WasmForeignCall {
            plugin_name,
            arguments,
        }
    }
}

/// The error a foreign function returns to the plugin.
///
/// These are the statuses that a plugin of the Rust SDK accepts from `proxy_call_foreign_function`.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WasmForeignFunctionError {
    /// The plugin gets `NOT_FOUND`.
    NotFound,
    /// The plugin gets `BAD_ARGUMENT`.
    BadArgument,
    /// The plugin gets `SERIALIZATION_FAILURE`.
    SerializationFailure,
    /// The plugin gets `INTERNAL_FAILURE`.
    InternalFailure,
}

impl fmt::Display for WasmForeignFunctionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            WasmForeignFunctionError::NotFound => "not found",
            WasmForeignFunctionError::BadArgument => "bad argument",
            WasmForeignFunctionError::SerializationFailure => "serialization failure",
            WasmForeignFunctionError::InternalFailure => "internal failure",
        })
    }
}

impl std::error::Error for WasmForeignFunctionError {}

impl From<WasmForeignFunctionError> for Status {
    fn from(error: WasmForeignFunctionError) -> Self {
        match error {
            WasmForeignFunctionError::NotFound => Status::NotFound,
            WasmForeignFunctionError::BadArgument => Status::BadArgument,
            WasmForeignFunctionError::SerializationFailure => Status::SerializationFailure,
            WasmForeignFunctionError::InternalFailure => Status::InternalFailure,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn call_runs_function_by_name() {
        let mut functions = WasmForeignFunctions::new();
        functions.insert("echo", |call| {
            Ok([call.plugin_name.as_bytes(), b":", call.arguments].concat())
        });
        functions.insert("refuse", |_| Err(WasmForeignFunctionError::NotFound));
        let replaced = functions.insert("refuse", |_| Err(WasmForeignFunctionError::BadArgument));
        assert!(replaced);
        let names: [&[u8]; 4] = [b"echo", b"refuse", b"missing", b"\xff"];

        let got = names.map(|name| {
            let mut out = Vec::new();
            functions
                .call("authz", name, b"args", &mut out)
                .map(|()| out)
        });

        let want = [
            Ok(b"authz:args".to_vec()),
            Err(Status::BadArgument),
            Err(Status::NotFound),
            Err(Status::NotFound),
        ];
        assert_eq!(got, want);
    }
}
