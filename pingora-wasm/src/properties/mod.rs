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

//! The properties that plugins read with `proxy_get_property`.

pub(crate) mod built_in;

use std::collections::HashMap;

/// A property value in the encoding that plugins expect.
///
/// A string or bytes stay as they are, a bool is one byte, and an integer is 8 little-endian
/// bytes, as Envoy encodes them. Create one with `From`, for example
/// `WasmPropertyValue::from(8080_u16)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmPropertyValue(Vec<u8>);

impl WasmPropertyValue {
    /// Return the encoded bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl From<&str> for WasmPropertyValue {
    fn from(value: &str) -> Self {
        WasmPropertyValue(value.as_bytes().to_vec())
    }
}

impl From<String> for WasmPropertyValue {
    fn from(value: String) -> Self {
        WasmPropertyValue(value.into_bytes())
    }
}

impl From<&[u8]> for WasmPropertyValue {
    fn from(value: &[u8]) -> Self {
        WasmPropertyValue(value.to_vec())
    }
}

impl From<Vec<u8>> for WasmPropertyValue {
    fn from(value: Vec<u8>) -> Self {
        WasmPropertyValue(value)
    }
}

impl From<bool> for WasmPropertyValue {
    fn from(value: bool) -> Self {
        WasmPropertyValue(vec![u8::from(value)])
    }
}

macro_rules! integer_property_value {
    ($($integer:ty => $wide:ty),*) => {
        $(impl From<$integer> for WasmPropertyValue {
            fn from(value: $integer) -> Self {
                WasmPropertyValue(<$wide>::from(value).to_le_bytes().to_vec())
            }
        })*
    };
}

integer_property_value!(i32 => i64, i64 => i64, u16 => u64, u32 => u64, u64 => u64);

/// A set of property values, by path.
///
/// A path is a list of segments, such as `["node", "metadata", "NAME"]` for the property that a
/// plugin reads as `node.metadata.NAME`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WasmProperties {
    values: HashMap<Vec<u8>, Vec<u8>>,
}

impl WasmProperties {
    /// Create an empty set of properties.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the value of `path`, replacing any previous value.
    pub fn insert(&mut self, path: &[&str], value: impl Into<WasmPropertyValue>) {
        let mut key = Vec::new();
        join_path(path.iter().map(|segment| segment.as_bytes()), &mut key);
        self.values.insert(key, value.into().0);
    }

    /// Return the value of `path`, in the encoding that plugins read.
    pub fn get(&self, path: &[&str]) -> Option<&[u8]> {
        let mut key = Vec::new();
        join_path(path.iter().map(|segment| segment.as_bytes()), &mut key);
        self.values.get(&key).map(Vec::as_slice)
    }

    /// Set the value of the path whose segments are joined in `key`.
    pub(crate) fn insert_joined(&mut self, key: &[u8], value: &[u8]) {
        self.values.insert(key.to_vec(), value.to_vec());
    }

    /// Return the value of the path whose segments are joined in `key`.
    pub(crate) fn get_joined(&self, key: &[u8]) -> Option<&[u8]> {
        self.values.get(key).map(Vec::as_slice)
    }
}

/// Join the segments of a path with `\0` into `key`, as the ABI sends a path.
pub(crate) fn join_path<'a>(segments: impl IntoIterator<Item = &'a [u8]>, key: &mut Vec<u8>) {
    key.clear();
    for (index, segment) in segments.into_iter().enumerate() {
        if index > 0 {
            key.push(0);
        }
        key.extend_from_slice(segment);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_have_the_encoding_of_envoy() {
        let cases: [(WasmPropertyValue, &[u8]); 4] = [
            ("route".into(), b"route"),
            (true.into(), &[1]),
            (8080_u16.into(), &8080_u64.to_le_bytes()),
            ((-1_i32).into(), &(-1_i64).to_le_bytes()),
        ];

        for (value, expected) in cases {
            assert_eq!(value.as_bytes(), expected);
        }
    }

    #[test]
    fn a_value_is_found_by_its_segments_and_by_its_joined_path_but_not_by_a_prefix() {
        let mut properties = WasmProperties::new();

        properties.insert(&["xds", "route_name"], "checkout");

        assert_eq!(
            properties.get(&["xds", "route_name"]),
            Some(&b"checkout"[..])
        );
        assert_eq!(
            properties.get_joined(b"xds\0route_name"),
            Some(&b"checkout"[..])
        );
        assert_eq!(properties.get(&["xds"]), None);
    }
}
