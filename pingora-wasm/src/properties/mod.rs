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

//! Plugin properties
//!
//! Values a plugin reads with `proxy_get_property`, keyed by path.

pub(crate) mod built_in;

use std::collections::HashMap;

/// A property value, encoded the way plugins expect to read it.
///
/// Create one with `From`, e.g. `WasmPropertyValue::from(8080_u16)`, or pass anything that
/// converts into it wherever you set a property. Strings and byte slices are stored unchanged, a
/// `bool` becomes a single byte, and an integer becomes 8 little-endian bytes.
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

/// A set of property values keyed by path.
///
/// You give a path as its segments, so the property a plugin reads as `node.metadata.NAME` has
/// the path `["node", "metadata", "NAME"]`. Use this to fill
/// [WasmServices::fixed_properties](crate::WasmServices::fixed_properties).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WasmProperties {
    values: HashMap<Vec<u8>, Vec<u8>>,
}

impl WasmProperties {
    /// Create an empty set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the value at `path`, replacing any existing value.
    pub fn insert(&mut self, path: &[&str], value: impl Into<WasmPropertyValue>) {
        let mut joined_path = Vec::new();
        join_path(
            path.iter().map(|segment| segment.as_bytes()),
            &mut joined_path,
        );
        self.values.insert(joined_path, value.into().0);
    }

    /// Return the encoded value at `path`, or `None` if nothing is set there.
    pub fn get(&self, path: &[&str]) -> Option<&[u8]> {
        let mut joined_path = Vec::new();
        join_path(
            path.iter().map(|segment| segment.as_bytes()),
            &mut joined_path,
        );
        self.values.get(&joined_path).map(Vec::as_slice)
    }

    /// Set the value at a path whose segments are already joined with `\0`.
    pub(crate) fn insert_joined(&mut self, joined_path: &[u8], value: &[u8]) {
        self.values.insert(joined_path.to_vec(), value.to_vec());
    }

    /// Return the value at a path whose segments are already joined with `\0`.
    pub(crate) fn get_joined(&self, joined_path: &[u8]) -> Option<&[u8]> {
        self.values.get(joined_path).map(Vec::as_slice)
    }
}

/// Join path segments with `\0`, the form the ABI uses for a path.
///
/// The result replaces the contents of `joined_path`, which lets a caller reuse one buffer.
pub(crate) fn join_path<'a>(
    segments: impl IntoIterator<Item = &'a [u8]>,
    joined_path: &mut Vec<u8>,
) {
    joined_path.clear();
    for (index, segment) in segments.into_iter().enumerate() {
        if index > 0 {
            joined_path.push(0);
        }
        joined_path.extend_from_slice(segment);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_are_encoded_for_plugins() {
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
    fn value_is_found_by_segments_and_joined_path_not_by_prefix() {
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
