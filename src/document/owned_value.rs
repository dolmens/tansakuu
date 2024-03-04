use std::collections::{btree_map, BTreeMap};
use std::net::Ipv6Addr;

use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use super::ReferenceValueLeaf;
use crate::document::{ReferenceValue, Value};
use crate::schema::Facet;
use crate::tokenizer::PreTokenizedString;
use tantivy_common::DateTime;

/// This is a owned variant of `Value`, that can be passed around without lifetimes.
/// Represents the value of a any field.
/// It is an enum over all over all of the possible field type.
#[derive(Debug, Clone, PartialEq)]
pub enum OwnedValue {
    /// A null value.
    Null,
    /// The str type is used for any text information.
    Str(String),
    /// Pre-tokenized str type,
    PreTokStr(PreTokenizedString),
    /// Unsigned 64-bits Integer `u64`
    U64(u64),
    /// Signed 64-bits Integer `i64`
    I64(i64),
    /// 64-bits Float `f64`
    F64(f64),
    /// Bool value
    Bool(bool),
    /// Date/time with nanoseconds precision
    Date(DateTime),
    /// Facet
    Facet(Facet),
    /// Arbitrarily sized byte array
    Bytes(Vec<u8>),
    /// A set of values.
    Array(Vec<Self>),
    /// Dynamic object value.
    Object(BTreeMap<String, Self>),
    /// IpV6 Address. Internally there is no IpV4, it needs to be converted to `Ipv6Addr`.
    IpAddr(Ipv6Addr),
}

impl AsRef<OwnedValue> for OwnedValue {
    #[inline]
    fn as_ref(&self) -> &OwnedValue {
        self
    }
}

impl<'a> Value<'a> for &'a OwnedValue {
    type ArrayIter = std::slice::Iter<'a, OwnedValue>;
    type ObjectIter = ObjectMapIter<'a>;

    fn as_value(&self) -> ReferenceValue<'a, Self> {
        match self {
            OwnedValue::Null => ReferenceValueLeaf::Null.into(),
            OwnedValue::Str(val) => ReferenceValueLeaf::Str(val).into(),
            OwnedValue::PreTokStr(val) => ReferenceValueLeaf::PreTokStr(val).into(),
            OwnedValue::U64(val) => ReferenceValueLeaf::U64(*val).into(),
            OwnedValue::I64(val) => ReferenceValueLeaf::I64(*val).into(),
            OwnedValue::F64(val) => ReferenceValueLeaf::F64(*val).into(),
            OwnedValue::Bool(val) => ReferenceValueLeaf::Bool(*val).into(),
            OwnedValue::Date(val) => ReferenceValueLeaf::Date(*val).into(),
            OwnedValue::Facet(val) => ReferenceValueLeaf::Facet(val).into(),
            OwnedValue::Bytes(val) => ReferenceValueLeaf::Bytes(val).into(),
            OwnedValue::IpAddr(val) => ReferenceValueLeaf::IpAddr(*val).into(),
            OwnedValue::Array(array) => ReferenceValue::Array(array.iter()),
            OwnedValue::Object(object) => ReferenceValue::Object(ObjectMapIter(object.iter())),
        }
    }
}

impl Eq for OwnedValue {}

impl<'a, V: Value<'a>> From<ReferenceValue<'a, V>> for OwnedValue {
    fn from(val: ReferenceValue<'a, V>) -> OwnedValue {
        match val {
            ReferenceValue::Leaf(leaf) => match leaf {
                ReferenceValueLeaf::Null => OwnedValue::Null,
                ReferenceValueLeaf::Str(val) => OwnedValue::Str(val.to_string()),
                ReferenceValueLeaf::U64(val) => OwnedValue::U64(val),
                ReferenceValueLeaf::I64(val) => OwnedValue::I64(val),
                ReferenceValueLeaf::F64(val) => OwnedValue::F64(val),
                ReferenceValueLeaf::Date(val) => OwnedValue::Date(val),
                ReferenceValueLeaf::Facet(val) => OwnedValue::Facet(val.clone()),
                ReferenceValueLeaf::Bytes(val) => OwnedValue::Bytes(val.to_vec()),
                ReferenceValueLeaf::IpAddr(val) => OwnedValue::IpAddr(val),
                ReferenceValueLeaf::Bool(val) => OwnedValue::Bool(val),
                ReferenceValueLeaf::PreTokStr(val) => OwnedValue::PreTokStr(val.clone()),
            },
            ReferenceValue::Array(val) => {
                OwnedValue::Array(val.map(|v| v.as_value().into()).collect())
            }
            ReferenceValue::Object(val) => OwnedValue::Object(
                val.map(|(k, v)| (k.to_string(), v.as_value().into()))
                    .collect(),
            ),
        }
    }
}

impl<'a, V: Value<'a>> From<V> for OwnedValue {
    fn from(value: V) -> Self {
        value.as_value().into()
    }
}

impl From<String> for OwnedValue {
    fn from(s: String) -> OwnedValue {
        OwnedValue::Str(s)
    }
}

impl From<Ipv6Addr> for OwnedValue {
    fn from(v: Ipv6Addr) -> OwnedValue {
        OwnedValue::IpAddr(v)
    }
}

impl From<u64> for OwnedValue {
    fn from(v: u64) -> OwnedValue {
        OwnedValue::U64(v)
    }
}

impl From<i64> for OwnedValue {
    fn from(v: i64) -> OwnedValue {
        OwnedValue::I64(v)
    }
}

impl From<f64> for OwnedValue {
    fn from(v: f64) -> OwnedValue {
        OwnedValue::F64(v)
    }
}

impl From<bool> for OwnedValue {
    fn from(b: bool) -> Self {
        OwnedValue::Bool(b)
    }
}

impl From<DateTime> for OwnedValue {
    fn from(dt: DateTime) -> OwnedValue {
        OwnedValue::Date(dt)
    }
}

impl<'a> From<&'a str> for OwnedValue {
    fn from(s: &'a str) -> OwnedValue {
        OwnedValue::Str(s.to_string())
    }
}

impl<'a> From<&'a [u8]> for OwnedValue {
    fn from(bytes: &'a [u8]) -> OwnedValue {
        OwnedValue::Bytes(bytes.to_vec())
    }
}

impl From<Facet> for OwnedValue {
    fn from(facet: Facet) -> OwnedValue {
        OwnedValue::Facet(facet)
    }
}

impl From<Vec<u8>> for OwnedValue {
    fn from(bytes: Vec<u8>) -> OwnedValue {
        OwnedValue::Bytes(bytes)
    }
}

impl From<PreTokenizedString> for OwnedValue {
    fn from(pretokenized_string: PreTokenizedString) -> OwnedValue {
        OwnedValue::PreTokStr(pretokenized_string)
    }
}

impl From<BTreeMap<String, OwnedValue>> for OwnedValue {
    fn from(object: BTreeMap<String, OwnedValue>) -> OwnedValue {
        OwnedValue::Object(object)
    }
}

fn can_be_rfc3339_date_time(text: &str) -> bool {
    if let Some(&first_byte) = text.as_bytes().first() {
        if first_byte.is_ascii_digit() {
            return true;
        }
    }

    false
}

impl From<serde_json::Value> for OwnedValue {
    fn from(value: serde_json::Value) -> Self {
        match value {
            serde_json::Value::Null => Self::Null,
            serde_json::Value::Bool(val) => Self::Bool(val),
            serde_json::Value::Number(number) => {
                if let Some(val) = number.as_i64() {
                    Self::I64(val)
                } else if let Some(val) = number.as_u64() {
                    Self::U64(val)
                } else if let Some(val) = number.as_f64() {
                    Self::F64(val)
                } else {
                    panic!("Unsupported serde_json number {number}");
                }
            }
            serde_json::Value::String(text) => {
                if can_be_rfc3339_date_time(&text) {
                    match OffsetDateTime::parse(&text, &Rfc3339) {
                        Ok(dt) => {
                            let dt_utc = dt.to_offset(time::UtcOffset::UTC);
                            Self::Date(DateTime::from_utc(dt_utc))
                        }
                        Err(_) => Self::Str(text),
                    }
                } else {
                    Self::Str(text)
                }
            }
            serde_json::Value::Array(elements) => {
                let converted_elements = elements.into_iter().map(Self::from).collect();
                Self::Array(converted_elements)
            }
            serde_json::Value::Object(object) => Self::from(object),
        }
    }
}

impl From<serde_json::Map<String, serde_json::Value>> for OwnedValue {
    fn from(map: serde_json::Map<String, serde_json::Value>) -> Self {
        let mut object = BTreeMap::new();

        for (key, value) in map {
            object.insert(key, OwnedValue::from(value));
        }

        OwnedValue::Object(object)
    }
}

/// A wrapper type for iterating over a serde_json object producing reference values.
pub struct ObjectMapIter<'a>(btree_map::Iter<'a, String, OwnedValue>);

impl<'a> Iterator for ObjectMapIter<'a> {
    type Item = (&'a str, &'a OwnedValue);

    fn next(&mut self) -> Option<Self::Item> {
        let (key, value) = self.0.next()?;
        Some((key.as_str(), value))
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::schema::{BytesOptions, Schema};
//     use time::format_description::well_known::Rfc3339;
//     use time::OffsetDateTime;
//     use tantivy_common::DateTime;
//     use crate::{Document, InputDocument};

//     #[test]
//     fn test_parse_bytes_doc() {
//         let mut schema_builder = Schema::builder();
//         let bytes_options = BytesOptions::default();
//         let bytes_field = schema_builder.add_bytes_field("my_bytes", bytes_options);
//         let schema = schema_builder.build();
//         let mut doc = TantivyDocument::default();
//         doc.add_bytes(bytes_field, "this is a test".as_bytes());
//         let json_string = doc.to_json(&schema);
//         assert_eq!(json_string, r#"{"my_bytes":["dGhpcyBpcyBhIHRlc3Q="]}"#);
//     }

//     #[test]
//     fn test_parse_empty_bytes_doc() {
//         let mut schema_builder = Schema::builder();
//         let bytes_options = BytesOptions::default();
//         let bytes_field = schema_builder.add_bytes_field("my_bytes", bytes_options);
//         let schema = schema_builder.build();
//         let mut doc = TantivyDocument::default();
//         doc.add_bytes(bytes_field, "".as_bytes());
//         let json_string = doc.to_json(&schema);
//         assert_eq!(json_string, r#"{"my_bytes":[""]}"#);
//     }

//     #[test]
//     fn test_parse_many_bytes_doc() {
//         let mut schema_builder = Schema::builder();
//         let bytes_options = BytesOptions::default();
//         let bytes_field = schema_builder.add_bytes_field("my_bytes", bytes_options);
//         let schema = schema_builder.build();
//         let mut doc = TantivyDocument::default();
//         doc.add_bytes(
//             bytes_field,
//             "A bigger test I guess\nspanning on multiple lines\nhoping this will work".as_bytes(),
//         );
//         let json_string = doc.to_json(&schema);
//         assert_eq!(
//             json_string,
//             r#"{"my_bytes":["QSBiaWdnZXIgdGVzdCBJIGd1ZXNzCnNwYW5uaW5nIG9uIG11bHRpcGxlIGxpbmVzCmhvcGluZyB0aGlzIHdpbGwgd29yaw=="]}"#
//         );
//     }

//     #[test]
//     fn test_serialize_date() {
//         let value = OwnedValue::from(DateTime::from_utc(
//             OffsetDateTime::parse("1996-12-20T00:39:57+00:00", &Rfc3339).unwrap(),
//         ));
//         let serialized_value_json = serde_json::to_string_pretty(&value).unwrap();
//         assert_eq!(serialized_value_json, r#""1996-12-20T00:39:57Z""#);
//         let value = OwnedValue::from(DateTime::from_utc(
//             OffsetDateTime::parse("1996-12-20T00:39:57-01:00", &Rfc3339).unwrap(),
//         ));
//         let serialized_value_json = serde_json::to_string_pretty(&value).unwrap();
//         // The time zone information gets lost by conversion into `Value::Date` and
//         // implicitly becomes UTC.
//         assert_eq!(serialized_value_json, r#""1996-12-20T01:39:57Z""#);
//     }
// }
