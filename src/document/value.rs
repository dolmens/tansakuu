use std::fmt::Debug;
use std::net::Ipv6Addr;

use tantivy_common::DateTime;

use crate::schema::Facet;
use crate::tokenizer::PreTokenizedString;

/// A single field value.
pub trait Value<'a>: Send + Sync + Debug {
    /// The child value type returned by this doc value.
    /// The iterator for walking through the elements within the array.
    type ArrayIter: Iterator<Item = Self>;
    /// The visitor walking through the key-value pairs within
    /// the object.
    type ObjectIter: Iterator<Item = (&'a str, Self)>;

    /// Returns the field value represented by an enum which borrows it's data.
    fn as_value(&self) -> ReferenceValue<'a, Self>;

    #[inline]
    /// Returns if the value is `null` or not.
    fn is_null(&self) -> bool {
        matches!(
            self.as_value(),
            ReferenceValue::Leaf(ReferenceValueLeaf::Null)
        )
    }

    #[inline]
    /// If the Value is a String, returns the associated str. Returns None otherwise.
    fn as_leaf(&self) -> Option<ReferenceValueLeaf<'a>> {
        if let ReferenceValue::Leaf(val) = self.as_value() {
            Some(val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a String, returns the associated str. Returns None otherwise.
    fn as_str(&self) -> Option<&'a str> {
        self.as_leaf().and_then(|leaf| leaf.as_str())
    }

    #[inline]
    /// If the Value is a u64, returns the associated u64. Returns None otherwise.
    fn as_u64(&self) -> Option<u64> {
        self.as_leaf().and_then(|leaf| leaf.as_u64())
    }

    #[inline]
    /// If the Value is a i64, returns the associated i64. Returns None otherwise.
    fn as_i64(&self) -> Option<i64> {
        self.as_leaf().and_then(|leaf| leaf.as_i64())
    }

    #[inline]
    /// If the Value is a f64, returns the associated f64. Returns None otherwise.
    fn as_f64(&self) -> Option<f64> {
        self.as_leaf().and_then(|leaf| leaf.as_f64())
    }

    #[inline]
    fn as_i8(&self) -> Option<i8> {
        self.as_leaf().and_then(|leaf| leaf.as_i8())
    }

    #[inline]
    fn as_i16(&self) -> Option<i16> {
        self.as_leaf().and_then(|leaf| leaf.as_i16())
    }

    #[inline]
    fn as_i32(&self) -> Option<i32> {
        self.as_leaf().and_then(|leaf| leaf.as_i32())
    }

    #[inline]
    fn as_u8(&self) -> Option<u8> {
        self.as_leaf().and_then(|leaf| leaf.as_u8())
    }

    #[inline]
    fn as_u16(&self) -> Option<u16> {
        self.as_leaf().and_then(|leaf| leaf.as_u16())
    }

    #[inline]
    fn as_u32(&self) -> Option<u32> {
        self.as_leaf().and_then(|leaf| leaf.as_u32())
    }

    #[inline]
    /// If the Value is a i32, returns the associated i32. Returns None otherwise.
    fn as_f32(&self) -> Option<f32> {
        self.as_leaf().and_then(|leaf| leaf.as_f32())
    }

    #[inline]
    /// If the Value is a datetime, returns the associated datetime. Returns None otherwise.
    fn as_datetime(&self) -> Option<DateTime> {
        self.as_leaf().and_then(|leaf| leaf.as_datetime())
    }

    #[inline]
    /// If the Value is a IP address, returns the associated IP. Returns None otherwise.
    fn as_ip_addr(&self) -> Option<Ipv6Addr> {
        self.as_leaf().and_then(|leaf| leaf.as_ip_addr())
    }

    #[inline]
    /// If the Value is a bool, returns the associated bool. Returns None otherwise.
    fn as_bool(&self) -> Option<bool> {
        self.as_leaf().and_then(|leaf| leaf.as_bool())
    }

    #[inline]
    /// If the Value is a pre-tokenized string, returns the associated string. Returns None
    /// otherwise.
    fn as_pre_tokenized_text(&self) -> Option<&'a PreTokenizedString> {
        self.as_leaf().and_then(|leaf| leaf.as_pre_tokenized_text())
    }

    #[inline]
    /// If the Value is a bytes value, returns the associated set of bytes. Returns None otherwise.
    fn as_bytes(&self) -> Option<&'a [u8]> {
        self.as_leaf().and_then(|leaf| leaf.as_bytes())
    }

    #[inline]
    /// If the Value is a facet, returns the associated facet. Returns None otherwise.
    fn as_facet(&self) -> Option<&'a Facet> {
        self.as_leaf().and_then(|leaf| leaf.as_facet())
    }

    #[inline]
    /// Returns the iterator over the array if the Value is an array.
    fn as_array(&self) -> Option<Self::ArrayIter> {
        if let ReferenceValue::Array(val) = self.as_value() {
            Some(val)
        } else {
            None
        }
    }

    #[inline]
    /// Returns the iterator over the object if the Value is an object.
    fn as_object(&self) -> Option<Self::ObjectIter> {
        if let ReferenceValue::Object(val) = self.as_value() {
            Some(val)
        } else {
            None
        }
    }

    #[inline]
    /// Returns true if the Value is an array.
    fn is_array(&self) -> bool {
        matches!(self.as_value(), ReferenceValue::Object(_))
    }

    #[inline]
    /// Returns true if the Value is an object.
    fn is_object(&self) -> bool {
        matches!(self.as_value(), ReferenceValue::Object(_))
    }
}

/// A enum representing a leaf value for tantivy to index.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ReferenceValueLeaf<'a> {
    /// A null value.
    Null,
    /// The str type is used for any text information.
    Str(&'a str),
    /// Signed 8-bits Integer `i8`
    I8(i8),
    /// Signed 16-bits Integer `i16`
    I16(i16),
    /// Signed 32-bits Integer `i32`
    I32(i32),
    /// Signed 64-bits Integer `i64`
    I64(i64),
    /// Unsigned 8-bits Integer `u8`
    U8(u8),
    /// Unsigned 16-bits Integer `u16`
    U16(u16),
    /// Unsigned 32-bits Integer `u32`
    U32(u32),
    /// Unsigned 64-bits Integer `u64`
    U64(u64),
    /// 32-bits Float `f32`
    F32(f32),
    /// 64-bits Float `f64`
    F64(f64),
    /// Date/time with nanoseconds precision
    Date(DateTime),
    /// Facet
    Facet(&'a Facet),
    /// Arbitrarily sized byte array
    Bytes(&'a [u8]),
    /// IpV6 Address. Internally there is no IpV4, it needs to be converted to `Ipv6Addr`.
    IpAddr(Ipv6Addr),
    /// Bool value
    Bool(bool),
    /// Pre-tokenized str type,
    PreTokStr(&'a PreTokenizedString),
}

impl<'a, T: Value<'a> + ?Sized> From<ReferenceValueLeaf<'a>> for ReferenceValue<'a, T> {
    #[inline]
    fn from(value: ReferenceValueLeaf<'a>) -> Self {
        match value {
            ReferenceValueLeaf::Null => ReferenceValue::Leaf(ReferenceValueLeaf::Null),
            ReferenceValueLeaf::Str(val) => ReferenceValue::Leaf(ReferenceValueLeaf::Str(val)),
            ReferenceValueLeaf::I8(val) => ReferenceValue::Leaf(ReferenceValueLeaf::I8(val)),
            ReferenceValueLeaf::I16(val) => ReferenceValue::Leaf(ReferenceValueLeaf::I16(val)),
            ReferenceValueLeaf::I32(val) => ReferenceValue::Leaf(ReferenceValueLeaf::I32(val)),
            ReferenceValueLeaf::I64(val) => ReferenceValue::Leaf(ReferenceValueLeaf::I64(val)),
            ReferenceValueLeaf::U8(val) => ReferenceValue::Leaf(ReferenceValueLeaf::U8(val)),
            ReferenceValueLeaf::U16(val) => ReferenceValue::Leaf(ReferenceValueLeaf::U16(val)),
            ReferenceValueLeaf::U32(val) => ReferenceValue::Leaf(ReferenceValueLeaf::U32(val)),
            ReferenceValueLeaf::U64(val) => ReferenceValue::Leaf(ReferenceValueLeaf::U64(val)),
            ReferenceValueLeaf::F32(val) => ReferenceValue::Leaf(ReferenceValueLeaf::F32(val)),
            ReferenceValueLeaf::F64(val) => ReferenceValue::Leaf(ReferenceValueLeaf::F64(val)),
            ReferenceValueLeaf::Date(val) => ReferenceValue::Leaf(ReferenceValueLeaf::Date(val)),
            ReferenceValueLeaf::Facet(val) => ReferenceValue::Leaf(ReferenceValueLeaf::Facet(val)),
            ReferenceValueLeaf::Bytes(val) => ReferenceValue::Leaf(ReferenceValueLeaf::Bytes(val)),
            ReferenceValueLeaf::IpAddr(val) => {
                ReferenceValue::Leaf(ReferenceValueLeaf::IpAddr(val))
            }
            ReferenceValueLeaf::Bool(val) => ReferenceValue::Leaf(ReferenceValueLeaf::Bool(val)),
            ReferenceValueLeaf::PreTokStr(val) => {
                ReferenceValue::Leaf(ReferenceValueLeaf::PreTokStr(val))
            }
        }
    }
}

impl<'a> ReferenceValueLeaf<'a> {
    #[inline]
    /// Returns if the value is `null` or not.
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    #[inline]
    /// If the Value is a String, returns the associated str. Returns None otherwise.
    pub fn as_str(&self) -> Option<&'a str> {
        if let Self::Str(val) = self {
            Some(val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a u64, returns the associated u64. Returns None otherwise.
    pub fn as_u64(&self) -> Option<u64> {
        if let Self::U64(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a i64, returns the associated i64. Returns None otherwise.
    pub fn as_i64(&self) -> Option<i64> {
        if let Self::I64(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a i8, returns the associated i8. Returns None otherwise.
    pub fn as_i8(&self) -> Option<i8> {
        if let Self::I8(val) = self {
            Some(*val)
        } else if let Self::I64(val) = self {
            Some(*val as _)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a i16, returns the associated i16. Returns None otherwise.
    pub fn as_i16(&self) -> Option<i16> {
        if let Self::I16(val) = self {
            Some(*val)
        } else if let Self::I64(val) = self {
            Some(*val as _)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a i32, returns the associated i32. Returns None otherwise.
    pub fn as_i32(&self) -> Option<i32> {
        if let Self::I32(val) = self {
            Some(*val)
        } else if let Self::I64(val) = self {
            Some(*val as _)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a u8, returns the associated u8. Returns None otherwise.
    pub fn as_u8(&self) -> Option<u8> {
        if let Self::U8(val) = self {
            Some(*val)
        } else if let Self::U64(val) = self {
            Some(*val as _)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a u16, returns the associated u16. Returns None otherwise.
    pub fn as_u16(&self) -> Option<u16> {
        if let Self::U16(val) = self {
            Some(*val)
        } else if let Self::U64(val) = self {
            Some(*val as _)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a u32, returns the associated u32. Returns None otherwise.
    pub fn as_u32(&self) -> Option<u32> {
        if let Self::U32(val) = self {
            Some(*val)
        } else if let Self::U64(val) = self {
            Some(*val as _)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a f64, returns the associated f64. Returns None otherwise.
    pub fn as_f64(&self) -> Option<f64> {
        if let Self::F64(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a f32, returns the associated f32. Returns None otherwise.
    pub fn as_f32(&self) -> Option<f32> {
        if let Self::F32(val) = self {
            Some(*val)
        } else if let Self::F64(val) = self {
            Some(*val as _)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a datetime, returns the associated datetime. Returns None otherwise.
    pub fn as_datetime(&self) -> Option<DateTime> {
        if let Self::Date(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a IP address, returns the associated IP. Returns None otherwise.
    pub fn as_ip_addr(&self) -> Option<Ipv6Addr> {
        if let Self::IpAddr(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a bool, returns the associated bool. Returns None otherwise.
    pub fn as_bool(&self) -> Option<bool> {
        if let Self::Bool(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a pre-tokenized string, returns the associated string. Returns None
    /// otherwise.
    pub fn as_pre_tokenized_text(&self) -> Option<&'a PreTokenizedString> {
        if let Self::PreTokStr(val) = self {
            Some(val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a bytes value, returns the associated set of bytes. Returns None otherwise.
    pub fn as_bytes(&self) -> Option<&'a [u8]> {
        if let Self::Bytes(val) = self {
            Some(val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a facet, returns the associated facet. Returns None otherwise.
    pub fn as_facet(&self) -> Option<&'a Facet> {
        if let Self::Facet(val) = self {
            Some(val)
        } else {
            None
        }
    }
}

/// A enum representing a value for tantivy to index.
#[derive(Clone, Debug, PartialEq)]
pub enum ReferenceValue<'a, V>
where
    V: Value<'a> + ?Sized,
{
    /// A null value.
    Leaf(ReferenceValueLeaf<'a>),
    /// A an array containing multiple values.
    Array(V::ArrayIter),
    /// A nested / dynamic object.
    Object(V::ObjectIter),
}

impl<'a, V> ReferenceValue<'a, V>
where
    V: Value<'a>,
{
    #[inline]
    /// Returns if the value is `null` or not.
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Leaf(ReferenceValueLeaf::Null))
    }

    #[inline]
    /// If the Value is a leaf, returns the associated leaf. Returns None otherwise.
    pub fn as_leaf(&self) -> Option<&ReferenceValueLeaf<'a>> {
        if let Self::Leaf(val) = self {
            Some(val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a String, returns the associated str. Returns None otherwise.
    pub fn as_str(&self) -> Option<&'a str> {
        self.as_leaf().and_then(|leaf| leaf.as_str())
    }

    #[inline]
    /// If the Value is a u64, returns the associated u64. Returns None otherwise.
    pub fn as_u64(&self) -> Option<u64> {
        self.as_leaf().and_then(|leaf| leaf.as_u64())
    }

    #[inline]
    /// If the Value is a i64, returns the associated i64. Returns None otherwise.
    pub fn as_i64(&self) -> Option<i64> {
        self.as_leaf().and_then(|leaf| leaf.as_i64())
    }

    #[inline]
    /// If the Value is a f64, returns the associated f64. Returns None otherwise.
    pub fn as_f64(&self) -> Option<f64> {
        self.as_leaf().and_then(|leaf| leaf.as_f64())
    }

    #[inline]
    /// If the Value is a datetime, returns the associated datetime. Returns None otherwise.
    pub fn as_datetime(&self) -> Option<DateTime> {
        self.as_leaf().and_then(|leaf| leaf.as_datetime())
    }

    #[inline]
    /// If the Value is a IP address, returns the associated IP. Returns None otherwise.
    pub fn as_ip_addr(&self) -> Option<Ipv6Addr> {
        self.as_leaf().and_then(|leaf| leaf.as_ip_addr())
    }

    #[inline]
    /// If the Value is a bool, returns the associated bool. Returns None otherwise.
    pub fn as_bool(&self) -> Option<bool> {
        self.as_leaf().and_then(|leaf| leaf.as_bool())
    }

    #[inline]
    /// If the Value is a pre-tokenized string, returns the associated string. Returns None
    /// otherwise.
    pub fn as_pre_tokenized_text(&self) -> Option<&'a PreTokenizedString> {
        self.as_leaf().and_then(|leaf| leaf.as_pre_tokenized_text())
    }

    #[inline]
    /// If the Value is a bytes value, returns the associated set of bytes. Returns None otherwise.
    pub fn as_bytes(&self) -> Option<&'a [u8]> {
        self.as_leaf().and_then(|leaf| leaf.as_bytes())
    }

    #[inline]
    /// If the Value is a facet, returns the associated facet. Returns None otherwise.
    pub fn as_facet(&self) -> Option<&'a Facet> {
        self.as_leaf().and_then(|leaf| leaf.as_facet())
    }

    #[inline]
    /// Returns true if the Value is an array.
    pub fn is_array(&self) -> bool {
        matches!(self, Self::Array(_))
    }

    #[inline]
    /// Returns true if the Value is an object.
    pub fn is_object(&self) -> bool {
        matches!(self, Self::Object(_))
    }
}
