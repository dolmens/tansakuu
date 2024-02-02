//! Document definition for Tantivy to index and store.
//!
//! A document and its values are defined by a couple core traits:
//! - [Document] which describes your top-level document and it's fields.
//! - [Value] which provides tantivy with a way to access the document's values in a common way
//!   without performing any additional allocations.
//! - [DocumentDeserialize] which implements the necessary code to deserialize the document from the
//!   doc store.
//!
//! Tantivy provides a few out-of-box implementations of these core traits to provide
//! some simple usage if you don't want to implement these traits on a custom type yourself.
//!
//! # Out-of-box document implementations
//! - [Document] the old document type used by Tantivy before the trait based approach was
//!   implemented. This type is still valid and provides all of the original behaviour you might
//!   expect.
//! - `BTreeMap<Field, Value>` a mapping of field_ids to their relevant schema value using a
//!   BTreeMap.
//! - `HashMap<Field, Value>` a mapping of field_ids to their relevant schema value using a HashMap.
//!
//! # Implementing your custom documents
//! Often in larger projects or higher performance applications you want to avoid the extra overhead
//! of converting your own types to the Tantivy [Document] type, this can often save you a
//! significant amount of time when indexing by avoiding the additional allocations.
//!
//! ### Important Note
//! The implementor of the `Document` trait must be `'static` and safe to send across
//! thread boundaries.
//!
//! ## Reusing existing types
//! The API design of the document traits allow you to reuse as much of as little of the
//! existing trait implementations as you like, this can save quite a bit of boilerplate
//! as shown by the following example.
//!
//! ## A basic custom document
//! ```
//! use std::collections::{btree_map, BTreeMap};
//! use tantivy::schema::{Document, Field};
//! use tantivy::schema::document::{DeserializeError, DocumentDeserialize, DocumentDeserializer};
//!
//! /// Our custom document to let us use a map of `serde_json::Values`.
//! pub struct MyCustomDocument {
//!     // Tantivy provides trait implementations for common `serde_json` types.
//!     fields: BTreeMap<Field, serde_json::Value>
//! }
//!
//! impl Document for MyCustomDocument {
//!     // The value type produced by the `iter_fields_and_values` iterator.
//!     type Value<'a> = &'a serde_json::Value;
//!     // The iterator which is produced by `iter_fields_and_values`.
//!     // Often this is a simple new-type wrapper unless you like super long generics.
//!     type FieldsValuesIter<'a> = MyCustomIter<'a>;
//!
//!     /// Produces an iterator over the document fields and values.
//!     /// This method will be called multiple times, it's important
//!     /// to not do anything too heavy in this step, any heavy operations
//!     /// should be done before and effectively cached.
//!     fn iter_fields_and_values(&self) -> Self::FieldsValuesIter<'_> {
//!         MyCustomIter(self.fields.iter())
//!     }
//! }
//!
//! // Our document must also provide a way to get the original doc
//! // back when it's deserialized from the doc store.
//! // The API for this is very similar to serde but a little bit
//! // more specialised, giving you access to types like IP addresses, datetime, etc...
//! impl DocumentDeserialize for MyCustomDocument {
//!     fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
//!     where D: DocumentDeserializer<'de>
//!     {
//!         // We're not going to implement the necessary logic for this example
//!         // see the `Deserialization` section of implementing a custom document
//!         // for more information on how this works.
//!         unimplemented!()
//!     }
//! }
//!
//! /// Our custom iterator just helps us to avoid some messy generics.
//! pub struct MyCustomIter<'a>(btree_map::Iter<'a, Field, serde_json::Value>);
//! impl<'a> Iterator for MyCustomIter<'a> {
//!     // Here we can see our field-value pairs being produced by the iterator.
//!     // The value returned alongside the field is the same type as `Document::Value<'_>`.
//!     type Item = (Field, &'a serde_json::Value);
//!
//!     fn next(&mut self) -> Option<Self::Item> {
//!         let (field, value) = self.0.next()?;
//!         Some((*field, value))
//!     }
//! }
//! ```
//!
//! You may have noticed in this example that we haven't needed to implement any custom value types,
//! instead we've just used a [serde_json::Value] type which tantivy provides an existing
//! implementation for.
//!
//! ## Implementing custom values
//! Internally, Tantivy only works with `ReferenceValue` which is an enum that tries to borrow
//! as much data as it can, in order to allow documents to return custom types, they must implement
//! the `Value` trait which provides a way for Tantivy to get a `ReferenceValue` that it can then
//! index and store.
//!
//! Values can just as easily be customised as documents by implementing the `Value` trait.
//!
//! The implementor of this type should not own the data it's returning, instead it should just
//! hold references of the data held by the parent [Document] which can then be passed
//! on to the [ReferenceValue].
//!
//! This is why `Value` is implemented for `&'a serde_json::Value` and `&'a
//! tantivy::schema::Value` but not for their owned counterparts, as we cannot satisfy the lifetime
//! bounds necessary when indexing the documents.
//!
//! ### A note about returning values
//! The custom value type does not have to be the type stored by the document, instead the
//! implementor of a `Value` can just be used as a way to convert between the owned type
//! kept in the parent document, and the value passed into Tantivy.
//!
//! ```
//! use tantivy::schema::document::ReferenceValue;
//! use tantivy::schema::document::ReferenceValueLeaf;
//! use tantivy::schema::{Value};
//!
//! #[derive(Debug)]
//! /// Our custom value type which has 3 types, a string, float and bool.
//! #[allow(dead_code)]
//! pub enum MyCustomValue<'a> {
//!     // Our string data is owned by the parent document, instead we just
//!     // hold onto a reference of this data.
//!     String(&'a str),
//!     Float(f64),
//!     Bool(bool),
//! }
//!
//! impl<'a> Value<'a> for MyCustomValue<'a> {
//!     // We don't need to worry about these types here as we're not
//!     // working with nested types, but if we wanted to we would
//!     // define our two iterator types, a sequence of ReferenceValues
//!     // for the array iterator and a sequence of key-value pairs for objects.
//!     type ArrayIter = std::iter::Empty<Self>;
//!     type ObjectIter = std::iter::Empty<(&'a str, Self)>;
//!
//!     // The ReferenceValue which Tantivy can use.
//!     fn as_value(&self) -> ReferenceValue<'a, Self> {
//!         // We can support any type that Tantivy itself supports.
//!         match self {
//!             MyCustomValue::String(val) => ReferenceValue::Leaf(ReferenceValueLeaf::Str(*val)),
//!             MyCustomValue::Float(val) => ReferenceValue::Leaf(ReferenceValueLeaf::F64(*val)),
//!             MyCustomValue::Bool(val) => ReferenceValue::Leaf(ReferenceValueLeaf::Bool(*val)),
//!         }
//!     }
//!
//! }
//! ```
//!
//! TODO: Complete this section...

mod de;
// mod existing_type_impls;
mod binary_serializer_cow;
mod cow_string;
mod input_document;
mod owned_value;
mod se;
mod value;

pub use cow_string::CowString;
// pub(crate) use self::de::BinaryDocumentDeserializer;
pub use self::de::{
    ArrayAccess, DeserializeError, DocumentDeserialize, DocumentDeserializer, ObjectAccess,
    ValueDeserialize, ValueDeserializer, ValueType, ValueVisitor,
};
pub use self::input_document::InputDocument;
pub use self::owned_value::OwnedValue;
// pub(crate) use self::se::BinaryDocumentSerializer;
pub use self::value::{value_to_string, ReferenceValue, ReferenceValueLeaf, Value};

/// The core trait representing a document within the index.
pub trait Document: Send + Sync + 'static {
    /// The value of the field.
    type Value<'a>: Value<'a> + Clone
    where
        Self: 'a;

    /// The iterator over all of the fields and values within the doc.
    type FieldsValuesIter<'a>: Iterator<Item = (&'a str, Self::Value<'a>)>
    where
        Self: 'a;

    /// Get an iterator iterating over all fields and values in a document.
    fn iter_fields_and_values(&self) -> Self::FieldsValuesIter<'_>;
}

pub(crate) mod type_codes {
    pub const TEXT_CODE: u8 = 0;
    pub const U64_CODE: u8 = 1;
    pub const I64_CODE: u8 = 2;
    pub const HIERARCHICAL_FACET_CODE: u8 = 3;
    pub const BYTES_CODE: u8 = 4;
    pub const DATE_CODE: u8 = 5;
    pub const F64_CODE: u8 = 6;
    pub const EXT_CODE: u8 = 7;

    #[deprecated]
    pub const JSON_OBJ_CODE: u8 = 8; // Replaced by the `OBJECT_CODE`.
    pub const BOOL_CODE: u8 = 9;
    pub const IP_CODE: u8 = 10;
    pub const NULL_CODE: u8 = 11;
    pub const ARRAY_CODE: u8 = 12;
    pub const OBJECT_CODE: u8 = 13;

    // Extended type codes
    pub const TOK_STR_EXT_CODE: u8 = 0;
}
