use super::Value;

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
