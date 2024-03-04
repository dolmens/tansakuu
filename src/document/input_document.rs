use std::collections::BTreeMap;

use super::{Document, OwnedValue};

pub struct InputDocument {
    fields: BTreeMap<String, OwnedValue>,
}

pub struct InputFieldValueIter<'a>(
    pub(crate) std::collections::btree_map::Iter<'a, String, OwnedValue>,
);

impl InputDocument {
    pub fn new() -> Self {
        Self {
            fields: BTreeMap::new(),
        }
    }

    pub fn add_field<I: Into<OwnedValue>>(&mut self, field_name: String, value: I) {
        self.fields.insert(field_name, value.into());
    }

    pub fn get_field(&self, field_name: &str) -> Option<&OwnedValue> {
        self.fields.get(field_name)
    }
}

impl Document for InputDocument {
    type Value<'a> = &'a OwnedValue;
    type FieldsValuesIter<'a> = InputFieldValueIter<'a>;

    fn iter_fields_and_values(&self) -> Self::FieldsValuesIter<'_> {
        InputFieldValueIter(self.fields.iter())
    }
}

impl<'a> Iterator for InputFieldValueIter<'a> {
    type Item = (&'a str, &'a OwnedValue);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(f, v)| (f.as_str(), v))
    }
}
