use std::collections::BTreeMap;

use super::{Document, OwnedValue};

pub struct InnerInputDocument {
    fields: BTreeMap<String, OwnedValue>,
}

impl InnerInputDocument {
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

    pub fn iter_fields_and_values(&self) -> impl Iterator<Item = (&str, &OwnedValue)> {
        self.fields.iter().map(|(f, v)| (f.as_str(), v))
    }
}

impl<D: Document> From<D> for InnerInputDocument {
    fn from(document: D) -> Self {
        let mut fields = BTreeMap::new();
        for (field, value) in document.iter_fields_and_values() {
            fields.insert(field.to_string(), value.into());
        }

        Self { fields }
    }
}
