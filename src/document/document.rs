use std::collections::HashMap;

use super::Value;

pub struct Document {
    fields: HashMap<String, Value>,
}

impl Document {
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }

    pub fn add_field<I: Into<Value>>(&mut self, name: String, value: I) {
        self.fields.insert(name, value.into());
    }

    pub fn fields(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.fields.iter().map(|(k, v)| (k.as_str(), v))
    }
}

