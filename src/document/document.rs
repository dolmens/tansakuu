use std::collections::HashMap;

pub struct Document {
    fields: HashMap<String, String>,
}

impl Document {
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }

    pub fn add_field(&mut self, name: String, value: String) {
        self.fields.insert(name, value);
    }

    pub fn fields(&self) -> impl Iterator<Item = (&str, &str)> {
        self.fields.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }
}
