#[derive(Default, Clone)]
pub struct SkipListFormat {
    has_value: bool,
}

#[derive(Default)]
pub struct SkipListFormatBuilder {
    has_value: bool,
}

impl SkipListFormatBuilder {
    pub fn with_value(self, has_value: bool) -> Self {
        Self { has_value }
    }

    pub fn build(self) -> SkipListFormat {
        SkipListFormat {
            has_value: self.has_value,
        }
    }
}

impl SkipListFormat {
    pub fn builder() -> SkipListFormatBuilder {
        SkipListFormatBuilder::default()
    }

    pub fn has_value(&self) -> bool {
        self.has_value
    }
}
