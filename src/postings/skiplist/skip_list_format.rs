#[derive(Default, Clone)]
pub struct SkipListFormat {
    has_tflist: bool,
}

#[derive(Default)]
pub struct SkipListFormatBuilder {
    has_tflist: bool,
}

impl SkipListFormatBuilder {
    pub fn with_tflist(self) -> Self {
        Self { has_tflist: true }
    }

    pub fn build(self) -> SkipListFormat {
        SkipListFormat {
            has_tflist: self.has_tflist,
        }
    }
}

impl SkipListFormat {
    pub fn builder() -> SkipListFormatBuilder {
        SkipListFormatBuilder::default()
    }

    pub fn has_tflist(&self) -> bool {
        self.has_tflist
    }
}
