#[derive(Default, Clone)]
pub struct SkipListFormat {
    has_tflist: bool,
}

#[derive(Default)]
pub struct SkipListFormatBuilder {
    has_tflist: bool,
}

impl SkipListFormat {
    pub fn has_tflist(&self) -> bool {
        self.has_tflist
    }
}

impl SkipListFormatBuilder {
    pub fn with_tflist(self, has_tflist: bool) -> Self {
        Self { has_tflist }
    }

    pub fn build(self) -> SkipListFormat {
        SkipListFormat {
            has_tflist: self.has_tflist,
        }
    }
}
