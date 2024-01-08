#[derive(Clone)]
pub struct DocListFormat {
    has_tflist: bool,
    has_fieldmask: bool,
    skiplist_format: Option<SkipListFormat>,
}

#[derive(Clone)]
pub struct SkipListFormat {
    has_tflist: bool,
}

impl DocListFormat {
    pub fn new(has_tflist: bool, has_fieldmask: bool, has_skiplist: bool) -> Self {
        let skiplist_format = if has_skiplist {
            Some(SkipListFormat::new(has_tflist))
        } else {
            None
        };

        Self {
            has_tflist,
            has_fieldmask,
            skiplist_format,
        }
    }

    pub fn has_tflist(&self) -> bool {
        self.has_tflist
    }

    pub fn has_fieldmask(&self) -> bool {
        self.has_fieldmask
    }

    pub fn skip_list_format(&self) -> Option<&SkipListFormat> {
        self.skiplist_format.as_ref()
    }
}

impl SkipListFormat {
    pub fn new(has_tflist: bool) -> Self {
        Self { has_tflist }
    }

    pub fn has_tflist(&self) -> bool {
        self.has_tflist
    }
}
