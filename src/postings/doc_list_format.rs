use super::skiplist::{SkipListFormat, SkipListFormatBuilder};

#[derive(Clone)]
pub struct DocListFormat {
    has_tflist: bool,
    has_fieldmask: bool,
    skiplist_format: SkipListFormat,
}

impl DocListFormat {
    pub fn new(has_tflist: bool, has_fieldmask: bool) -> Self {
        let skiplist_format = SkipListFormatBuilder::default().with_tflist(false).build();

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

    pub fn skip_list_format(&self) -> &SkipListFormat {
        &self.skiplist_format
    }
}
