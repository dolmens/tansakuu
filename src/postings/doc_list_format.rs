use crate::{DocId, FieldMask, TermFreq};

use super::{copy_decode, copy_encode, multi_value_buffer::MultiValue};

pub struct DocListFormat {
    has_tflist: bool,
    has_fieldmask: bool,
    value_items: MultiValue,
    skip_list_format: DocSkipListFormat,
}

#[derive(Clone)]
pub struct DocSkipListFormat {
    has_tflist: bool,
    value_items: MultiValue,
}

impl DocListFormat {
    pub fn new(has_tflist: bool, has_fieldmask: bool) -> Self {
        let mut value_items = MultiValue::new();
        value_items.add_value::<DocId>(copy_encode, copy_decode);
        if has_tflist {
            value_items.add_value::<TermFreq>(copy_encode, copy_decode);
        }
        if has_fieldmask {
            value_items.add_value::<FieldMask>(copy_encode, copy_decode);
        }
        let skip_list_format = DocSkipListFormat::new(has_tflist);

        Self {
            has_tflist,
            has_fieldmask,
            value_items,
            skip_list_format,
        }
    }

    pub fn has_tflist(&self) -> bool {
        self.has_tflist
    }

    pub fn has_fieldmask(&self) -> bool {
        self.has_fieldmask
    }

    pub fn skip_list_format(&self) -> &DocSkipListFormat {
        &self.skip_list_format
    }

    pub fn value_items(&self) -> &MultiValue {
        &self.value_items
    }
}

impl DocSkipListFormat {
    pub fn new(has_tflist: bool) -> Self {
        let mut value_items = MultiValue::new();
        value_items.add_value::<DocId>(copy_encode, copy_decode);
        if has_tflist {
            value_items.add_value::<TermFreq>(copy_encode, copy_decode);
        }
        value_items.add_value::<usize>(copy_encode, copy_decode);

        Self {
            has_tflist,
            value_items,
        }
    }

    pub fn has_tflist(&self) -> bool {
        self.has_tflist
    }

    pub fn value_items(&self) -> &MultiValue {
        &self.value_items
    }
}
