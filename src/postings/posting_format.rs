use super::skip_list::SkipListFormat;

#[derive(Default, Clone)]
pub struct PostingFormat {
    has_tflist: bool,
    has_fieldmask: bool,
    skip_list_format: SkipListFormat,
}

#[derive(Default)]
pub struct PostingFormatBuilder {
    has_tflist: bool,
    has_fieldmask: bool,
}

impl PostingFormatBuilder {
    pub fn with_tflist(self) -> Self {
        Self {
            has_tflist: true,
            has_fieldmask: self.has_fieldmask,
        }
    }

    pub fn with_fieldmask(self) -> Self {
        Self {
            has_tflist: self.has_tflist,
            has_fieldmask: true,
        }
    }

    pub fn build(self) -> PostingFormat {
        let skip_list_format = SkipListFormat::builder().build();
        PostingFormat {
            has_tflist: self.has_tflist,
            has_fieldmask: self.has_fieldmask,
            skip_list_format,
        }
    }
}

impl PostingFormat {
    pub fn builder() -> PostingFormatBuilder {
        PostingFormatBuilder::default()
    }

    pub fn has_tflist(&self) -> bool {
        self.has_tflist
    }

    pub fn has_fieldmask(&self) -> bool {
        self.has_fieldmask
    }

    pub fn skip_list_format(&self) -> &SkipListFormat {
        &self.skip_list_format
    }
}
