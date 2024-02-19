use super::skip_list::SkipListFormat;

#[derive(Default, Clone, Copy)]
pub struct DocListFormat {
    has_tflist: bool,
    has_fieldmask: bool,
    skip_list_format: SkipListFormat,
}

#[derive(Default)]
pub struct DocListFormatBuilder {
    has_tflist: bool,
    has_fieldmask: bool,
}

impl DocListFormatBuilder {
    pub fn with_tflist(self) -> Self {
        Self {
            has_tflist: true,
            has_fieldmask: self.has_fieldmask,
        }
    }

    pub fn with_tflist_as(self, has_tflist: bool) -> Self {
        Self {
            has_tflist,
            has_fieldmask: self.has_fieldmask,
        }
    }

    pub fn with_fieldmask(self) -> Self {
        Self {
            has_tflist: self.has_tflist,
            has_fieldmask: true,
        }
    }

    pub fn with_fieldmask_as(self, has_fieldmask: bool) -> Self {
        Self {
            has_tflist: self.has_tflist,
            has_fieldmask,
        }
    }

    pub fn build(self) -> DocListFormat {
        let skip_list_format = SkipListFormat::builder()
            .with_value(self.has_tflist)
            .build();

        DocListFormat {
            has_tflist: self.has_tflist,
            has_fieldmask: self.has_fieldmask,
            skip_list_format,
        }
    }
}

impl DocListFormat {
    pub fn builder() -> DocListFormatBuilder {
        DocListFormatBuilder::default()
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
