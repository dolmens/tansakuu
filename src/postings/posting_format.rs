use super::skip_list::SkipListFormat;

#[derive(Default, Clone)]
pub struct PostingFormat {
    has_tflist: bool,
    has_fieldmask: bool,
    has_position_list: bool,
    skip_list_format: SkipListFormat,
}

#[derive(Default)]
pub struct PostingFormatBuilder {
    has_tflist: bool,
    has_fieldmask: bool,
    has_position_list: bool,
}

impl PostingFormatBuilder {
    pub fn with_tflist(self) -> Self {
        Self {
            has_tflist: true,
            has_fieldmask: self.has_fieldmask,
            has_position_list: self.has_position_list,
        }
    }

    pub fn with_fieldmask(self) -> Self {
        Self {
            has_tflist: self.has_tflist,
            has_fieldmask: true,
            has_position_list: self.has_position_list,
        }
    }

    pub fn with_position_list(self) -> Self {
        Self {
            has_tflist: true,
            has_fieldmask: self.has_fieldmask,
            has_position_list: true,
        }
    }

    pub fn build(self) -> PostingFormat {
        let skip_list_format = SkipListFormat::builder()
            .with_value(self.has_tflist)
            .build();

        PostingFormat {
            has_tflist: self.has_tflist,
            has_fieldmask: self.has_fieldmask,
            has_position_list: self.has_position_list,
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

    pub fn has_position_list(&self) -> bool {
        self.has_position_list
    }

    pub fn skip_list_format(&self) -> &SkipListFormat {
        &self.skip_list_format
    }
}
