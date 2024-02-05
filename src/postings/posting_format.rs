use crate::schema::TextIndexOptions;

use super::{skip_list::SkipListFormat, DocListFormat};

#[derive(Default, Clone)]
pub struct PostingFormat {
    doc_list_format: DocListFormat,
    has_position_list: bool,
}

#[derive(Default)]
pub struct PostingFormatBuilder {
    has_tflist: bool,
    has_fieldmask: bool,
    has_position_list: bool,
}

impl PostingFormatBuilder {
    pub fn with_text_index_options(self, text_index_options: &TextIndexOptions) -> Self {
        Self {
            has_tflist: text_index_options.has_tflist,
            has_fieldmask: text_index_options.has_fieldmask,
            has_position_list: text_index_options.has_position_list,
        }
    }

    pub fn with_tflist(self) -> Self {
        Self {
            has_tflist: true,
            has_fieldmask: self.has_fieldmask,
            has_position_list: self.has_position_list,
        }
    }

    pub fn with_tflist_as(self, has_tflist: bool) -> Self {
        Self {
            has_tflist,
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

    pub fn with_fieldmask_as(self, has_fieldmask: bool) -> Self {
        Self {
            has_tflist: self.has_tflist,
            has_fieldmask,
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

    pub fn with_position_list_as(self, has_position_list: bool) -> Self {
        Self {
            has_tflist: true,
            has_fieldmask: self.has_fieldmask,
            has_position_list,
        }
    }

    pub fn build(self) -> PostingFormat {
        let doc_list_format = DocListFormat::builder()
            .with_tflist_as(self.has_tflist)
            .with_fieldmask_as(self.has_fieldmask)
            .build();

        PostingFormat {
            doc_list_format,
            has_position_list: self.has_position_list,
        }
    }
}

impl PostingFormat {
    pub fn builder() -> PostingFormatBuilder {
        PostingFormatBuilder::default()
    }

    pub fn has_tflist(&self) -> bool {
        self.doc_list_format.has_tflist()
    }

    pub fn has_fieldmask(&self) -> bool {
        self.doc_list_format.has_fieldmask()
    }

    pub fn has_position_list(&self) -> bool {
        self.has_position_list
    }

    pub fn doc_list_format(&self) -> &DocListFormat {
        &self.doc_list_format
    }

    pub fn skip_list_format(&self) -> &SkipListFormat {
        self.doc_list_format.skip_list_format()
    }
}
