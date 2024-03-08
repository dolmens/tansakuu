use std::sync::Arc;

use crate::{table::SegmentStat, tokenizer::TokenizerManager};

pub struct IndexWriterResourceBuilder<'a> {
    resource: IndexWriterResource<'a>,
}

pub struct IndexWriterResource<'a> {
    tokenizers: &'a TokenizerManager,
    recent_segment_stat: Option<&'a Arc<SegmentStat>>,
}

impl<'a> IndexWriterResourceBuilder<'a> {
    pub fn new(tokenizers: &'a TokenizerManager) -> Self {
        let resource = IndexWriterResource {
            tokenizers,
            recent_segment_stat: None,
        };

        Self { resource }
    }

    pub fn set_tokenizers(mut self, tokenizers: &'a TokenizerManager) -> Self {
        self.resource.tokenizers = tokenizers;
        self
    }

    pub fn set_recent_segment_stat(
        mut self,
        recent_segment_stat: Option<&'a Arc<SegmentStat>>,
    ) -> Self {
        self.resource.recent_segment_stat = recent_segment_stat;
        self
    }

    pub fn build(self) -> IndexWriterResource<'a> {
        self.resource
    }
}

impl<'a> IndexWriterResource<'a> {
    pub fn tokenizers(&self) -> &'a TokenizerManager {
        self.tokenizers
    }

    pub fn recent_segment_stat(&self) -> Option<&'a Arc<SegmentStat>> {
        self.recent_segment_stat
    }
}
