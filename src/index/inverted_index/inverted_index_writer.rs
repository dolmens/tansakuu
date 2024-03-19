use std::sync::Arc;

use tantivy_tokenizer_api::{BoxTokenStream, Token, TokenStream};

use crate::{
    document::{OwnedValue, Value},
    index::{IndexWriter, IndexWriterResource},
    postings::PostingFormatBuilder,
    schema::{FieldRef, FieldType, IndexRef, IndexType},
    tokenizer::{
        ChainedTokenStream, OwnedMultiTokenStream, OwnedTextAnalyzerStream, OwnedTokenStream,
        PreTokenizedStream, TextAnalyzer,
    },
    DocId, FIELD_POS_GAP, HASHMAP_INITIAL_CAPACITY,
};

use super::{InvertedIndexBuildingSegmentData, InvertedIndexPostingWriter, TokenHasher};

pub struct InvertedIndexWriter {
    posting_writer: InvertedIndexPostingWriter,
    index_data: Arc<InvertedIndexBuildingSegmentData>,
    tokenizer: TextAnalyzer,
}

impl InvertedIndexWriter {
    pub fn new(index: IndexRef, writer_resource: &IndexWriterResource) -> Self {
        let index_options = match index.index_type() {
            IndexType::Text(index_options) => index_options,
            _ => {
                panic!("InvertedIndexWriter index non text index.");
            }
        };
        let tokenizer = writer_resource
            .tokenizers()
            .get(
                index_options
                    .tokenizer
                    .as_ref()
                    .map(|s| s.as_str())
                    .unwrap_or("default"),
            )
            .unwrap();
        let posting_format = PostingFormatBuilder::default()
            .with_index_options(index_options)
            .build();
        let hashmap_initial_capacity = writer_resource
            .recent_segment_stat()
            .and_then(|stat| stat.index_term_count.get(index.name()))
            .cloned()
            .unwrap_or(HASHMAP_INITIAL_CAPACITY);
        let hashmap_initial_capacity = if hashmap_initial_capacity > 0 {
            hashmap_initial_capacity
        } else {
            HASHMAP_INITIAL_CAPACITY
        };
        let posting_writer =
            InvertedIndexPostingWriter::new(posting_format, hashmap_initial_capacity);
        let posting_data = posting_writer.posting_data();

        Self {
            posting_writer,
            index_data: Arc::new(InvertedIndexBuildingSegmentData::new(index, posting_data)),
            tokenizer,
        }
    }

    fn tokenize_field<'a>(
        &mut self,
        field: &FieldRef,
        value: &'a OwnedValue,
    ) -> Option<BoxTokenStream<'a>> {
        match field.data_type() {
            FieldType::Str => self.tokenize_str_field(field, value),
            FieldType::Text => self.tokenize_text_field(field, value),
            FieldType::Int8 | FieldType::Int16 | FieldType::Int32 | FieldType::Int64 => {
                self.tokenize_i64_field(field, value)
            }
            FieldType::UInt8 | FieldType::UInt16 | FieldType::UInt32 | FieldType::UInt64 => {
                self.tokenize_u64_field(field, value)
            }
            _ => {
                warn!(
                    "Unsupported index field: {}, {}",
                    field.name(),
                    field.data_type()
                );
                None
            }
        }
    }

    fn tokenize_str_field<'a>(
        &mut self,
        field: &FieldRef,
        value: &OwnedValue,
    ) -> Option<BoxTokenStream<'a>> {
        if field.is_multi() && value.is_array() {
            let values = value.as_array().unwrap();
            let tokens = values
                .enumerate()
                .filter_map(|(pos, value)| value.as_str().map(|s| (pos, s)))
                .map(|(pos, s)| Token {
                    offset_from: 0,
                    offset_to: s.len(),
                    position: pos,
                    position_length: 1,
                    text: s.to_string(),
                })
                .collect::<Vec<_>>();
            Some(OwnedMultiTokenStream::new(tokens).into_boxed_stream())
        } else {
            if let Some(s) = value.as_str() {
                Some(OwnedTokenStream::new(s.to_string()).into_boxed_stream())
            } else {
                None
            }
        }
    }

    fn tokenize_text_field<'a>(
        &mut self,
        field: &FieldRef,
        value: &'a OwnedValue,
    ) -> Option<BoxTokenStream<'a>> {
        if field.is_multi() && value.is_array() {
            let values = value.as_array().unwrap();
            let mut streams = vec![];
            for value in values {
                if let Some(text) = value.as_str() {
                    streams.push(
                        OwnedTextAnalyzerStream::new(self.tokenizer.clone(), text)
                            .into_boxed_stream(),
                    );
                } else if let Some(pretok) = value.as_pre_tokenized_text() {
                    streams.push(PreTokenizedStream::from(pretok.clone()).into_boxed_stream());
                }
            }
            Some(ChainedTokenStream::new(FIELD_POS_GAP, streams).into_boxed_stream())
        } else {
            if let Some(text) = value.as_str() {
                Some(OwnedTextAnalyzerStream::new(self.tokenizer.clone(), text).into_boxed_stream())
            } else if let Some(pretok) = value.as_pre_tokenized_text() {
                Some(PreTokenizedStream::from(pretok.clone()).into_boxed_stream())
            } else {
                None
            }
        }
    }

    fn tokenize_i64_field<'a>(
        &mut self,
        field: &FieldRef,
        value: &OwnedValue,
    ) -> Option<BoxTokenStream<'a>> {
        if field.is_multi() && value.is_array() {
            let values = value.as_array().unwrap();
            let tokens = values
                .enumerate()
                .filter_map(|(pos, value)| value.as_i64().map(|s| (pos, s)))
                .map(|(pos, v)| Token {
                    offset_from: 0,
                    offset_to: 1,
                    position: pos,
                    position_length: 1,
                    text: v.to_string(),
                })
                .collect::<Vec<_>>();
            Some(OwnedMultiTokenStream::new(tokens).into_boxed_stream())
        } else {
            match value.as_i64() {
                Some(value) => {
                    let token = value.to_string();
                    Some(OwnedTokenStream::new(token).into_boxed_stream())
                }
                None => None,
            }
        }
    }

    fn tokenize_u64_field<'a>(
        &mut self,
        field: &FieldRef,
        value: &OwnedValue,
    ) -> Option<BoxTokenStream<'a>> {
        if field.is_multi() && value.is_array() {
            let values = value.as_array().unwrap();
            let tokens = values
                .enumerate()
                .filter_map(|(pos, value)| value.as_u64().map(|s| (pos, s)))
                .map(|(pos, v)| Token {
                    offset_from: 0,
                    offset_to: 1,
                    position: pos,
                    position_length: 1,
                    text: v.to_string(),
                })
                .collect::<Vec<_>>();
            Some(OwnedMultiTokenStream::new(tokens).into_boxed_stream())
        } else {
            match value.as_u64() {
                Some(value) => {
                    let token = value.to_string();
                    Some(OwnedTokenStream::new(token).into_boxed_stream())
                }
                None => None,
            }
        }
    }
}

impl IndexWriter for InvertedIndexWriter {
    fn add_field(&mut self, field: &FieldRef, value: &OwnedValue) {
        if let Some(mut token_stream) = self.tokenize_field(field, value) {
            let field_offset = self
                .index_data
                .index
                .field_offset(field)
                .unwrap_or_default();

            while token_stream.advance() {
                let token = token_stream.token();
                let hashkey = TokenHasher::default().hash_token(token);
                self.posting_writer.add_token_with_position(
                    hashkey,
                    field_offset,
                    token.position as u32,
                );
            }
        }
    }

    fn end_document(&mut self, docid: DocId) {
        self.posting_writer.end_document(docid);
    }

    fn index_data(&self) -> std::sync::Arc<dyn crate::index::IndexSegmentData> {
        self.index_data.clone()
    }
}
