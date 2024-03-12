use std::{
    cell::RefCell,
    collections::{hash_map::RandomState, HashMap},
    rc::Rc,
    sync::Arc,
};

use tantivy_tokenizer_api::{BoxTokenStream, Token, TokenStream};

use crate::{
    document::{OwnedValue, Value},
    index::{IndexWriter, IndexWriterResource},
    postings::{BuildingPostingList, BuildingPostingWriter, PostingFormat, PostingFormatBuilder},
    schema::{DataType, FieldRef, IndexRef, IndexType},
    tokenizer::{
        ChainedTokenStream, OwnedMultiTokenStream, OwnedTextAnalyzerStream, OwnedTokenStream,
        PreTokenizedStream, TextAnalyzer,
    },
    util::{ha3_capacity_policy::Ha3CapacityPolicy, layered_hashmap::LayeredHashMapWriter},
    DocId, FIELD_POS_GAP, HASHMAP_INITIAL_CAPACITY,
};

use super::{InvertedIndexBuildingSegmentData, TokenHasher};

type PostingTable = LayeredHashMapWriter<u64, BuildingPostingList, RandomState, Ha3CapacityPolicy>;

pub struct InvertedIndexWriter {
    posting_table: PostingTable,
    posting_writers: HashMap<u64, Rc<RefCell<BuildingPostingWriter>>>,
    modified_postings: HashMap<u64, Rc<RefCell<BuildingPostingWriter>>>,
    index_data: Arc<InvertedIndexBuildingSegmentData>,
    tokenizer: TextAnalyzer,
    posting_format: PostingFormat,
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
        let hasher_builder = RandomState::new();
        let capacity_policy = Ha3CapacityPolicy;
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
        let posting_table =
            PostingTable::with_capacity(hashmap_initial_capacity, hasher_builder, capacity_policy);
        let postings = posting_table.hashmap();

        Self {
            posting_table,
            posting_writers: HashMap::new(),
            modified_postings: HashMap::new(),
            index_data: Arc::new(InvertedIndexBuildingSegmentData::new(index, postings)),
            tokenizer,
            posting_format,
        }
    }

    fn tokenize_field<'a>(
        &mut self,
        field: &FieldRef,
        value: &'a OwnedValue,
    ) -> Option<BoxTokenStream<'a>> {
        match field.data_type() {
            DataType::Str => self.tokenize_str_field(field, value),
            DataType::Text => self.tokenize_text_field(field, value),
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                self.tokenize_i64_field(field, value)
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
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
                let posting_writer = self
                    .posting_writers
                    .entry(hashkey)
                    .or_insert_with(|| {
                        let posting_writer = Rc::new(RefCell::new(BuildingPostingWriter::new(
                            self.posting_format.clone(),
                        )));
                        let building_posting_list =
                            posting_writer.borrow().building_posting_list().clone();
                        self.posting_table.insert(hashkey, building_posting_list);
                        posting_writer
                    })
                    .clone();
                self.modified_postings
                    .entry(hashkey)
                    .or_insert_with(|| posting_writer.clone());

                posting_writer
                    .borrow_mut()
                    .add_pos(field_offset, token.position as u32)
                    .unwrap();
            }
        }
    }

    fn end_document(&mut self, docid: DocId) {
        for (_, posting_writer) in &self.modified_postings {
            posting_writer.borrow_mut().end_doc(docid).unwrap();
        }
        self.modified_postings.clear();
    }

    fn index_data(&self) -> std::sync::Arc<dyn crate::index::IndexSegmentData> {
        self.index_data.clone()
    }
}
