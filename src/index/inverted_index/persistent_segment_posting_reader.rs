use std::io::Cursor;

use crate::postings::{
    positions::{PositionListDecode, PositionListDecoder},
    skip_list::SkipListReader,
    DocListDecode, DocListDecoder, PostingFormat, PostingRead, TermInfo,
};

use super::InvertedIndexPersistentSegmentData;

pub type PersistentSegmentDataReader<'a> = Cursor<&'a [u8]>;

pub type PersistentSegmentSkipListReader<'a> = SkipListReader<&'a [u8]>;

pub type PersistentSegmentDocListDecoder<'a> =
    DocListDecoder<PersistentSegmentDataReader<'a>, PersistentSegmentSkipListReader<'a>>;

pub type PersistentSegmentPositionListDecoder<'a> =
    PositionListDecoder<PersistentSegmentDataReader<'a>, PersistentSegmentSkipListReader<'a>>;

pub struct PersistentSegmentPostingReader<'a> {
    doc_list_decoder: PersistentSegmentDocListDecoder<'a>,
    position_list_decoder: Option<PersistentSegmentPositionListDecoder<'a>>,
    term_info: TermInfo,
    posting_format: PostingFormat,
    index_data: &'a InvertedIndexPersistentSegmentData,
}

impl<'a> PersistentSegmentPostingReader<'a> {
    pub fn open(term_info: TermInfo, index_data: &'a InvertedIndexPersistentSegmentData) -> Self {
        let posting_format = index_data.posting_format.clone();
        let doc_list_format = posting_format.doc_list_format().clone();

        let doc_list_data = &index_data.doc_list_data.as_slice()[term_info.doc_list_range()];
        let doc_list_data = Cursor::new(doc_list_data);
        let skip_list_data = &index_data.skip_list_data.as_slice()[term_info.skip_list_range()];

        let doc_list_decoder =
            DocListDecoder::open(doc_list_format, term_info.df, doc_list_data, skip_list_data);

        Self {
            doc_list_decoder,
            position_list_decoder: None,
            term_info,
            posting_format,
            index_data,
        }
    }

    fn init_position_list_decoder(&mut self) {
        let position_list_data =
            &self.index_data.position_list_data.as_slice()[self.term_info.position_list_range()];
        let position_list_data = Cursor::new(position_list_data);
        let position_skip_list_data = &self.index_data.position_skip_list_data.as_slice()
            [self.term_info.position_skip_list_range()];

        self.position_list_decoder = Some(PositionListDecoder::open(
            self.term_info.ttf,
            position_list_data,
            position_skip_list_data,
        ));
    }
}

impl<'a> PostingRead for PersistentSegmentPostingReader<'a> {
    fn decode_doc_buffer(
        &mut self,
        docid: crate::DocId,
        doc_list_block: &mut crate::postings::DocListBlock,
    ) -> std::io::Result<bool> {
        self.doc_list_decoder
            .decode_doc_buffer(docid, doc_list_block)
    }

    fn decode_tf_buffer(
        &mut self,
        doc_list_block: &mut crate::postings::DocListBlock,
    ) -> std::io::Result<bool> {
        self.doc_list_decoder.decode_tf_buffer(doc_list_block)
    }

    fn decode_fieldmask_buffer(
        &mut self,
        doc_list_block: &mut crate::postings::DocListBlock,
    ) -> std::io::Result<bool> {
        self.doc_list_decoder
            .decode_fieldmask_buffer(doc_list_block)
    }

    fn decode_one_block(
        &mut self,
        docid: crate::DocId,
        doc_list_block: &mut crate::postings::DocListBlock,
    ) -> std::io::Result<bool> {
        self.doc_list_decoder
            .decode_one_block(docid, doc_list_block)
    }

    fn decode_position_buffer(
        &mut self,
        from_ttf: u64,
        position_list_block: &mut crate::postings::positions::PositionListBlock,
    ) -> std::io::Result<bool> {
        if !self.posting_format.has_position_list() {
            return Ok(false);
        }
        if self.position_list_decoder.is_none() {
            self.init_position_list_decoder();
        }
        self.position_list_decoder
            .as_mut()
            .unwrap()
            .decode_position_buffer(from_ttf, position_list_block)
    }

    fn decode_next_position_record(
        &mut self,
        position_list_block: &mut crate::postings::positions::PositionListBlock,
    ) -> std::io::Result<bool> {
        if !self.posting_format.has_position_list() {
            return Ok(false);
        }
        if self.position_list_decoder.is_none() {
            self.init_position_list_decoder();
        }
        self.position_list_decoder
            .as_mut()
            .unwrap()
            .decode_next_record(position_list_block)
    }
}
