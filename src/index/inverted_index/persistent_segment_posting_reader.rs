use std::io::Cursor;

use crate::postings::{
    positions::PositionListDecoder, skip_list::SkipListReader, DocListDecoder, PostingRead,
    PostingReader, TermInfo,
};

use super::InvertedIndexPersistentSegmentData;

pub type PersistentSegmentDataReader<'a> = Cursor<&'a [u8]>;

pub type PersistentSegmentSkipListReader<'a> = SkipListReader<&'a [u8]>;

pub type PersistentSegmentDocListDecoder<'a> =
    DocListDecoder<PersistentSegmentDataReader<'a>, PersistentSegmentSkipListReader<'a>>;

pub type PersistentSegmentPositionListDecoder<'a> =
    PositionListDecoder<PersistentSegmentDataReader<'a>, PersistentSegmentSkipListReader<'a>>;

pub struct PersistentSegmentPostingReader<'a> {
    posting_reader: PostingReader<
        PersistentSegmentDocListDecoder<'a>,
        PersistentSegmentPositionListDecoder<'a>,
    >,
}

impl<'a> PersistentSegmentPostingReader<'a> {
    pub fn open(term_info: TermInfo, index_data: &'a InvertedIndexPersistentSegmentData) -> Self {
        let posting_format = index_data.posting_format.clone();
        let doc_list_format = posting_format.doc_list_format().clone();

        let doc_list_data = &index_data.doc_list_data.as_slice()[term_info.doc_list_range()];
        let doc_list_data = Cursor::new(doc_list_data);
        let skip_list_data = &index_data.skip_list_data.as_slice()[term_info.skip_list_range()];

        let doc_list_decoder = DocListDecoder::open(
            doc_list_format,
            term_info.doc_count,
            doc_list_data,
            term_info.skip_list_item_count,
            skip_list_data,
        );

        let position_list_decoder = if posting_format.has_position_list() {
            let position_list_data =
                &index_data.position_list_data.as_slice()[term_info.position_list_range()];
            let position_list_data = Cursor::new(position_list_data);
            let position_skip_list_data = &index_data.position_skip_list_data.as_slice()
                [term_info.position_skip_list_range()];

            Some(PositionListDecoder::open(
                term_info.position_list_item_count,
                position_list_data,
                term_info.position_skip_list_item_count,
                position_skip_list_data,
            ))
        } else {
            None
        };

        let posting_reader =
            PostingReader::new(posting_format, doc_list_decoder, position_list_decoder);

        Self { posting_reader }
    }
}

impl<'a> PostingRead for PersistentSegmentPostingReader<'a> {
    fn decode_one_block(
        &mut self,
        docid: crate::DocId,
        doc_list_block: &mut crate::postings::DocListBlock,
    ) -> std::io::Result<bool> {
        self.posting_reader.decode_one_block(docid, doc_list_block)
    }

    fn decode_one_position_block(
        &mut self,
        from_ttf: u64,
        position_list_block: &mut crate::postings::positions::PositionListBlock,
    ) -> std::io::Result<bool> {
        self.posting_reader
            .decode_one_position_block(from_ttf, position_list_block)
    }
}
