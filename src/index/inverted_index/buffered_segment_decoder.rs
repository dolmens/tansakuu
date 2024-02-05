use std::io::{self};

use crate::{
    postings::{BuildingPostingReader, DocListBlock, PostingRead},
    DocId,
};

use super::{
    persistent_segment_posting_reader::PersistentSegmentPostingReader,
    segment_posting::{BuildingSegmentPosting, PersistentSegmentPosting, SegmentPostingData},
    SegmentPosting,
};

pub struct BufferedSegmentDecoder<'a> {
    base_docid: DocId,
    decoder_inner: SegmentDecoderInner<'a>,
}

pub enum SegmentDecoderInner<'a> {
    Persistent(PersistentSegmentDecoder<'a>),
    Building(BuildingSegmentDecoder<'a>),
}

pub struct PersistentSegmentDecoder<'a> {
    posting_reader: PersistentSegmentPostingReader<'a>,
}

pub struct BuildingSegmentDecoder<'a> {
    building_posting_reader: BuildingPostingReader<'a>,
}

impl<'a> BufferedSegmentDecoder<'a> {
    pub fn open(segment_posting: &'static SegmentPosting<'a>) -> BufferedSegmentDecoder<'a> {
        Self {
            base_docid: segment_posting.base_docid(),
            decoder_inner: SegmentDecoderInner::open(segment_posting),
        }
    }

    pub fn decode_one_block(
        &mut self,
        docid: DocId,
        doc_list_block: &mut DocListBlock,
    ) -> io::Result<bool> {
        let docid = if docid > self.base_docid {
            docid - self.base_docid
        } else {
            0
        };
        if self.decoder_inner.decode_one_block(docid, doc_list_block)? {
            doc_list_block.base_docid += self.base_docid;
            doc_list_block.last_docid += self.base_docid;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl<'a> SegmentDecoderInner<'a> {
    pub fn open(segment_posting: &'static SegmentPosting<'a>) -> SegmentDecoderInner<'a> {
        match segment_posting.posting_data() {
            SegmentPostingData::Persistent(persistent_segment_posting) => {
                Self::Persistent(PersistentSegmentDecoder::open(persistent_segment_posting))
            }
            SegmentPostingData::Building(building_segment_posting) => {
                Self::Building(BuildingSegmentDecoder::open(building_segment_posting))
            }
        }
    }

    pub fn decode_one_block(
        &mut self,
        docid: DocId,
        doc_list_block: &mut DocListBlock,
    ) -> io::Result<bool> {
        match self {
            Self::Persistent(persistent_segment_decoder) => {
                persistent_segment_decoder.decode_one_block(docid, doc_list_block)
            }
            Self::Building(building_segment_decoder) => {
                building_segment_decoder.decode_one_block(docid, doc_list_block)
            }
        }
    }
}

impl<'a> PersistentSegmentDecoder<'a> {
    pub fn open(
        persistent_segment_posting: &'static PersistentSegmentPosting<'a>,
    ) -> PersistentSegmentDecoder<'a> {
        let posting_reader = PersistentSegmentPostingReader::open(
            persistent_segment_posting.term_info.clone(),
            persistent_segment_posting.index_data,
        );

        Self { posting_reader }
    }

    pub fn decode_one_block(
        &mut self,
        docid: DocId,
        doc_list_block: &mut DocListBlock,
    ) -> io::Result<bool> {
        self.posting_reader.decode_one_block(docid, doc_list_block)
    }
}

impl<'a> BuildingSegmentDecoder<'a> {
    pub fn open(
        segment_posting: &'static BuildingSegmentPosting<'a>,
    ) -> BuildingSegmentDecoder<'a> {
        Self {
            building_posting_reader: BuildingPostingReader::open(
                segment_posting.building_posting_list,
            ),
        }
    }

    pub fn decode_one_block(
        &mut self,
        docid: DocId,
        doc_list_block: &mut DocListBlock,
    ) -> io::Result<bool> {
        self.building_posting_reader
            .decode_one_block(docid, doc_list_block)
    }
}
