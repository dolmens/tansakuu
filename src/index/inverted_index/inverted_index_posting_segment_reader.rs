use std::io;

use crate::{
    postings::{positions::PositionListBlock, BuildingPostingReader, DocListBlock, PostingRead},
    DocId,
};

use super::{
    persistent_segment_posting_reader::PersistentSegmentPostingReader,
    segment_posting::{BuildingSegmentPosting, PersistentSegmentPosting, SegmentPostingData},
    SegmentPosting,
};

pub struct InvertedIndexPostingSegmentReader<'a> {
    base_docid: DocId,
    inner_reader: SegmentReaderInner<'a>,
}

enum SegmentReaderInner<'a> {
    Persistent(PersistentSegmentReader<'a>),
    Building(BuildingSegmentReader<'a>),
}

struct PersistentSegmentReader<'a> {
    posting_reader: PersistentSegmentPostingReader<'a>,
}

struct BuildingSegmentReader<'a> {
    building_posting_reader: BuildingPostingReader<'a>,
}

impl<'a> InvertedIndexPostingSegmentReader<'a> {
    pub fn open(
        segment_posting: &'static SegmentPosting<'a>,
    ) -> InvertedIndexPostingSegmentReader<'a> {
        Self {
            base_docid: segment_posting.base_docid(),
            inner_reader: SegmentReaderInner::open(segment_posting),
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
        if self.inner_reader.decode_one_block(docid, doc_list_block)? {
            doc_list_block.base_docid += self.base_docid;
            doc_list_block.last_docid += self.base_docid;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn decode_one_position_block(
        &mut self,
        from_ttf: u64,
        position_list_block: &mut PositionListBlock,
    ) -> io::Result<bool> {
        self.inner_reader
            .decode_one_position_block(from_ttf, position_list_block)
    }
}

impl<'a> SegmentReaderInner<'a> {
    pub fn open(segment_posting: &'static SegmentPosting<'a>) -> SegmentReaderInner<'a> {
        match segment_posting.posting_data() {
            SegmentPostingData::Persistent(persistent_segment_posting) => {
                Self::Persistent(PersistentSegmentReader::open(persistent_segment_posting))
            }
            SegmentPostingData::Building(building_segment_posting) => {
                Self::Building(BuildingSegmentReader::open(building_segment_posting))
            }
        }
    }

    pub fn decode_one_block(
        &mut self,
        docid: DocId,
        doc_list_block: &mut DocListBlock,
    ) -> io::Result<bool> {
        match self {
            Self::Persistent(persistent_segment_reader) => {
                persistent_segment_reader.decode_one_block(docid, doc_list_block)
            }
            Self::Building(building_segment_reader) => {
                building_segment_reader.decode_one_block(docid, doc_list_block)
            }
        }
    }

    pub fn decode_one_position_block(
        &mut self,
        from_ttf: u64,
        position_list_block: &mut PositionListBlock,
    ) -> io::Result<bool> {
        match self {
            Self::Persistent(persistent_segment_reader) => {
                persistent_segment_reader.decode_one_position_block(from_ttf, position_list_block)
            }
            Self::Building(building_segment_reader) => {
                building_segment_reader.decode_one_position_block(from_ttf, position_list_block)
            }
        }
    }
}

impl<'a> PersistentSegmentReader<'a> {
    pub fn open(
        persistent_segment_posting: &'static PersistentSegmentPosting<'a>,
    ) -> PersistentSegmentReader<'a> {
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

    pub fn decode_one_position_block(
        &mut self,
        from_ttf: u64,
        position_list_block: &mut PositionListBlock,
    ) -> io::Result<bool> {
        self.posting_reader
            .decode_one_position_block(from_ttf, position_list_block)
    }
}

impl<'a> BuildingSegmentReader<'a> {
    pub fn open(segment_posting: &'static BuildingSegmentPosting<'a>) -> BuildingSegmentReader<'a> {
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

    pub fn decode_one_position_block(
        &mut self,
        from_ttf: u64,
        position_list_block: &mut PositionListBlock,
    ) -> io::Result<bool> {
        self.building_posting_reader
            .decode_one_position_block(from_ttf, position_list_block)
    }
}
