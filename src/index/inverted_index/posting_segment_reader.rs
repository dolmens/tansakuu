use std::io;

use crate::{
    postings::{positions::PositionListBlock, BuildingPostingReader, DocListBlock, PostingRead},
    DocId,
};

use super::{
    persistent_posting_reader::PersistentPostingReader,
    segment_posting::{BuildingSegmentPosting, PersistentSegmentPosting, SegmentPostingData},
    SegmentPosting,
};

pub struct PostingSegmentReader<'a> {
    base_docid: DocId,
    inner_reader: SegmentReaderInner<'a>,
}

enum SegmentReaderInner<'a> {
    Persistent(PersistentSegmentReader<'a>),
    Building(BuildingSegmentReader<'a>),
}

struct PersistentSegmentReader<'a> {
    posting_reader: PersistentPostingReader<'a>,
}

struct BuildingSegmentReader<'a> {
    building_posting_reader: BuildingPostingReader<'a>,
}

impl<'a> PostingSegmentReader<'a> {
    pub fn open(segment_posting: &'a SegmentPosting<'a>) -> Self {
        Self {
            base_docid: segment_posting.base_docid(),
            inner_reader: SegmentReaderInner::open(segment_posting),
        }
    }

    pub fn decode_doc_buffer(
        &mut self,
        docid: DocId,
        doc_list_block: &mut DocListBlock,
    ) -> io::Result<bool> {
        let docid = if docid > self.base_docid {
            docid - self.base_docid
        } else {
            0
        };
        if self.inner_reader.decode_doc_buffer(docid, doc_list_block)? {
            doc_list_block.base_docid += self.base_docid;
            doc_list_block.last_docid += self.base_docid;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn decode_tf_buffer(&mut self, doc_list_block: &mut DocListBlock) -> io::Result<bool> {
        self.inner_reader.decode_tf_buffer(doc_list_block)
    }

    pub fn decode_fieldmask_buffer(
        &mut self,
        doc_list_block: &mut DocListBlock,
    ) -> io::Result<bool> {
        self.inner_reader.decode_fieldmask_buffer(doc_list_block)
    }

    pub fn decode_position_buffer(
        &mut self,
        from_ttf: u64,
        position_list_block: &mut PositionListBlock,
    ) -> io::Result<bool> {
        self.inner_reader
            .decode_position_buffer(from_ttf, position_list_block)
    }

    pub fn decode_next_position_record(
        &mut self,
        position_list_block: &mut PositionListBlock,
    ) -> io::Result<bool> {
        self.inner_reader
            .decode_next_position_record(position_list_block)
    }
}

impl<'a> SegmentReaderInner<'a> {
    pub fn open(segment_posting: &'a SegmentPosting<'a>) -> Self {
        match segment_posting.posting_data() {
            SegmentPostingData::Persistent(persistent_segment_posting) => {
                Self::Persistent(PersistentSegmentReader::open(persistent_segment_posting))
            }
            SegmentPostingData::Building(building_segment_posting) => {
                Self::Building(BuildingSegmentReader::open(building_segment_posting))
            }
        }
    }
    pub fn decode_doc_buffer(
        &mut self,
        docid: DocId,
        doc_list_block: &mut DocListBlock,
    ) -> io::Result<bool> {
        match self {
            Self::Persistent(persistent_segment_reader) => {
                persistent_segment_reader.decode_doc_buffer(docid, doc_list_block)
            }
            Self::Building(building_segment_reader) => {
                building_segment_reader.decode_doc_buffer(docid, doc_list_block)
            }
        }
    }

    pub fn decode_tf_buffer(&mut self, doc_list_block: &mut DocListBlock) -> io::Result<bool> {
        match self {
            Self::Persistent(persistent_segment_reader) => {
                persistent_segment_reader.decode_tf_buffer(doc_list_block)
            }
            Self::Building(building_segment_reader) => {
                building_segment_reader.decode_tf_buffer(doc_list_block)
            }
        }
    }

    pub fn decode_fieldmask_buffer(
        &mut self,
        doc_list_block: &mut DocListBlock,
    ) -> io::Result<bool> {
        match self {
            Self::Persistent(persistent_segment_reader) => {
                persistent_segment_reader.decode_fieldmask_buffer(doc_list_block)
            }
            Self::Building(building_segment_reader) => {
                building_segment_reader.decode_fieldmask_buffer(doc_list_block)
            }
        }
    }

    pub fn decode_position_buffer(
        &mut self,
        from_ttf: u64,
        position_list_block: &mut PositionListBlock,
    ) -> io::Result<bool> {
        match self {
            Self::Persistent(persistent_segment_reader) => {
                persistent_segment_reader.decode_position_buffer(from_ttf, position_list_block)
            }
            Self::Building(building_segment_reader) => {
                building_segment_reader.decode_position_buffer(from_ttf, position_list_block)
            }
        }
    }

    pub fn decode_next_position_record(
        &mut self,
        position_list_block: &mut PositionListBlock,
    ) -> io::Result<bool> {
        match self {
            Self::Persistent(persistent_segment_reader) => {
                persistent_segment_reader.decode_next_position_record(position_list_block)
            }
            Self::Building(building_segment_reader) => {
                building_segment_reader.decode_next_position_record(position_list_block)
            }
        }
    }
}

impl<'a> PersistentSegmentReader<'a> {
    pub fn open(persistent_segment_posting: &'a PersistentSegmentPosting<'a>) -> Self {
        let posting_reader = PersistentPostingReader::open(
            persistent_segment_posting.term_info.clone(),
            persistent_segment_posting.posting_data,
        );

        Self { posting_reader }
    }

    pub fn decode_doc_buffer(
        &mut self,
        docid: DocId,
        doc_list_block: &mut DocListBlock,
    ) -> io::Result<bool> {
        self.posting_reader.decode_doc_buffer(docid, doc_list_block)
    }

    pub fn decode_tf_buffer(&mut self, doc_list_block: &mut DocListBlock) -> io::Result<bool> {
        self.posting_reader.decode_tf_buffer(doc_list_block)
    }

    pub fn decode_fieldmask_buffer(
        &mut self,
        doc_list_block: &mut DocListBlock,
    ) -> io::Result<bool> {
        self.posting_reader.decode_fieldmask_buffer(doc_list_block)
    }

    pub fn decode_position_buffer(
        &mut self,
        from_ttf: u64,
        position_list_block: &mut PositionListBlock,
    ) -> io::Result<bool> {
        self.posting_reader
            .decode_position_buffer(from_ttf, position_list_block)
    }

    pub fn decode_next_position_record(
        &mut self,
        position_list_block: &mut PositionListBlock,
    ) -> io::Result<bool> {
        self.posting_reader
            .decode_next_position_record(position_list_block)
    }
}

impl<'a> BuildingSegmentReader<'a> {
    pub fn open(segment_posting: &'a BuildingSegmentPosting<'a>) -> Self {
        Self {
            building_posting_reader: BuildingPostingReader::open(
                segment_posting.building_posting_list,
            ),
        }
    }

    pub fn decode_doc_buffer(
        &mut self,
        docid: DocId,
        doc_list_block: &mut DocListBlock,
    ) -> io::Result<bool> {
        self.building_posting_reader
            .decode_doc_buffer(docid, doc_list_block)
    }

    pub fn decode_tf_buffer(&mut self, doc_list_block: &mut DocListBlock) -> io::Result<bool> {
        self.building_posting_reader
            .decode_tf_buffer(doc_list_block)
    }

    pub fn decode_fieldmask_buffer(
        &mut self,
        doc_list_block: &mut DocListBlock,
    ) -> io::Result<bool> {
        self.building_posting_reader
            .decode_fieldmask_buffer(doc_list_block)
    }

    pub fn decode_position_buffer(
        &mut self,
        from_ttf: u64,
        position_list_block: &mut PositionListBlock,
    ) -> io::Result<bool> {
        self.building_posting_reader
            .decode_position_buffer(from_ttf, position_list_block)
    }

    pub fn decode_next_position_record(
        &mut self,
        position_list_block: &mut PositionListBlock,
    ) -> io::Result<bool> {
        self.building_posting_reader
            .decode_next_position_record(position_list_block)
    }
}
