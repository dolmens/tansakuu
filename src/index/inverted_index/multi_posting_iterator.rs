use crate::{index::PostingIterator, postings::PostingFormat, DocId, END_DOCID, INVALID_DOCID};

use super::{PostingSegmentMultiReader, SegmentMultiPosting};

pub struct MultiPostingIterator<'a> {
    segment_reader: Option<PostingSegmentMultiReader<'a>>,
    cursor: usize,
    postings: Vec<SegmentMultiPosting<'a>>,
    posting_format: PostingFormat,
}

impl<'a> MultiPostingIterator<'a> {
    pub fn new(posting_format: PostingFormat, postings: Vec<SegmentMultiPosting<'a>>) -> Self {
        Self {
            segment_reader: None,
            cursor: 0,
            postings,
            posting_format,
        }
    }

    fn move_to_segment(&mut self, docid: DocId) -> bool {
        let cursor = self.locate_segment(self.cursor, docid);
        if cursor >= self.postings.len() {
            return false;
        }

        if self.cursor != cursor || self.segment_reader.is_none() {
            self.cursor = cursor;
            self.segment_reader = Some(PostingSegmentMultiReader::open(
                self.posting_format.doc_list_format().clone(),
                unsafe { std::mem::transmute(&self.postings[self.cursor]) },
            ));
        }
        true
    }

    fn locate_segment(&self, cursor: usize, docid: DocId) -> usize {
        let curr_seg_base_docid = self.segment_base_docid(cursor);
        if curr_seg_base_docid == INVALID_DOCID {
            return cursor;
        }
        let mut cursor = cursor;
        let mut next_seg_base_docid = self.segment_base_docid(cursor + 1);
        while next_seg_base_docid != INVALID_DOCID && docid >= next_seg_base_docid {
            cursor += 1;
            next_seg_base_docid = self.segment_base_docid(cursor + 1);
        }
        cursor
    }

    fn segment_base_docid(&self, cursor: usize) -> DocId {
        if cursor >= self.postings.len() {
            INVALID_DOCID
        } else {
            self.postings[cursor].base_docid()
        }
    }
}

impl<'a> PostingIterator for MultiPostingIterator<'a> {
    fn seek(&mut self, docid: crate::DocId) -> std::io::Result<crate::DocId> {
        loop {
            if !self.move_to_segment(docid) {
                return Ok(END_DOCID);
            }

            let result_docid = self.segment_reader.as_mut().unwrap().seek(docid)?;
            if result_docid != END_DOCID {
                return Ok(result_docid);
            }
            self.segment_reader = None;
            self.cursor += 1;
        }
    }

    fn seek_pos(&mut self, _pos: u32) -> std::io::Result<u32> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use crate::{
        index::{
            inverted_index::{
                BuildingSegmentPosting, SegmentMultiPosting, SegmentMultiPostingData,
            },
            PostingIterator,
        },
        postings::{BuildingPostingWriter, PostingFormat},
        DocId, DOC_LIST_BLOCK_LEN, END_DOCID,
    };

    use super::MultiPostingIterator;

    #[test]
    fn test_single_segment() -> io::Result<()> {
        const BLOCK_LEN: usize = DOC_LIST_BLOCK_LEN;
        let posting_format = PostingFormat::default();
        let doc_list_format = posting_format.doc_list_format().clone();
        let mut posting_writer1 = BuildingPostingWriter::new(posting_format.clone());
        for i in 0..BLOCK_LEN + 3 {
            posting_writer1.add_pos(0, 0)?;
            posting_writer1.end_doc((i * 3) as DocId)?;
        }
        let mut posting_writer2 = BuildingPostingWriter::new(posting_format.clone());
        for i in 0..BLOCK_LEN + 3 {
            posting_writer2.add_pos(0, 0)?;
            posting_writer2.end_doc((i * 3 + 2) as DocId)?;
        }
        let mut posting_writer3 = BuildingPostingWriter::new(posting_format.clone());
        for i in 0..BLOCK_LEN + 3 {
            posting_writer3.add_pos(0, 0)?;
            posting_writer3.end_doc((i * 3 + 1) as DocId)?;
        }

        let segment_posting1 = BuildingSegmentPosting {
            building_posting_list: posting_writer1.building_posting_list(),
        };
        let segment_posting2 = BuildingSegmentPosting {
            building_posting_list: posting_writer2.building_posting_list(),
        };
        let segment_posting3 = BuildingSegmentPosting {
            building_posting_list: posting_writer3.building_posting_list(),
        };
        let base_docid = 10;
        let multi_posting_data = SegmentMultiPostingData::Building(vec![
            segment_posting1,
            segment_posting2,
            segment_posting3,
        ]);
        let segment_multi_posting = SegmentMultiPosting::new(base_docid, multi_posting_data);

        let mut posting_iterator =
            MultiPostingIterator::new(PostingFormat::default(), vec![segment_multi_posting]);

        let mut docids = vec![];
        let mut docid = 0;
        loop {
            docid = posting_iterator.seek(docid)?;
            if docid == END_DOCID {
                break;
            }
            docids.push(docid);
        }

        let expect: Vec<_> = (0..((BLOCK_LEN + 3) * 3) as DocId)
            .map(|docid| docid + base_docid)
            .collect();
        assert_eq!(docids, expect);

        Ok(())
    }

    #[test]
    fn test_multi_segment() -> io::Result<()> {
        const BLOCK_LEN: usize = DOC_LIST_BLOCK_LEN;
        let posting_format = PostingFormat::default();
        let mut posting_writer1 = BuildingPostingWriter::new(posting_format.clone());
        for i in 0..BLOCK_LEN + 3 {
            posting_writer1.add_pos(0, 0)?;
            posting_writer1.end_doc((i * 3) as DocId)?;
        }
        let mut posting_writer2 = BuildingPostingWriter::new(posting_format.clone());
        for i in 0..BLOCK_LEN + 3 {
            posting_writer2.add_pos(0, 0)?;
            posting_writer2.end_doc((i * 3 + 2) as DocId)?;
        }
        let mut posting_writer3 = BuildingPostingWriter::new(posting_format.clone());
        for i in 0..BLOCK_LEN + 3 {
            posting_writer3.add_pos(0, 0)?;
            posting_writer3.end_doc((i * 3 + 1) as DocId)?;
        }

        let segment_posting1 = BuildingSegmentPosting {
            building_posting_list: posting_writer1.building_posting_list(),
        };
        let segment_posting2 = BuildingSegmentPosting {
            building_posting_list: posting_writer2.building_posting_list(),
        };
        let segment_posting3 = BuildingSegmentPosting {
            building_posting_list: posting_writer3.building_posting_list(),
        };
        let base_docid = 0;
        let multi_posting_data = SegmentMultiPostingData::Building(vec![
            segment_posting1,
            segment_posting2,
            segment_posting3,
        ]);
        let segment_multi_posting = SegmentMultiPosting::new(base_docid, multi_posting_data);

        let second_segment_posting1 = BuildingSegmentPosting {
            building_posting_list: posting_writer1.building_posting_list(),
        };
        let second_segment_posting2 = BuildingSegmentPosting {
            building_posting_list: posting_writer2.building_posting_list(),
        };
        let second_segment_posting3 = BuildingSegmentPosting {
            building_posting_list: posting_writer3.building_posting_list(),
        };
        let second_base_docid = 1000;
        let second_multi_posting_data = SegmentMultiPostingData::Building(vec![
            second_segment_posting1,
            second_segment_posting2,
            second_segment_posting3,
        ]);
        let second_segment_multi_posting =
            SegmentMultiPosting::new(second_base_docid, second_multi_posting_data);

        let mut posting_iterator = MultiPostingIterator::new(
            PostingFormat::default(),
            vec![segment_multi_posting, second_segment_multi_posting],
        );

        let mut docids = vec![];
        let mut docid = 0;
        loop {
            docid = posting_iterator.seek(docid)?;
            if docid == END_DOCID {
                break;
            }
            docids.push(docid);
        }

        let expect: Vec<_> = (0..((BLOCK_LEN + 3) * 3) as DocId)
            .chain((0..((BLOCK_LEN + 3) * 3) as DocId).map(|docid| docid + second_base_docid))
            .collect();
        assert_eq!(docids, expect);

        Ok(())
    }
}
