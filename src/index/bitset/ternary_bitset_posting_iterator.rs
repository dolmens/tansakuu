use crate::{
    index::PostingIterator,
    table::SegmentMetaRegistry,
    util::{ExpandableBitset, ImmutableBitset},
    DocId, END_DOCID, INVALID_DOCID,
};

use super::{ImmutableBitsetPostingIterator, TernaryBuildingBitsetPostingIterator};

pub struct TernaryBitsetPostingIterator<'a, const POSITIVE: bool> {
    current_docid: DocId,
    segment_cursor: usize,
    segment_meta_registry: SegmentMetaRegistry,
    persistent_segments: Vec<ImmutableBitsetPostingIterator<'a>>,
    building_segments: Vec<TernaryBuildingBitsetPostingIterator<'a, POSITIVE>>,
}

impl<'a, const POSITIVE: bool> TernaryBitsetPostingIterator<'a, POSITIVE> {
    pub fn new(
        segment_meta_registry: SegmentMetaRegistry,
        _persistent_segment_datas: &[&'a ImmutableBitset],
        building_segment_datas: &[(&'a ExpandableBitset, Option<&'a ExpandableBitset>)],
    ) -> Self {
        let persistent_segments = vec![];
        let building_segments: Vec<_> = building_segment_datas
            .iter()
            .map(|(values, nulls)| {
                TernaryBuildingBitsetPostingIterator::<POSITIVE>::new(*values, *nulls)
            })
            .collect();

        Self {
            current_docid: INVALID_DOCID,
            segment_cursor: 0,
            segment_meta_registry,
            persistent_segments,
            building_segments,
        }
    }
}

impl<'a, const POSITIVE: bool> PostingIterator for TernaryBitsetPostingIterator<'a, POSITIVE> {
    fn seek(&mut self, docid: crate::DocId) -> std::io::Result<crate::DocId> {
        let docid = if docid < 0 { 0 } else { docid };
        if docid <= self.current_docid {
            return Ok(self.current_docid);
        }

        if let Some(segment_cursor) = self
            .segment_meta_registry
            .locate_segment_from(docid, self.segment_cursor)
        {
            let mut segment_cursor = segment_cursor;
            while segment_cursor < self.persistent_segments.len() {
                segment_cursor += 1;
            }
            while segment_cursor < self.persistent_segments.len() + self.building_segments.len() {
                let segment_base_docid = self
                    .segment_meta_registry
                    .segment(segment_cursor)
                    .base_docid();
                let building_segment_cursor = segment_cursor - self.persistent_segments.len();
                let got_docid = self.building_segments[building_segment_cursor]
                    .seek(docid - segment_base_docid);
                if got_docid != END_DOCID {
                    self.segment_cursor = segment_cursor;
                    let docid = got_docid + segment_base_docid;
                    self.current_docid = docid;
                    return Ok(docid);
                }
                segment_cursor += 1;
            }
        }
        self.current_docid = END_DOCID;
        Ok(END_DOCID)
    }
}
