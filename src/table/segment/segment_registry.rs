use crate::DocId;

use super::{BuildingDocCount, DocCountVariant, SegmentMeta};

#[derive(Clone, Default)]
pub struct SegmentRegistry {
    pub segments: Vec<SegmentInfo>,
}

#[derive(Clone)]
pub struct SegmentInfo {
    pub base_docid: DocId,
    pub doc_count: DocCountVariant,
}

impl SegmentRegistry {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            segments: Vec::with_capacity(capacity),
        }
    }

    pub fn add_persistent_segment(&mut self, segment_meta: &SegmentMeta) {
        self.segments.push(SegmentInfo::new_static_segment(
            segment_meta.base_docid(),
            segment_meta.doc_count(),
        ));
    }

    pub fn add_building_segment(
        &mut self,
        segment_meta: &SegmentMeta,
        doc_count: &BuildingDocCount,
    ) {
        if segment_meta.doc_count() > 0 {
            self.add_static_segment(segment_meta.base_docid(), segment_meta.doc_count());
        } else {
            self.add_dynamic_segment(segment_meta.base_docid(), doc_count.clone());
        }
    }

    pub fn clear(&mut self) {
        self.segments.clear();
    }

    pub fn add_static_segment(&mut self, base_docid: DocId, doc_count: usize) {
        self.segments
            .push(SegmentInfo::new_static_segment(base_docid, doc_count));
    }

    pub fn add_dynamic_segment(&mut self, base_docid: DocId, doc_count: BuildingDocCount) {
        self.segments
            .push(SegmentInfo::new_dynamic_segment(base_docid, doc_count));
    }

    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    pub fn docid_in_segment(&self, docid: DocId, segment_cursor: usize) -> DocId {
        let segment = &self.segments[segment_cursor];
        debug_assert!(
            docid >= segment.base_docid
                && docid < segment.base_docid + (segment.doc_count() as DocId)
        );
        docid - segment.base_docid
    }

    pub fn segment_base_docid(&self, segment_cursor: usize) -> DocId {
        let segment = &self.segments[segment_cursor];
        segment.base_docid
    }

    pub fn segment_end_docid(&self, segment_cursor: usize) -> DocId {
        let segment = &self.segments[segment_cursor];
        segment.base_docid + (segment.doc_count() as DocId)
    }

    pub fn segment_doc_count(&self, segment_cursor: usize) -> usize {
        let segment = &self.segments[segment_cursor];
        segment.doc_count()
    }

    pub fn locate_segment(&self, docid: DocId) -> Option<usize> {
        for (i, segment) in self.segments.iter().enumerate() {
            if docid < segment.base_docid + (segment.doc_count() as DocId) {
                return Some(i);
            }
        }
        None
    }

    pub fn locate_segment_from(&self, docid: DocId, current_cursor: usize) -> Option<usize> {
        for (i, segment) in self.segments.iter().enumerate().skip(current_cursor) {
            if docid < segment.base_docid + (segment.doc_count() as DocId) {
                return Some(i);
            }
        }
        None
    }

    pub fn locate_segment_from_rewind(
        &self,
        _docid: DocId,
        _current_cursor: usize,
    ) -> Option<usize> {
        unimplemented!()
    }
}

impl SegmentInfo {
    pub fn new_static_segment(base_docid: DocId, doc_count: usize) -> Self {
        Self {
            base_docid,
            doc_count: DocCountVariant::Static(doc_count),
        }
    }

    pub fn new_dynamic_segment(base_docid: DocId, doc_count: BuildingDocCount) -> Self {
        Self {
            base_docid,
            doc_count: DocCountVariant::Dynamic(doc_count),
        }
    }

    pub fn doc_count(&self) -> usize {
        self.doc_count.get()
    }
}
