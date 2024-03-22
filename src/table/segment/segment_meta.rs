use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::{Directory, DocId};

use super::SegmentId;

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct SegmentMetaData {
    doc_count: usize,
}

#[derive(Clone)]
pub struct SegmentMeta {
    segment_id: SegmentId,
    base_docid: DocId,
    doc_count: usize,
}

#[derive(Clone)]
pub struct SegmentMetaRegistry {
    pub segments: Vec<SegmentMeta>,
}

impl SegmentMetaData {
    pub fn new(doc_count: usize) -> Self {
        Self { doc_count }
    }

    pub fn load(directory: &dyn Directory, path: impl AsRef<Path>) -> Self {
        let json_data = directory.atomic_read(path.as_ref()).unwrap();
        serde_json::from_slice(&json_data).unwrap()
    }

    pub fn save(&self, directory: &dyn Directory, path: impl AsRef<Path>) {
        let json = serde_json::to_string_pretty(self).unwrap();
        directory
            .atomic_write(path.as_ref(), json.as_bytes())
            .unwrap()
    }

    pub fn doc_count(&self) -> usize {
        self.doc_count
    }
}

impl SegmentMeta {
    pub fn new(segment_id: SegmentId, base_docid: DocId, doc_count: usize) -> Self {
        Self {
            segment_id,
            base_docid,
            doc_count,
        }
    }

    pub fn segment_id(&self) -> &SegmentId {
        &self.segment_id
    }

    pub fn base_docid(&self) -> DocId {
        self.base_docid
    }

    pub fn set_base_docid(&mut self, base_docid: DocId) {
        self.base_docid = base_docid;
    }

    pub fn doc_count(&self) -> usize {
        self.doc_count
    }

    pub fn set_doc_count(&mut self, doc_count: usize) {
        self.doc_count = doc_count;
    }

    pub fn end_docid(&self) -> DocId {
        self.base_docid + (self.doc_count as DocId)
    }

    pub fn inner_docid(&self, docid: DocId) -> DocId {
        assert!(docid > self.base_docid);
        docid - self.base_docid
    }
}

impl SegmentMetaRegistry {
    pub fn new(segment_metas: Vec<SegmentMeta>) -> Self {
        Self {
            segments: segment_metas,
        }
    }

    pub fn add_segment(&mut self, segment_meta: SegmentMeta) {
        self.segments.push(segment_meta);
    }

    pub fn iter(&self) -> impl Iterator<Item = &SegmentMeta> + '_ {
        self.segments.iter()
    }

    pub fn segment(&self, segment_cursor: usize) -> &SegmentMeta {
        &self.segments[segment_cursor]
    }

    pub fn locate_segment(&self, docid: DocId) -> Option<usize> {
        for (i, segment) in self.segments.iter().enumerate() {
            if (docid as usize) < (segment.base_docid as usize) + segment.doc_count {
                return Some(i);
            }
        }
        None
    }

    pub fn locate_segment_from(&self, docid: DocId, current_cursor: usize) -> Option<usize> {
        for (i, segment) in self.segments.iter().enumerate().skip(current_cursor) {
            if (docid as usize) < (segment.base_docid as usize) + segment.doc_count {
                return Some(i);
            }
        }
        None
    }

    pub fn locate_segment_rewind(&self, _docid: DocId, _current_cursor: usize) -> Option<usize> {
        unimplemented!()
    }
}
