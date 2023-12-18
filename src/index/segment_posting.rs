use crate::{DocId, END_DOCID};

pub struct SegmentPosting {
    pub docids: Vec<DocId>,
}

impl SegmentPosting {
    pub fn is_empty(&self) -> bool {
        return self.docids.is_empty();
    }

    pub fn seek(&self, docid: DocId) -> DocId {
        match self.docids.binary_search(&docid) {
            Ok(_) => docid,
            Err(index) => {
                if index < self.docids.len() {
                    self.docids[index]
                } else {
                    END_DOCID
                }
            }
        }
    }
}
