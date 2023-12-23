use crate::DocId;

pub struct SegmentPosting {
    base_docid: DocId,
    docids: Vec<DocId>,
}

impl SegmentPosting {
    pub fn new(base_docid: DocId, docids: Vec<DocId>) -> Self {
        Self { base_docid, docids }
    }

    pub fn is_empty(&self) -> bool {
        return self.docids.is_empty();
    }

    pub fn set_base_docid(&mut self, base_docid: DocId) {
        self.base_docid = base_docid;
    }

    pub fn seek(&self, docid: DocId) -> Option<DocId> {
        let docid = std::cmp::max(self.base_docid, docid);
        let docid = docid - self.base_docid;
        self.docids
            .iter()
            .find(|&value| *value >= docid)
            .cloned()
            .map(|value| value + self.base_docid)
    }
}
