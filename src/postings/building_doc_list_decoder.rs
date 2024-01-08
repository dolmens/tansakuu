use crate::{DocId, TermFreq};

struct DocBlock {
    docids: Vec<DocId>,
    termfreqs: Vec<TermFreq>,
}

pub struct BuildingDocListDecoder {}

impl BuildingDocListDecoder {
    pub fn decode(start_docid: DocId, doc_block: &mut DocBlock) {}
    pub fn decode_one_block(start_docid: DocId, buffers: &[&mut [u8]]) -> (DocId, DocId, usize) {

        (0, 0, 0)
    }
}
