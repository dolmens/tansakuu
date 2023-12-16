use crate::DocId;

pub struct BuildingDocListDecoder {}

impl BuildingDocListDecoder {
    pub fn decode_one_block(start_docid: DocId, buffers: &[&mut [u8]]) -> (DocId, DocId, usize) {

        (0, 0, 0)
    }
}
