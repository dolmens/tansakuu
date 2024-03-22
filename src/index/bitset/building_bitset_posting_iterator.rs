use crate::{util::ExpandableBitset, DocId, END_DOCID, INVALID_DOCID};

pub struct BuildingBitsetPostingIterator<'a> {
    current_docid: DocId,
    doc_count: usize,
    current_word: u64,
    values: &'a ExpandableBitset,
}

impl<'a> BuildingBitsetPostingIterator<'a> {
    pub fn new(values: &'a ExpandableBitset) -> Self {
        let doc_count = values.valid_len();

        Self {
            current_docid: INVALID_DOCID,
            doc_count,
            current_word: 0,
            values,
        }
    }

    pub fn seek(&mut self, docid: DocId) -> DocId {
        let docid = if docid < 0 { 0 } else { docid };
        if docid <= self.current_docid {
            return self.current_docid;
        }

        if (docid as usize) >= self.doc_count {
            self.current_docid = END_DOCID;
            return END_DOCID;
        }

        if (docid / 64) > (self.current_docid / 64) {
            self.current_word = self.values.word((docid as usize) / 64);
        }

        let mut docid = docid;
        loop {
            let value = self.current_word & (1 << ((docid as usize) % 64));
            if value != 0 {
                self.current_docid = docid;
                return docid;
            }
            docid += 1;
            if (docid as usize) >= self.doc_count {
                self.current_docid = END_DOCID;
                return END_DOCID;
            }
            if docid % 64 == 0 {
                self.current_word = self.values.word((docid as usize) / 64);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{util::ExpandableBitsetWriter, END_DOCID, INVALID_DOCID};

    use super::BuildingBitsetPostingIterator;

    #[test]
    fn test_basic() {
        let mut bitset_writer = ExpandableBitsetWriter::with_capacity(256);
        let bitset = bitset_writer.bitset();

        bitset_writer.insert(0);
        bitset_writer.insert(63);
        bitset_writer.insert(65);
        bitset_writer.insert(180);
        bitset_writer.insert(191);
        bitset_writer.set_item_len(192);

        let mut posting_iter = BuildingBitsetPostingIterator::new(&bitset);
        assert_eq!(posting_iter.seek(INVALID_DOCID), 0);
        assert_eq!(posting_iter.seek(0), 0);
        assert_eq!(posting_iter.seek(1), 63);
        assert_eq!(posting_iter.seek(64), 65);
        assert_eq!(posting_iter.seek(66), 180);
        assert_eq!(posting_iter.seek(181), 191);
        assert_eq!(posting_iter.seek(192), END_DOCID);

        let mut posting_iter = BuildingBitsetPostingIterator::new(&bitset);
        assert_eq!(posting_iter.seek(0), 0);
        assert_eq!(posting_iter.seek(66), 180);
        assert_eq!(posting_iter.seek(192), END_DOCID);

        let mut posting_iter = BuildingBitsetPostingIterator::new(&bitset);
        assert_eq!(posting_iter.seek(66), 180);
        assert_eq!(posting_iter.seek(192), END_DOCID);

        let mut posting_iter = BuildingBitsetPostingIterator::new(&bitset);
        assert_eq!(posting_iter.seek(191), 191);
        assert_eq!(posting_iter.seek(192), END_DOCID);

        let mut posting_iter = BuildingBitsetPostingIterator::new(&bitset);
        assert_eq!(posting_iter.seek(192), END_DOCID);
    }
}
