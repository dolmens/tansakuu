use crate::{util::ExpandableBitset, DocId, END_DOCID, INVALID_DOCID};

pub struct ExpandableBitsetPostingIterator<'a, const POSITIVE: bool> {
    current_docid: DocId,
    valid_doc_count: usize,
    current_word: u64,
    bitset: &'a ExpandableBitset,
}

impl<'a, const POSITIVE: bool> ExpandableBitsetPostingIterator<'a, POSITIVE> {
    pub fn new(doc_count: usize, bitset: &'a ExpandableBitset) -> Self {
        let capacity = bitset.capacity();
        Self {
            current_docid: INVALID_DOCID,
            valid_doc_count: std::cmp::min(doc_count, capacity),
            current_word: 0,
            bitset,
        }
    }

    pub fn seek(&mut self, docid: DocId) -> DocId {
        let docid = if docid < 0 { 0 } else { docid };
        if docid <= self.current_docid {
            return self.current_docid;
        }

        if (docid / 64) > (self.current_docid / 64) {
            if (docid as usize) >= self.valid_doc_count {
                self.current_docid = END_DOCID;
                return END_DOCID;
            }
            self.current_word = self.bitset.word((docid as usize) / 64);
        }

        let mut docid = docid;
        loop {
            let value = self.current_word & (1 << ((docid as usize) % 64));
            if (POSITIVE && value != 0) || (!POSITIVE && value == 0) {
                self.current_docid = docid;
                return docid;
            }
            docid += 1;
            if (docid as usize) >= self.valid_doc_count {
                self.current_docid = END_DOCID;
                return END_DOCID;
            }
            if docid % 64 == 0 {
                self.current_word = self.bitset.word((docid as usize) / 64);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{util::ExpandableBitsetWriter, END_DOCID, INVALID_DOCID};

    use super::ExpandableBitsetPostingIterator;

    #[test]
    fn test_basic() {
        let mut bitset_writer = ExpandableBitsetWriter::with_capacity(256);
        let bitset = bitset_writer.bitset();

        bitset_writer.insert(0);
        bitset_writer.insert(63);
        bitset_writer.insert(65);
        bitset_writer.insert(180);
        bitset_writer.insert(191);

        let mut posting_iter = ExpandableBitsetPostingIterator::<true>::new(192, &bitset);
        assert_eq!(posting_iter.seek(INVALID_DOCID), 0);
        assert_eq!(posting_iter.seek(0), 0);
        assert_eq!(posting_iter.seek(1), 63);
        assert_eq!(posting_iter.seek(64), 65);
        assert_eq!(posting_iter.seek(66), 180);
        assert_eq!(posting_iter.seek(181), 191);
        assert_eq!(posting_iter.seek(192), END_DOCID);

        let mut posting_iter = ExpandableBitsetPostingIterator::<false>::new(192, &bitset);
        assert_eq!(posting_iter.seek(INVALID_DOCID), 1);
        assert_eq!(posting_iter.seek(1), 1);
        assert_eq!(posting_iter.seek(2), 2);
        assert_eq!(posting_iter.seek(64), 64);
        assert_eq!(posting_iter.seek(191), END_DOCID);

        let mut posting_iter = ExpandableBitsetPostingIterator::<true>::new(192, &bitset);
        assert_eq!(posting_iter.seek(0), 0);
        assert_eq!(posting_iter.seek(66), 180);
        assert_eq!(posting_iter.seek(192), END_DOCID);

        let mut posting_iter = ExpandableBitsetPostingIterator::<false>::new(192, &bitset);
        assert_eq!(posting_iter.seek(64), 64);
        assert_eq!(posting_iter.seek(191), END_DOCID);

        let mut posting_iter = ExpandableBitsetPostingIterator::<true>::new(192, &bitset);
        assert_eq!(posting_iter.seek(66), 180);
        assert_eq!(posting_iter.seek(192), END_DOCID);

        let mut posting_iter = ExpandableBitsetPostingIterator::<true>::new(192, &bitset);
        assert_eq!(posting_iter.seek(191), 191);
        assert_eq!(posting_iter.seek(192), END_DOCID);

        let mut posting_iter = ExpandableBitsetPostingIterator::<true>::new(192, &bitset);
        assert_eq!(posting_iter.seek(192), END_DOCID);
    }
}
