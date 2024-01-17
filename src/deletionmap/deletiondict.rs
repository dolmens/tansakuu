use std::io::{self, Write};

use tantivy_common::file_slice::FileSlice;
use tantivy_sstable::{value::VoidValueWriter, Dictionary, Writer};

pub struct DeletionDict(Dictionary);

pub struct DeletionDictBuilder<W: Write>(Writer<W, VoidValueWriter>);

impl DeletionDict {
    pub fn open(file: FileSlice) -> io::Result<Self> {
        Dictionary::open(file).map(DeletionDict)
    }

    pub fn is_empty(&self) -> bool {
        self.0.num_terms() == 0
    }

    pub fn contains<K: AsRef<[u8]>>(&self, key: K) -> io::Result<bool> {
        self.0.get(key).map(|v| v.is_some())
    }

    pub fn iter(&self) -> DeletionDictIterator {
        DeletionDictIterator { idx: 0, dict: self }
    }
}

impl<W: Write> DeletionDictBuilder<W> {
    pub fn new(w: W) -> Self {
        Self(Writer::<W, VoidValueWriter>::new(w))
    }

    pub fn insert<K: AsRef<[u8]>>(&mut self, key: K) -> io::Result<()> {
        self.0.insert(key, &())
    }

    pub fn finish(self) -> io::Result<W> {
        self.0.finish()
    }
}

pub struct DeletionDictIterator<'a> {
    idx: usize,
    dict: &'a DeletionDict,
}

impl<'a> Iterator for DeletionDictIterator<'a> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx < self.dict.0.num_terms() {
            let mut term = Vec::new();
            assert!(self.dict.0.ord_to_term(self.idx as u64, &mut term).unwrap());
            self.idx += 1;
            Some(term)
        } else {
            None
        }
    }
}
