use std::io::{self, Write};

use tantivy_common::{file_slice::FileSlice, VInt};
use tantivy_sstable::{
    value::{ValueReader, ValueWriter},
    Dictionary, SSTable, Writer,
};

use crate::DocId;

pub struct PrimaryKeyDict(Dictionary<DocIdSSTable>);

pub struct PrimaryKeyDictBuilder<W: Write>(Writer<W, DocIdValueWriter>);

impl<W: Write> PrimaryKeyDictBuilder<W> {
    pub fn new(w: W) -> Self {
        Self(Writer::<W, DocIdValueWriter>::new(w))
    }

    pub fn insert<K: AsRef<[u8]>>(&mut self, key: K, value: &DocId) -> io::Result<()> {
        self.0.insert(key, value)
    }

    pub fn finish(self) -> io::Result<W> {
        self.0.finish()
    }
}

impl PrimaryKeyDict {
    pub fn open(file: FileSlice) -> io::Result<Self> {
        Dictionary::open(file).map(PrimaryKeyDict)
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> io::Result<Option<DocId>> {
        self.0.get(key)
    }

    pub fn iter(&self) -> PrimaryKeyDictIterator {
        PrimaryKeyDictIterator { idx: 0, dict: self }
    }
}

pub struct PrimaryKeyDictIterator<'a> {
    idx: usize,
    dict: &'a PrimaryKeyDict,
}

impl<'a> Iterator for PrimaryKeyDictIterator<'a> {
    type Item = (Vec<u8>, DocId);

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx < self.dict.0.num_terms() {
            let mut term = Vec::new();
            assert!(self.dict.0.ord_to_term(self.idx as u64, &mut term).unwrap());
            let docid = self
                .dict
                .0
                .term_info_from_ord(self.idx as u64)
                .unwrap()
                .unwrap();
            self.idx += 1;
            Some((term, docid))
        } else {
            None
        }
    }
}

pub struct DocIdSSTable;

impl SSTable for DocIdSSTable {
    type Value = DocId;
    type ValueReader = DocIdValueReader;
    type ValueWriter = DocIdValueWriter;
}

#[derive(Default)]
pub struct DocIdValueReader {
    docids: Vec<DocId>,
}

#[derive(Default)]
pub struct DocIdValueWriter {
    docids: Vec<DocId>,
}

impl ValueReader for DocIdValueReader {
    type Value = DocId;

    fn value(&self, idx: usize) -> &Self::Value {
        &self.docids[idx]
    }

    fn load(&mut self, mut data: &[u8]) -> std::io::Result<usize> {
        let len_before = data.len();
        self.docids.clear();
        let num_els = VInt::deserialize_u64(&mut data)?;
        for _ in 0..num_els {
            let docid = VInt::deserialize_u64(&mut data)? as DocId;
            self.docids.push(docid);
        }
        let consumed_len = len_before - data.len();
        Ok(consumed_len)
    }
}

impl ValueWriter for DocIdValueWriter {
    type Value = DocId;

    fn write(&mut self, val: &Self::Value) {
        self.docids.push(*val);
    }

    fn serialize_block(&self, output: &mut Vec<u8>) {
        VInt(self.docids.len() as u64).serialize_into_vec(output);
        if self.docids.is_empty() {
            return;
        }
        for &docid in &self.docids {
            VInt(docid as u64).serialize_into_vec(output);
        }
    }

    fn clear(&mut self) {
        self.docids.clear();
    }
}
