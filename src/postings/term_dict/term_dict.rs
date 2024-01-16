use std::io::{self, Write};

use tantivy_common::{file_slice::FileSlice, VInt};
use tantivy_sstable::{
    value::{ValueReader, ValueWriter},
    Dictionary, SSTable, Writer,
};

use crate::postings::TermInfo;

pub struct TermDict(Dictionary<TermSSTable>);

pub struct TermSSTable;

pub struct TermDictBuilder<W: Write>(Writer<W, TermInfoValueWriter>);

impl TermDict {
    pub fn open(file: FileSlice) -> io::Result<Self> {
        Dictionary::open(file).map(TermDict)
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> io::Result<Option<TermInfo>> {
        self.0.get(key)
    }
}

impl<W: Write> TermDictBuilder<W> {
    pub fn new(w: W) -> Self {
        Self(Writer::<W, TermInfoValueWriter>::new(w))
    }

    pub fn insert<K: AsRef<[u8]>>(&mut self, key: K, value: &TermInfo) -> io::Result<()> {
        self.0.insert(key, value)
    }

    pub fn finish(self) -> io::Result<W> {
        self.0.finish()
    }
}

impl SSTable for TermSSTable {
    type Value = TermInfo;
    type ValueReader = TermInfoValueReader;
    type ValueWriter = TermInfoValueWriter;
}

#[derive(Default)]
pub struct TermInfoValueReader {
    term_infos: Vec<TermInfo>,
}

impl ValueReader for TermInfoValueReader {
    type Value = TermInfo;

    #[inline(always)]
    fn value(&self, idx: usize) -> &TermInfo {
        &self.term_infos[idx]
    }

    fn load(&mut self, mut data: &[u8]) -> io::Result<usize> {
        let len_before = data.len();
        self.term_infos.clear();
        let num_els = VInt::deserialize_u64(&mut data)?;
        for _ in 0..num_els {
            let skip_item_count = VInt::deserialize_u64(&mut data)? as usize;
            let skip_start = VInt::deserialize_u64(&mut data)? as usize;
            let skip_end = VInt::deserialize_u64(&mut data)? as usize;
            let posting_item_count = VInt::deserialize_u64(&mut data)? as usize;
            let posting_start = VInt::deserialize_u64(&mut data)? as usize;
            let posting_end = VInt::deserialize_u64(&mut data)? as usize;
            let term_info = TermInfo {
                skip_item_count,
                skip_start,
                skip_end,
                posting_item_count,
                posting_start,
                posting_end,
            };
            self.term_infos.push(term_info);
        }
        let consumed_len = len_before - data.len();
        Ok(consumed_len)
    }
}

#[derive(Default)]
pub struct TermInfoValueWriter {
    term_infos: Vec<TermInfo>,
}

impl ValueWriter for TermInfoValueWriter {
    type Value = TermInfo;

    fn write(&mut self, term_info: &TermInfo) {
        self.term_infos.push(term_info.clone());
    }

    fn serialize_block(&self, buffer: &mut Vec<u8>) {
        VInt(self.term_infos.len() as u64).serialize_into_vec(buffer);
        if self.term_infos.is_empty() {
            return;
        }
        for term_info in &self.term_infos {
            VInt(term_info.skip_item_count as u64).serialize_into_vec(buffer);
            VInt(term_info.skip_start as u64).serialize_into_vec(buffer);
            VInt(term_info.skip_end as u64).serialize_into_vec(buffer);
            VInt(term_info.posting_item_count as u64).serialize_into_vec(buffer);
            VInt(term_info.posting_start as u64).serialize_into_vec(buffer);
            VInt(term_info.posting_end as u64).serialize_into_vec(buffer);
        }
    }

    fn clear(&mut self) {
        self.term_infos.clear();
    }
}
