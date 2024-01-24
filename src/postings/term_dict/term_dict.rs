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

    pub fn iter(&self) -> TermDictIterator {
        TermDictIterator { idx: 0, dict: self }
    }
}

pub struct TermDictIterator<'a> {
    idx: usize,
    dict: &'a TermDict,
}

impl<'a> Iterator for TermDictIterator<'a> {
    type Item = (Vec<u8>, TermInfo);

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx < self.dict.0.num_terms() {
            let mut term = Vec::new();
            assert!(self.dict.0.ord_to_term(self.idx as u64, &mut term).unwrap());
            let term_info = self
                .dict
                .0
                .term_info_from_ord(self.idx as u64)
                .unwrap()
                .unwrap();
            self.idx += 1;
            Some((term, term_info))
        } else {
            None
        }
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
            let position_skip_item_count = VInt::deserialize_u64(&mut data)? as usize;
            let position_skip_start = VInt::deserialize_u64(&mut data)? as usize;
            let position_skip_end = VInt::deserialize_u64(&mut data)? as usize;
            let position_item_count = VInt::deserialize_u64(&mut data)? as usize;
            let position_start = VInt::deserialize_u64(&mut data)? as usize;
            let position_end = VInt::deserialize_u64(&mut data)? as usize;
            let term_info = TermInfo {
                skip_list_item_count: skip_item_count,
                skip_list_start: skip_start,
                skip_list_end: skip_end,
                posting_item_count,
                posting_start,
                posting_end,
                position_skip_list_item_count: position_skip_item_count,
                position_skip_list_start: position_skip_start,
                position_skip_list_end: position_skip_end,
                position_list_item_count: position_item_count,
                position_list_start: position_start,
                position_list_end: position_end
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
            VInt(term_info.skip_list_item_count as u64).serialize_into_vec(buffer);
            VInt(term_info.skip_list_start as u64).serialize_into_vec(buffer);
            VInt(term_info.skip_list_end as u64).serialize_into_vec(buffer);
            VInt(term_info.posting_item_count as u64).serialize_into_vec(buffer);
            VInt(term_info.posting_start as u64).serialize_into_vec(buffer);
            VInt(term_info.posting_end as u64).serialize_into_vec(buffer);
            VInt(term_info.position_skip_list_item_count as u64).serialize_into_vec(buffer);
            VInt(term_info.position_skip_list_start as u64).serialize_into_vec(buffer);
            VInt(term_info.position_skip_list_end as u64).serialize_into_vec(buffer);
            VInt(term_info.position_list_item_count as u64).serialize_into_vec(buffer);
            VInt(term_info.position_list_start as u64).serialize_into_vec(buffer);
            VInt(term_info.position_list_end as u64).serialize_into_vec(buffer);
        }
    }

    fn clear(&mut self) {
        self.term_infos.clear();
    }
}
