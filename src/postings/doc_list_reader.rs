use std::io;

use crate::{DocId, DOCLIST_BLOCK_LEN};

use super::{compression, DocListBlock, DocListFormat};

pub struct DocListReader<R: io::Read> {
    last_docid: DocId,
    doc_list_format: DocListFormat,
    reader: R,
}

impl<R: io::Read> DocListReader<R> {
    pub fn open(doc_list_format: DocListFormat, reader: R) -> Self {
        Self {
            last_docid: 0,
            doc_list_format,
            reader,
        }
    }

    pub fn decode_one_block(
        &mut self,
        query_docid: DocId,
        doc_list_block: &mut DocListBlock,
    ) -> io::Result<bool> {
        loop {
            let mut len_buf = [0_u8; 1];
            let len_buf_read = self.reader.read(&mut len_buf)?;
            if len_buf_read == 0 {
                return Ok(false);
            }
            let block_len = len_buf[0] as usize;
            if block_len < DOCLIST_BLOCK_LEN {
                doc_list_block.len = DOCLIST_BLOCK_LEN;
                compression::copy_read(&mut self.reader, &mut doc_list_block.docids);
                if self.doc_list_format.has_tflist() {
                    if let Some(termfreqs) = doc_list_block.termfreqs.as_deref_mut() {
                        compression::copy_read(&mut self.reader, termfreqs);
                    }
                }
                if self.doc_list_format.has_fieldmask() {
                    if let Some(fieldmasks) = doc_list_block.fieldmasks.as_deref_mut() {
                        compression::copy_read(&mut self.reader, fieldmasks);
                    } else {
                        assert!(false);
                    }
                }
            } else {
                doc_list_block.len = block_len;
                compression::copy_read(&mut self.reader, &mut doc_list_block.docids[0..block_len]);
                if self.doc_list_format.has_tflist() {
                    if let Some(termfreqs) = doc_list_block.termfreqs.as_deref_mut() {
                        compression::copy_read(&mut self.reader, &mut termfreqs[0..block_len]);
                    }
                }
                if self.doc_list_format.has_fieldmask() {
                    if let Some(fieldmasks) = doc_list_block.fieldmasks.as_deref_mut() {
                        compression::copy_read(&mut self.reader, &mut fieldmasks[0..block_len]);
                    } else {
                        assert!(false);
                    }
                }
            }
            doc_list_block.decode(self.last_docid);
            self.last_docid = doc_list_block.last_docid();
            if doc_list_block.last_docid() >= query_docid {
                return Ok(true);
            }
        }
    }
}
