use crate::{DocFreq, DocId, FieldMask, TermFreq};

use super::{skiplist::BufferedSkipListWriter, BufferedByteSlice, DocListFormat};

pub struct DocListEncoder {
    fieldmask: FieldMask,
    current_tf: TermFreq,
    total_tf: TermFreq,
    df: DocFreq,
    last_docid: DocId,
    doc_list_format: DocListFormat,
    doc_list_buffer: BufferedByteSlice,
    skip_list_writer: Option<BufferedSkipListWriter>,
}

impl DocListEncoder {
    pub fn add_pos(&mut self, field_index: usize) {
        if self.doc_list_format.has_fieldmask() {
            debug_assert!(field_index < 8);
            self.fieldmask |= 1 << field_index;
        }
        self.current_tf += 1;
        self.total_tf += 1;
    }

    pub fn end_doc(&mut self, docid: DocId) {
        self.add_doc(docid);
        self.last_docid = docid;
        self.current_tf = 0;
        self.fieldmask = 0;
        self.df += 1;
    }

    fn add_doc(&mut self, docid: DocId) {
        self.doc_list_buffer.push(0, docid - self.last_docid);
        let mut row = 1;
        if self.doc_list_format.has_tflist() {
            self.doc_list_buffer.push(row, 1);
            row += 1;
        }
        if self.doc_list_format.has_fieldmask() {
            self.doc_list_buffer.push(row, 1);
            row += 1;
        }
        assert_eq!(row, self.doc_list_format.value_items().value_items_count());
        let flush_size = self.doc_list_buffer.end_push();
        if flush_size > 0 {
            self.add_skip_item(flush_size);
        }
    }

    fn add_skip_item(&mut self, offset: usize) {
        if self.skip_list_writer.is_none() {
            self.skip_list_writer = Some(self.create_skip_list_writer());
        }
        let skip_list_writer = self.skip_list_writer.as_mut().unwrap();
        if self.doc_list_format.has_tflist() {
            skip_list_writer.add_with_tf_and_offset(self.last_docid, self.total_tf, offset);
        } else {
            skip_list_writer.add_with_offset(self.last_docid, offset);
        }
    }

    fn create_skip_list_writer(&self) -> BufferedSkipListWriter {
        BufferedSkipListWriter::new(self.doc_list_format.skip_list_format().clone())
    }
}
