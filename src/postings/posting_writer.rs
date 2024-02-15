use std::io;

use crate::DocId;

use super::{positions::PositionListEncode, DocListEncode, PostingFormat};

pub struct PostingWriter<D: DocListEncode, P: PositionListEncode> {
    doc_list_encoder: D,
    position_list_encoder: Option<P>,
    posting_format: PostingFormat,
}

impl<D: DocListEncode, P: PositionListEncode> PostingWriter<D, P> {
    pub fn new(
        posting_format: PostingFormat,
        doc_list_encoder: D,
        position_list_encoder: Option<P>,
    ) -> Self {
        Self {
            doc_list_encoder,
            position_list_encoder,
            posting_format,
        }
    }

    pub fn doc_list_encoder(&self) -> &D {
        &self.doc_list_encoder
    }

    pub fn position_list_encoder(&self) -> Option<&P> {
        self.position_list_encoder.as_ref()
    }

    pub fn posting_format(&self) -> &PostingFormat {
        &self.posting_format
    }

    pub fn add_pos(&mut self, field: usize, pos: u32) -> io::Result<()> {
        self.doc_list_encoder.add_pos(field);

        if let Some(position_list_encoder) = self.position_list_encoder.as_mut() {
            position_list_encoder.add_pos(pos)?;
        }

        Ok(())
    }

    pub fn set_fieldmask(&mut self, fieldmask: u8) {
        self.doc_list_encoder.set_field_mask(fieldmask);
    }

    pub fn end_doc(&mut self, docid: DocId) -> io::Result<()> {
        self.doc_list_encoder.end_doc(docid)?;
        if let Some(position_list_encoder) = self.position_list_encoder.as_mut() {
            position_list_encoder.end_doc();
        }

        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.doc_list_encoder.flush()?;
        if let Some(position_list_encoder) = self.position_list_encoder.as_mut() {
            position_list_encoder.flush()?;
        }

        Ok(())
    }

    pub fn df(&self) -> usize {
        self.doc_list_encoder.df()
    }

    pub fn ttf(&self) -> Option<usize> {
        self.position_list_encoder.as_ref().map(|p| p.ttf())
    }

    pub fn doc_list_written_bytes(&self) -> (usize, usize) {
        self.doc_list_encoder.written_bytes()
    }

    pub fn position_list_written_bytes(&self) -> Option<(usize, usize)> {
        self.position_list_encoder
            .as_ref()
            .map(|p| p.written_bytes())
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, BufReader};

    use doc_list_encoder::doc_list_encoder_builder;

    use crate::{
        postings::{
            compression::BlockEncoder, doc_list_encoder, positions::none_position_list_encoder,
            PostingFormat,
        },
        DocId, DOC_LIST_BLOCK_LEN,
    };

    use super::PostingWriter;

    #[test]
    fn test_basic() -> io::Result<()> {
        const BLOCK_LEN: usize = DOC_LIST_BLOCK_LEN;
        let posting_format = PostingFormat::builder().with_tflist().build();
        let doc_list_format = posting_format.doc_list_format().clone();
        let mut buf = vec![];
        let mut skip_list_buf = vec![];
        let doc_list_encoder = doc_list_encoder_builder(doc_list_format)
            .with_writer(&mut buf)
            .with_skip_list_output_writer(&mut skip_list_buf)
            .build();
        let mut posting_writer = PostingWriter::new(
            posting_format,
            doc_list_encoder,
            none_position_list_encoder(),
        );
        let building_block = posting_writer.doc_list_encoder().building_block().clone();

        let docids_deltas: Vec<_> = (0..(BLOCK_LEN * 2 + 3) as DocId).collect();
        let docids_deltas = &docids_deltas[..];
        let docids: Vec<_> = docids_deltas
            .iter()
            .scan(0, |acc, &x| {
                *acc += x;
                Some(*acc)
            })
            .collect();
        let docids = &docids[..];

        let termfreqs: Vec<_> = (0..BLOCK_LEN * 2 + 3)
            .enumerate()
            .map(|(i, _)| (i % 3 + 1) as u32)
            .collect();
        let termfreqs = &termfreqs[..];

        for _ in 0..termfreqs[0] {
            posting_writer.add_pos(0, 1)?;
        }
        posting_writer.end_doc(docids[0])?;

        let flush_info_snapshot = building_block.flush_info.load();
        assert_eq!(flush_info_snapshot.flushed_count(), 0);
        assert_eq!(flush_info_snapshot.buffer_len(), 1);
        assert_eq!(building_block.docids[0].load(), docids[0]);
        assert_eq!(
            building_block.termfreqs.as_ref().unwrap()[0].load(),
            termfreqs[0]
        );

        for _ in 0..termfreqs[1] {
            posting_writer.add_pos(0, 1)?;
        }
        posting_writer.end_doc(docids[1])?;

        let flush_info_snapshot = building_block.flush_info.load();
        assert_eq!(flush_info_snapshot.flushed_count(), 0);
        assert_eq!(flush_info_snapshot.buffer_len(), 2);
        assert_eq!(building_block.docids[0].load(), docids[0]);
        assert_eq!(
            building_block.termfreqs.as_ref().unwrap()[0].load(),
            termfreqs[0]
        );
        assert_eq!(building_block.docids[1].load(), docids[1]);
        assert_eq!(
            building_block.termfreqs.as_ref().unwrap()[1].load(),
            termfreqs[1]
        );

        for i in 2..BLOCK_LEN {
            for _ in 0..termfreqs[i] {
                posting_writer.add_pos(0, 1)?;
            }
            posting_writer.end_doc(docids[i])?;
        }

        let flush_info_snapshot = building_block.flush_info.load();
        assert_eq!(flush_info_snapshot.flushed_count(), BLOCK_LEN);
        assert_eq!(flush_info_snapshot.buffer_len(), 0);

        for i in 0..BLOCK_LEN + 3 {
            for _ in 0..termfreqs[i + BLOCK_LEN] {
                posting_writer.add_pos(0, 1)?;
            }
            posting_writer.end_doc(docids[i + BLOCK_LEN])?;
        }

        let flush_info_snapshot = building_block.flush_info.load();
        assert_eq!(flush_info_snapshot.flushed_count(), BLOCK_LEN * 2);
        assert_eq!(flush_info_snapshot.buffer_len(), 3);

        posting_writer.flush()?;

        let flush_info_snapshot = building_block.flush_info.load();
        assert_eq!(flush_info_snapshot.flushed_count(), BLOCK_LEN * 2 + 3);
        assert_eq!(flush_info_snapshot.buffer_len(), 0);

        let block_encoder = BlockEncoder;

        let mut decoded_docids = [0; BLOCK_LEN];
        let mut decoded_termfreqs = [0; BLOCK_LEN];

        let mut reader = BufReader::new(buf.as_slice());
        block_encoder.decode_u32(&mut reader, &mut decoded_docids)?;
        assert_eq!(&docids_deltas[0..BLOCK_LEN], decoded_docids);
        block_encoder.decode_u32(&mut reader, &mut decoded_termfreqs)?;
        assert_eq!(&termfreqs[0..BLOCK_LEN], decoded_termfreqs);

        block_encoder.decode_u32(&mut reader, &mut decoded_docids)?;
        assert_eq!(&docids_deltas[BLOCK_LEN..BLOCK_LEN * 2], decoded_docids);
        block_encoder.decode_u32(&mut reader, &mut decoded_termfreqs)?;
        assert_eq!(&termfreqs[BLOCK_LEN..BLOCK_LEN * 2], decoded_termfreqs);

        block_encoder.decode_u32(&mut reader, &mut decoded_docids[0..3])?;
        assert_eq!(
            &docids_deltas[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3],
            &decoded_docids[0..3]
        );
        block_encoder.decode_u32(&mut reader, &mut decoded_termfreqs[0..3])?;
        assert_eq!(
            &termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3],
            &decoded_termfreqs[0..3]
        );

        Ok(())
    }
}
