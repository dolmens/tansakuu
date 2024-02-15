use std::io;

use allocator_api2::alloc::{Allocator, Global};

use crate::DocId;

use super::{
    building_doc_list::{BuildingDocList, BuildingDocListDecoder, BuildingDocListEncoder},
    positions::{BuildingPositionList, BuildingPositionListDecoder, BuildingPositionListEncoder},
    posting_writer::PostingWriter,
    DocListBlock, PostingFormat, PostingRead, PostingReader,
};

#[derive(Clone)]
pub struct BuildingPostingList<A: Allocator = Global> {
    pub building_doc_list: BuildingDocList<A>,
    pub building_position_list: Option<BuildingPositionList<A>>,
    pub posting_format: PostingFormat,
}

pub struct BuildingPostingWriter<A: Allocator + Clone = Global> {
    posting_writer: PostingWriter<BuildingDocListEncoder<A>, BuildingPositionListEncoder<A>>,
    building_posting_list: BuildingPostingList<A>,
}

pub struct BuildingPostingReader<'a, A: Allocator = Global> {
    posting_reader: PostingReader<BuildingDocListDecoder<'a>, BuildingPositionListDecoder<'a>>,
    building_posting_list: &'a BuildingPostingList<A>,
}

impl<A: Allocator + Clone + Default> BuildingPostingWriter<A> {
    pub fn new(posting_format: PostingFormat, initial_slice_capacity: usize) -> Self {
        Self::new_in(posting_format, initial_slice_capacity, A::default())
    }
}

impl<A: Allocator + Clone> BuildingPostingWriter<A> {
    pub fn new_in(
        posting_format: PostingFormat,
        initial_slice_capacity: usize,
        allocator: A,
    ) -> Self {
        let doc_list_format = posting_format.doc_list_format().clone();
        let doc_list_encoder = BuildingDocListEncoder::new_in(
            doc_list_format,
            initial_slice_capacity,
            allocator.clone(),
        );
        let building_doc_list = doc_list_encoder.building_doc_list().clone();

        let (position_list_encoder, building_position_list) = if posting_format.has_position_list()
        {
            let position_list_encoder =
                BuildingPositionListEncoder::new_in(initial_slice_capacity, allocator.clone());
            let building_position_list = position_list_encoder.building_position_list().clone();
            (Some(position_list_encoder), Some(building_position_list))
        } else {
            (None, None)
        };

        let posting_writer = PostingWriter::new(
            posting_format.clone(),
            doc_list_encoder,
            position_list_encoder,
        );

        let building_posting_list = BuildingPostingList {
            building_doc_list,
            building_position_list,
            posting_format,
        };

        Self {
            posting_writer,
            building_posting_list,
        }
    }

    pub fn building_posting_list(&self) -> &BuildingPostingList<A> {
        &self.building_posting_list
    }

    pub fn add_pos(&mut self, field: usize, pos: u32) -> io::Result<()> {
        self.posting_writer.add_pos(field, pos)
    }

    pub fn end_doc(&mut self, docid: DocId) -> io::Result<()> {
        self.posting_writer.end_doc(docid)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.posting_writer.flush()
    }
}

impl<'a, A: Allocator> BuildingPostingReader<'a, A> {
    pub fn open(building_posting_list: &'a BuildingPostingList<A>) -> Self {
        let doc_list_decoder =
            BuildingDocListDecoder::open(&building_posting_list.building_doc_list);

        let posting_reader = PostingReader::new(
            building_posting_list.posting_format.clone(),
            doc_list_decoder,
            None,
        );

        Self {
            posting_reader,
            building_posting_list,
        }
    }

    pub fn doc_list_decoder(&self) -> &BuildingDocListDecoder<'_> {
        self.posting_reader.doc_list_decoder()
    }

    pub fn position_list_decoder(&self) -> Option<&BuildingPositionListDecoder<'_>> {
        self.posting_reader.position_list_decoder()
    }

    pub fn eof(&self) -> bool {
        self.doc_list_decoder().eof()
    }

    pub fn df(&self) -> usize {
        self.doc_list_decoder().df()
    }

    pub fn read_count(&self) -> usize {
        self.doc_list_decoder().read_count()
    }
}

impl<'a, A: Allocator> PostingRead for BuildingPostingReader<'a, A> {
    fn decode_doc_buffer(
        &mut self,
        docid: DocId,
        doc_list_block: &mut DocListBlock,
    ) -> io::Result<bool> {
        self.posting_reader.decode_doc_buffer(docid, doc_list_block)
    }

    fn decode_tf_buffer(&mut self, doc_list_block: &mut DocListBlock) -> io::Result<bool> {
        self.posting_reader.decode_tf_buffer(doc_list_block)
    }

    fn decode_fieldmask_buffer(&mut self, doc_list_block: &mut DocListBlock) -> io::Result<bool> {
        self.posting_reader.decode_fieldmask_buffer(doc_list_block)
    }

    fn decode_one_block(
        &mut self,
        docid: DocId,
        doc_list_block: &mut DocListBlock,
    ) -> io::Result<bool> {
        self.posting_reader.decode_one_block(docid, doc_list_block)
    }

    fn decode_position_buffer(
        &mut self,
        from_ttf: u64,
        position_list_block: &mut super::positions::PositionListBlock,
    ) -> io::Result<bool> {
        if self.posting_reader.position_list_decoder().is_none() {
            let position_list_decoder = self
                .building_posting_list
                .building_position_list
                .as_ref()
                .map(|building_position_list| {
                    BuildingPositionListDecoder::open(building_position_list)
                });
            self.posting_reader
                .set_position_list_decoder(position_list_decoder);
        }
        self.posting_reader
            .decode_position_buffer(from_ttf, position_list_block)
    }

    fn decode_next_position_record(
        &mut self,
        position_list_block: &mut super::positions::PositionListBlock,
    ) -> io::Result<bool> {
        self.posting_reader
            .decode_next_position_record(position_list_block)
    }
}

#[cfg(test)]
mod tests {
    use std::{io, thread};

    use crate::{
        postings::{
            building_posting_list::BuildingPostingReader, positions::PositionListBlock,
            BuildingPostingWriter, DocListBlock, PostingFormat, PostingRead,
        },
        DocId, DOC_LIST_BLOCK_LEN, POSITION_LIST_BLOCK_LEN,
    };

    #[test]
    fn test_basic() -> io::Result<()> {
        const BLOCK_LEN: usize = DOC_LIST_BLOCK_LEN;
        let posting_format = PostingFormat::builder().with_tflist().build();
        let doc_list_format = posting_format.doc_list_format().clone();
        let mut posting_writer: BuildingPostingWriter =
            BuildingPostingWriter::new(posting_format.clone(), 1024);
        let posting_list = posting_writer.building_posting_list().clone();
        let mut doc_list_block = DocListBlock::new(&doc_list_format);
        let mut posting_reader = BuildingPostingReader::open(&posting_list);
        assert!(posting_reader.eof());
        assert_eq!(posting_reader.df(), 0);
        assert_eq!(posting_reader.read_count(), 0);
        assert!(!posting_reader.decode_one_block(0, &mut doc_list_block)?);
        assert!(posting_reader.eof());
        assert_eq!(posting_reader.df(), 0);
        assert_eq!(posting_reader.read_count(), 0);

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

        let mut posting_reader = BuildingPostingReader::open(&posting_list);
        assert!(!posting_reader.eof());
        assert_eq!(posting_reader.df(), 1);
        assert_eq!(posting_reader.read_count(), 0);
        assert!(posting_reader.decode_one_block(0, &mut doc_list_block)?);
        assert_eq!(doc_list_block.base_docid, 0);
        assert_eq!(doc_list_block.last_docid, docids[0]);
        assert_eq!(doc_list_block.len, 1);
        assert_eq!(doc_list_block.docids[0], docids[0]);
        assert_eq!(doc_list_block.termfreqs.as_ref().unwrap()[0], termfreqs[0]);

        assert!(posting_reader.eof());
        assert_eq!(posting_reader.df(), 1);
        assert_eq!(posting_reader.read_count(), 1);

        assert!(!posting_reader.decode_one_block(docids[0], &mut doc_list_block)?);

        for _ in 0..termfreqs[1] {
            posting_writer.add_pos(0, 1)?;
        }
        posting_writer.end_doc(docids[1])?;

        let mut posting_reader = BuildingPostingReader::open(&posting_list);
        assert!(posting_reader.decode_one_block(0, &mut doc_list_block)?);
        assert_eq!(doc_list_block.base_docid, 0);
        assert_eq!(doc_list_block.last_docid, docids[1]);
        assert_eq!(doc_list_block.len, 2);
        assert_eq!(doc_list_block.docids[0], docids_deltas[0]);
        assert_eq!(doc_list_block.termfreqs.as_ref().unwrap()[0], termfreqs[0]);
        assert_eq!(doc_list_block.docids[1], docids_deltas[1]);
        assert_eq!(doc_list_block.termfreqs.as_ref().unwrap()[1], termfreqs[1]);

        let block_last_docid = doc_list_block.last_docid;
        assert!(!posting_reader.decode_one_block(block_last_docid + 1, &mut doc_list_block)?);

        for i in 2..BLOCK_LEN {
            for _ in 0..termfreqs[i] {
                posting_writer.add_pos(0, 1)?;
            }
            posting_writer.end_doc(docids[i])?;
        }

        let mut posting_reader = BuildingPostingReader::open(&posting_list);
        assert!(posting_reader.decode_one_block(0, &mut doc_list_block)?);
        assert_eq!(doc_list_block.base_docid, 0);
        assert_eq!(doc_list_block.last_docid, docids[BLOCK_LEN - 1]);
        assert_eq!(doc_list_block.len, BLOCK_LEN);
        assert_eq!(doc_list_block.docids, &docids_deltas[0..BLOCK_LEN]);
        assert_eq!(
            &doc_list_block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
            &termfreqs[0..BLOCK_LEN]
        );

        let block_last_docid = doc_list_block.last_docid;
        assert!(!posting_reader.decode_one_block(block_last_docid + 1, &mut doc_list_block)?);

        for i in 0..BLOCK_LEN + 3 {
            for _ in 0..termfreqs[i + BLOCK_LEN] {
                posting_writer.add_pos(0, 1)?;
            }
            posting_writer.end_doc(docids[i + BLOCK_LEN])?;
        }

        let mut posting_reader = BuildingPostingReader::open(&posting_list);

        assert!(posting_reader.decode_one_block(0, &mut doc_list_block)?);
        assert_eq!(doc_list_block.base_docid, 0);
        assert_eq!(doc_list_block.last_docid, docids[BLOCK_LEN - 1]);
        assert_eq!(doc_list_block.len, BLOCK_LEN);
        assert_eq!(doc_list_block.docids, &docids_deltas[0..BLOCK_LEN]);
        assert_eq!(
            &doc_list_block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
            &termfreqs[0..BLOCK_LEN]
        );

        let block_last_docid = doc_list_block.last_docid;
        assert!(posting_reader.decode_one_block(block_last_docid + 1, &mut doc_list_block)?);
        assert_eq!(doc_list_block.base_docid, docids[BLOCK_LEN - 1]);
        assert_eq!(doc_list_block.last_docid, docids[BLOCK_LEN * 2 - 1]);
        assert_eq!(doc_list_block.len, BLOCK_LEN);
        assert_eq!(
            doc_list_block.docids,
            &docids_deltas[BLOCK_LEN..BLOCK_LEN * 2]
        );
        assert_eq!(
            &doc_list_block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
            &termfreqs[BLOCK_LEN..BLOCK_LEN * 2]
        );

        let block_last_docid = doc_list_block.last_docid;
        assert!(posting_reader.decode_one_block(block_last_docid + 1, &mut doc_list_block)?);
        assert_eq!(doc_list_block.base_docid, docids[BLOCK_LEN * 2 - 1]);
        assert_eq!(doc_list_block.last_docid, docids[BLOCK_LEN * 2 + 3 - 1]);
        assert_eq!(doc_list_block.len, 3);
        assert_eq!(
            &doc_list_block.docids[0..3],
            &docids_deltas[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );
        assert_eq!(
            &doc_list_block.termfreqs.as_ref().unwrap()[0..3],
            &termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );

        let block_last_docid = doc_list_block.last_docid;
        assert!(!posting_reader.decode_one_block(block_last_docid + 1, &mut doc_list_block)?);

        // skip one block

        let mut posting_reader = BuildingPostingReader::open(&posting_list);

        assert!(posting_reader.decode_one_block(docids[BLOCK_LEN - 1] + 1, &mut doc_list_block)?);
        assert_eq!(doc_list_block.base_docid, docids[BLOCK_LEN - 1]);
        assert_eq!(doc_list_block.last_docid, docids[BLOCK_LEN * 2 - 1]);
        assert_eq!(doc_list_block.len, BLOCK_LEN);
        assert_eq!(
            doc_list_block.docids,
            &docids_deltas[BLOCK_LEN..BLOCK_LEN * 2]
        );
        assert_eq!(
            &doc_list_block.termfreqs.as_ref().unwrap()[0..BLOCK_LEN],
            &termfreqs[BLOCK_LEN..BLOCK_LEN * 2]
        );

        let block_last_docid = doc_list_block.last_docid;
        assert!(posting_reader.decode_one_block(block_last_docid + 1, &mut doc_list_block)?);
        assert_eq!(doc_list_block.base_docid, docids[BLOCK_LEN * 2 - 1]);
        assert_eq!(doc_list_block.last_docid, docids[BLOCK_LEN * 2 + 3 - 1]);
        assert_eq!(doc_list_block.len, 3);
        assert_eq!(
            &doc_list_block.docids[0..3],
            &docids_deltas[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );
        assert_eq!(
            &doc_list_block.termfreqs.as_ref().unwrap()[0..3],
            &termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );

        let block_last_docid = doc_list_block.last_docid;
        assert!(!posting_reader.decode_one_block(block_last_docid + 1, &mut doc_list_block)?);

        // skip two blocks

        let mut posting_reader = BuildingPostingReader::open(&posting_list);

        assert!(
            posting_reader.decode_one_block(docids[BLOCK_LEN * 2 - 1] + 1, &mut doc_list_block)?
        );
        assert_eq!(doc_list_block.base_docid, docids[BLOCK_LEN * 2 - 1]);
        assert_eq!(doc_list_block.last_docid, docids[BLOCK_LEN * 2 + 3 - 1]);
        assert_eq!(doc_list_block.len, 3);
        assert_eq!(
            &doc_list_block.docids[0..3],
            &docids_deltas[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );
        assert_eq!(
            &doc_list_block.termfreqs.as_ref().unwrap()[0..3],
            &termfreqs[BLOCK_LEN * 2..BLOCK_LEN * 2 + 3]
        );

        let block_last_docid = doc_list_block.last_docid;
        assert!(!posting_reader.decode_one_block(block_last_docid + 1, &mut doc_list_block)?);

        // skip to end

        let mut posting_reader = BuildingPostingReader::open(&posting_list);

        assert!(!posting_reader
            .decode_one_block(docids.last().cloned().unwrap() + 1, &mut doc_list_block)?);

        Ok(())
    }

    #[test]
    fn test_multithread() -> io::Result<()> {
        const BLOCK_LEN: usize = DOC_LIST_BLOCK_LEN;
        let posting_format = PostingFormat::builder().with_tflist().build();
        let doc_list_format = posting_format.doc_list_format().clone();
        let mut posting_writer: BuildingPostingWriter =
            BuildingPostingWriter::new(posting_format.clone(), 1024);
        let posting_list = posting_writer.building_posting_list().clone();

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

        thread::scope(|scope| {
            let w = scope.spawn(move || {
                for i in 0..BLOCK_LEN * 2 + 3 {
                    for _ in 0..termfreqs[i] {
                        posting_writer.add_pos(0, 1).unwrap();
                    }
                    posting_writer.end_doc(docids[i]).unwrap();
                    thread::yield_now();
                }
            });

            let r = scope.spawn(move || loop {
                let mut doc_list_block = DocListBlock::new(&doc_list_format);
                let mut posting_reader = BuildingPostingReader::open(&posting_list);
                let mut query_docid = 0;
                let mut offset = 0;
                loop {
                    if posting_reader
                        .decode_one_block(query_docid, &mut doc_list_block)
                        .unwrap()
                    {
                        let block_len = doc_list_block.len;
                        let prev_docid = if offset > 0 { docids[offset - 1] } else { 0 };
                        assert_eq!(doc_list_block.base_docid, prev_docid);
                        assert_eq!(doc_list_block.last_docid, docids[offset + block_len - 1]);
                        assert_eq!(
                            &doc_list_block.docids[0..block_len],
                            &docids_deltas[offset..offset + block_len]
                        );

                        assert_eq!(
                            &doc_list_block.termfreqs.as_ref().unwrap()[0..block_len],
                            &termfreqs[offset..offset + block_len]
                        );
                        query_docid = doc_list_block.last_docid + 1;
                        offset += block_len;
                    } else {
                        break;
                    }
                }
                if posting_reader.df() == BLOCK_LEN * 2 + 3 {
                    break;
                }
                thread::yield_now();
            });

            w.join().unwrap();
            r.join().unwrap();
        });

        Ok(())
    }

    #[test]
    fn test_with_position_list() -> io::Result<()> {
        const BLOCK_LEN: usize = DOC_LIST_BLOCK_LEN;
        let posting_format = PostingFormat::builder()
            .with_tflist()
            .with_position_list()
            .build();
        let doc_list_format = posting_format.doc_list_format().clone();
        let mut posting_writer: BuildingPostingWriter =
            BuildingPostingWriter::new(posting_format.clone(), 1024);
        let posting_list = posting_writer.building_posting_list().clone();
        let mut doc_list_block = DocListBlock::new(&doc_list_format);
        let mut position_list_block = PositionListBlock::new();

        let mut posting_reader = BuildingPostingReader::open(&posting_list);
        assert!(posting_reader.eof());
        assert_eq!(posting_reader.df(), 0);
        assert_eq!(posting_reader.read_count(), 0);
        assert!(!posting_reader.decode_one_block(0, &mut doc_list_block)?);
        assert!(posting_reader.eof());
        assert_eq!(posting_reader.df(), 0);
        assert_eq!(posting_reader.read_count(), 0);

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

        let mut positions = vec![];
        let mut positions_deltas = vec![];
        for i in 0..BLOCK_LEN * 2 + 3 {
            let mut deltas = vec![];
            let mut ps = vec![];
            let mut p = 0;
            for j in 0..(i % 4) + 1 {
                let d = (i + j) as u32;
                deltas.push(d);
                p += d;
                ps.push(p);
            }
            positions_deltas.push(deltas);
            positions.push(ps);
        }

        let pos_delta_flatten: Vec<_> = positions_deltas.iter().flatten().cloned().collect();

        for &p in &positions[0] {
            posting_writer.add_pos(0, p)?;
        }
        posting_writer.end_doc(docids[0])?;

        let mut posting_reader = BuildingPostingReader::open(&posting_list);
        assert!(!posting_reader.eof());
        assert_eq!(posting_reader.df(), 1);
        assert_eq!(posting_reader.read_count(), 0);
        assert!(posting_reader.decode_one_block(0, &mut doc_list_block)?);
        assert_eq!(doc_list_block.base_docid, 0);
        assert_eq!(doc_list_block.last_docid, docids[0]);
        assert_eq!(doc_list_block.len, 1);
        assert_eq!(doc_list_block.docids[0], docids[0]);
        assert_eq!(
            doc_list_block.termfreqs.as_ref().unwrap()[0],
            positions[0].len() as u32
        );

        assert!(posting_reader.decode_position_buffer(0, &mut position_list_block)?);
        assert_eq!(
            &position_list_block.positions[0..position_list_block.len],
            &positions[0]
        );

        for &p in &positions[1] {
            posting_writer.add_pos(0, p)?;
        }
        posting_writer.end_doc(docids[1])?;

        let mut posting_reader = BuildingPostingReader::open(&posting_list);
        assert!(!posting_reader.eof());
        assert_eq!(posting_reader.df(), 2);
        assert_eq!(posting_reader.read_count(), 0);
        assert!(posting_reader.decode_one_block(0, &mut doc_list_block)?);
        assert_eq!(doc_list_block.base_docid, 0);
        assert_eq!(doc_list_block.last_docid, docids[1]);
        assert_eq!(doc_list_block.len, 2);
        assert_eq!(doc_list_block.docids[0], docids[0]);
        assert_eq!(
            doc_list_block.termfreqs.as_ref().unwrap()[0],
            positions[0].len() as u32
        );
        assert_eq!(doc_list_block.docids[1], docids[1]);
        assert_eq!(
            doc_list_block.termfreqs.as_ref().unwrap()[1],
            positions[1].len() as u32
        );

        assert!(posting_reader.decode_position_buffer(0, &mut position_list_block)?);
        let ttf = positions[0].len() + positions[1].len();
        assert_eq!(
            &position_list_block.positions[0..position_list_block.len],
            &pos_delta_flatten[0..ttf]
        );

        for i in 2..BLOCK_LEN {
            for &p in &positions[i] {
                posting_writer.add_pos(0, p)?;
            }
            posting_writer.end_doc(docids[i])?;
        }

        let mut posting_reader = BuildingPostingReader::open(&posting_list);
        assert!(!posting_reader.eof());
        assert_eq!(posting_reader.df(), BLOCK_LEN);
        assert_eq!(posting_reader.read_count(), 0);
        assert!(posting_reader.decode_one_block(0, &mut doc_list_block)?);
        assert_eq!(doc_list_block.base_docid, 0);
        assert_eq!(doc_list_block.last_docid, docids[BLOCK_LEN - 1]);
        assert_eq!(doc_list_block.len, BLOCK_LEN);

        let ttf: usize = positions[0..BLOCK_LEN].iter().map(|ps| ps.len()).sum();
        let mut current_ttf = 0;
        while current_ttf < ttf {
            let block_len = std::cmp::min(ttf - current_ttf, POSITION_LIST_BLOCK_LEN);
            assert!(posting_reader
                .decode_position_buffer(current_ttf as u64, &mut position_list_block)?);
            assert_eq!(position_list_block.len, block_len);
            assert_eq!(position_list_block.start_ttf, current_ttf as u64);
            assert_eq!(
                &position_list_block.positions[0..position_list_block.len],
                &pos_delta_flatten[current_ttf..current_ttf + block_len]
            );
            current_ttf += block_len;
        }

        Ok(())
    }
}
