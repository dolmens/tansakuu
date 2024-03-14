use std::{
    io::BufWriter,
    io::{self, Write},
    path::Path,
};

use tantivy_common::TerminatingWrite;

use crate::{
    postings::{
        doc_list_encoder_builder,
        positions::{position_list_encoder_builder, PositionListEncode, PositionListEncoder},
        skip_list::SkipListWriter,
        DocListEncode, DocListEncoder, PostingFormat, PostingWriter, TermDictBuilder, TermInfo,
    },
    Directory, DocId,
};

type SerializeWriter = BufWriter<Box<dyn TerminatingWrite>>;
type SerializeWriterRef<'a> = &'a mut SerializeWriter;
type SerializeSkipListWriter<'a> = SkipListWriter<SerializeWriterRef<'a>>;
type SerializeDocListEncoder<'a> =
    DocListEncoder<SerializeWriterRef<'a>, SerializeSkipListWriter<'a>>;
type SerializePositionListEncoder<'a> =
    PositionListEncoder<SerializeWriterRef<'a>, SerializeSkipListWriter<'a>>;

pub struct SerializePostingWriter<'a> {
    posting_writer: PostingWriter<SerializeDocListEncoder<'a>, SerializePositionListEncoder<'a>>,
    token: u64,
}

pub struct SerializePostingWriterStatus {
    token: u64,
    doc_list_written_bytes: usize,
    skip_list_written_bytes: usize,
    position_list_written_bytes: usize,
    position_skip_list_written_bytes: usize,
    df: usize,
    ttf: usize,
}

impl<'a> SerializePostingWriter<'a> {
    pub fn add_pos(&mut self, field: usize, pos: u32) -> io::Result<()> {
        self.posting_writer.add_pos(field, pos)
    }

    pub fn set_fieldmask(&mut self, fieldmask: u8) {
        self.posting_writer.set_fieldmask(fieldmask);
    }

    pub fn end_doc(&mut self, docid: DocId) -> io::Result<()> {
        self.posting_writer.end_doc(docid)
    }

    pub fn finish(mut self) -> io::Result<SerializePostingWriterStatus> {
        self.posting_writer.flush()?;

        let token = self.token;

        let (doc_list_written_bytes, skip_list_written_bytes) =
            self.posting_writer.doc_list_encoder().written_bytes();

        let (position_list_written_bytes, position_skip_list_written_bytes) = self
            .posting_writer
            .position_list_encoder()
            .map_or((0, 0), |encoder| encoder.written_bytes());

        let df = self.posting_writer.doc_list_encoder().df();
        let ttf = self
            .posting_writer
            .position_list_encoder()
            .map_or(0, |encoder| encoder.ttf());

        Ok(SerializePostingWriterStatus {
            token,
            doc_list_written_bytes,
            skip_list_written_bytes,
            position_list_written_bytes,
            position_skip_list_written_bytes,
            df,
            ttf,
        })
    }
}

pub struct InvertedIndexPostingSerializeWriter {
    current_token: Option<u64>,
    previous_token: Option<u64>,

    posting_format: PostingFormat,

    skip_list_start: usize,
    doc_list_start: usize,
    position_list_start: usize,
    position_skip_list_start: usize,

    term_dict_writer: TermDictBuilder<BufWriter<Box<dyn TerminatingWrite>>>,
    skip_list_output_writer: BufWriter<Box<dyn TerminatingWrite>>,
    posting_output_writer: BufWriter<Box<dyn TerminatingWrite>>,
    position_skip_list_output_writer: Option<BufWriter<Box<dyn TerminatingWrite>>>,
    position_list_output_writer: Option<BufWriter<Box<dyn TerminatingWrite>>>,
}

impl InvertedIndexPostingSerializeWriter {
    pub fn new(
        directory: &dyn Directory,
        path: &Path,
        name: &str,
        posting_format: PostingFormat,
    ) -> Self {
        let dict_path = path.join(name.to_string() + ".dict");
        let dict_output_writer = directory.open_write(&dict_path).unwrap();
        let term_dict_writer = TermDictBuilder::new(dict_output_writer);

        let skip_list_path = path.join(name.to_string() + ".skiplist");
        let skip_list_output_writer = directory.open_write(&skip_list_path).unwrap();
        let posting_path = path.join(name.to_string() + ".posting");
        let posting_output_writer = directory.open_write(&posting_path).unwrap();

        let position_skip_list_output_writer = if posting_format.has_position_list() {
            let position_skip_list_path = path.join(name.to_string() + ".positions.skiplist");
            Some(directory.open_write(&position_skip_list_path).unwrap())
        } else {
            None
        };

        let position_list_output_writer = if posting_format.has_position_list() {
            let position_list_path = path.join(name.to_string() + ".positions");
            Some(directory.open_write(&position_list_path).unwrap())
        } else {
            None
        };

        Self {
            current_token: None,
            previous_token: None,

            posting_format,

            skip_list_start: 0,
            doc_list_start: 0,
            position_list_start: 0,
            position_skip_list_start: 0,

            term_dict_writer,
            skip_list_output_writer,
            posting_output_writer,
            position_skip_list_output_writer,
            position_list_output_writer,
        }
    }

    pub fn start_token(&mut self, token: u64) -> SerializePostingWriter<'_> {
        assert!(self.current_token.is_none());
        if let Some(previous_token) = self.previous_token {
            assert!(previous_token.to_be_bytes() < token.to_be_bytes());
        }
        self.current_token = Some(token);

        let doc_list_encoder =
            doc_list_encoder_builder(self.posting_format.doc_list_format().clone())
                .with_writer(self.posting_output_writer.by_ref())
                .with_skip_list_output_writer(self.skip_list_output_writer.by_ref())
                .build();

        let position_list_encoder = if self.posting_format.has_position_list() {
            Some(
                position_list_encoder_builder()
                    .with_writer(
                        self.position_list_output_writer
                            .as_mut()
                            .map(|w| w.by_ref())
                            .unwrap(),
                    )
                    .with_skip_list_output_writer(
                        self.position_skip_list_output_writer
                            .as_mut()
                            .map(|w| w.by_ref())
                            .unwrap(),
                    )
                    .build(),
            )
        } else {
            None
        };

        SerializePostingWriter {
            posting_writer: PostingWriter::new(
                self.posting_format.clone(),
                doc_list_encoder,
                position_list_encoder,
            ),
            token,
        }
    }

    pub fn end_token(&mut self, status: SerializePostingWriterStatus) {
        let SerializePostingWriterStatus {
            token,
            doc_list_written_bytes,
            skip_list_written_bytes,
            position_list_written_bytes,
            position_skip_list_written_bytes,
            df,
            ttf,
        } = status;

        assert_eq!(self.current_token.unwrap(), token);

        let skip_list_end = self.skip_list_start + skip_list_written_bytes;
        let doc_list_end = self.doc_list_start + doc_list_written_bytes;
        let position_list_end = self.position_list_start + position_list_written_bytes;
        let position_skip_list_end =
            self.position_skip_list_start + position_skip_list_written_bytes;

        let term_info = TermInfo {
            df,
            doc_list_start: self.doc_list_start,
            doc_list_end,
            skip_list_start: self.skip_list_start,
            skip_list_end,

            ttf,
            position_list_start: self.position_list_start,
            position_list_end,
            position_skip_list_start: self.position_skip_list_start,
            position_skip_list_end,
        };

        self.skip_list_start = skip_list_end;
        self.doc_list_start = doc_list_end;
        self.position_skip_list_start = position_skip_list_end;
        self.position_list_start = position_list_end;

        self.term_dict_writer
            .insert(self.current_token.unwrap().to_be_bytes(), &term_info)
            .unwrap();

        self.previous_token = self.current_token;
        self.current_token = None;
    }

    pub fn finish(self) {
        self.skip_list_output_writer.terminate().unwrap();
        self.posting_output_writer.terminate().unwrap();
        self.position_skip_list_output_writer
            .map(|w| w.terminate().unwrap());
        self.position_list_output_writer
            .map(|w| w.terminate().unwrap());

        self.term_dict_writer.finish().unwrap().terminate().unwrap();
    }
}
