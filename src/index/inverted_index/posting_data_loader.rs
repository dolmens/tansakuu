use std::{io, path::Path};

use tantivy_common::{HasLen, OwnedBytes};

use crate::{
    postings::{PostingFormat, TermDict},
    Directory,
};

#[derive(Default)]
pub struct PostingDataLoader {}

pub struct PersistentPostingData {
    pub posting_format: PostingFormat,
    pub term_dict: TermDict,
    pub skip_list_data: OwnedBytes,
    pub doc_list_data: OwnedBytes,
    pub position_skip_list_data: OwnedBytes,
    pub position_list_data: OwnedBytes,
}

impl PostingDataLoader {
    pub fn load(
        &self,
        name: &str,
        posting_format: PostingFormat,
        directory: &dyn Directory,
        index_path: &Path,
    ) -> io::Result<PersistentPostingData> {
        let dict_path = index_path.join(name.to_string() + ".dict");
        let dict_data = directory.open_read(&dict_path).unwrap();
        let term_dict = TermDict::open(dict_data)?;

        let skip_list_path = index_path.join(name.to_string() + ".skiplist");
        let skip_list_slice = directory.open_read(&skip_list_path).unwrap();
        let skip_list_data = if skip_list_slice.len() > 0 {
            skip_list_slice.read_bytes()?
        } else {
            OwnedBytes::empty()
        };

        let posting_path = index_path.join(name.to_string() + ".posting");
        let posting_data = directory.open_read(&posting_path).unwrap();
        let posting_data = posting_data.read_bytes()?;

        let position_skip_list_data = if posting_format.has_position_list() {
            let position_skip_list_path = index_path.join(name.to_string() + ".positions.skiplist");
            let position_skip_list_slice = directory.open_read(&position_skip_list_path).unwrap();
            position_skip_list_slice.read_bytes()?
        } else {
            OwnedBytes::empty()
        };
        let position_list_data = if posting_format.has_position_list() {
            let position_list_path = index_path.join(name.to_string() + ".positions");
            let position_list_slice = directory.open_read(&position_list_path).unwrap();
            position_list_slice.read_bytes()?
        } else {
            OwnedBytes::empty()
        };

        Ok(PersistentPostingData {
            posting_format,
            term_dict,
            skip_list_data,
            doc_list_data: posting_data,
            position_skip_list_data,
            position_list_data,
        })
    }
}
