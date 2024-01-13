#[derive(Clone)]
pub struct TermInfo {
    pub data_offset: usize,
    pub skip_data_len: usize,
    pub posting_data_len: usize,
}
