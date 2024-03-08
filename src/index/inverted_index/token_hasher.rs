use tantivy_tokenizer_api::Token;

#[derive(Default)]
pub struct TokenHasher {}

impl TokenHasher {
    pub fn hash_token(&self, token: &Token) -> u64 {
        self.hash_bytes(token.text.as_bytes())
    }

    pub fn hash_bytes(&self, token: &[u8]) -> u64 {
        use std::hash::Hasher;
        let mut hasher = ahash::AHasher::default();
        hasher.write(token);
        hasher.finish()
    }
}
