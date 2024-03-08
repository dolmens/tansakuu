use tantivy_tokenizer_api::{BoxTokenStream, Token, TokenStream};

pub struct ChainedTokenStream<'a> {
    cursor: usize,
    token: Token,
    gap: usize,
    streams: Vec<BoxTokenStream<'a>>,
}

impl<'a> ChainedTokenStream<'a> {
    pub fn new(gap: usize, streams: Vec<BoxTokenStream<'a>>) -> Self {
        Self {
            cursor: 0,
            token: Default::default(),
            gap,
            streams,
        }
    }

    pub fn into_boxed_stream(self) -> BoxTokenStream<'a> {
        BoxTokenStream::new(self)
    }
}

impl<'a> TokenStream for ChainedTokenStream<'a> {
    fn advance(&mut self) -> bool {
        let has_token = if self.streams[self.cursor].advance() {
            true
        } else {
            self.cursor += 1;
            self.streams[self.cursor].advance()
        };
        if has_token {
            self.token = self.streams[self.cursor].token().clone();
            self.token.position += self.gap * self.cursor;
        }
        has_token
    }

    fn token(&self) -> &Token {
        &self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.token
    }
}
