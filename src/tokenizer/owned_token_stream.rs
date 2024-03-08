use tantivy_tokenizer_api::{BoxTokenStream, Token, TokenStream};

use super::TextAnalyzer;

pub struct OwnedTokenStream {
    token: Token,
    consumed: bool,
}

pub struct OwnedMultiTokenStream {
    cursor: usize,
    tokens: Vec<Token>,
}

pub struct OwnedTextAnalyzerStream<'a> {
    stream: BoxTokenStream<'a>,
    _tokenizer: TextAnalyzer,
}

impl OwnedTokenStream {
    pub fn new(text: String) -> Self {
        let token = Token {
            offset_from: 0,
            offset_to: text.len(),
            position: 0,
            position_length: 1,
            text,
        };

        Self {
            token,
            consumed: false,
        }
    }

    pub fn into_boxed_stream<'a>(self) -> BoxTokenStream<'a> {
        BoxTokenStream::new(self)
    }
}

impl TokenStream for OwnedTokenStream {
    fn advance(&mut self) -> bool {
        if !self.consumed {
            self.consumed = true;
            true
        } else {
            false
        }
    }

    fn token(&self) -> &Token {
        &self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.token
    }
}

impl OwnedMultiTokenStream {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self { cursor: 0, tokens }
    }

    pub fn into_boxed_stream<'a>(self) -> BoxTokenStream<'a> {
        BoxTokenStream::new(self)
    }
}

impl From<BoxTokenStream<'_>> for OwnedMultiTokenStream {
    fn from(mut stream: BoxTokenStream) -> Self {
        let mut tokens = vec![];
        while stream.advance() {
            tokens.push(stream.token().clone());
        }
        Self { cursor: 0, tokens }
    }
}

impl TokenStream for OwnedMultiTokenStream {
    fn advance(&mut self) -> bool {
        if self.cursor < self.tokens.len() {
            self.cursor += 1;
            true
        } else {
            false
        }
    }

    fn token(&self) -> &Token {
        &self.tokens[self.cursor]
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.tokens[self.cursor]
    }
}

impl<'a> OwnedTextAnalyzerStream<'a> {
    pub fn new(tokenizer: TextAnalyzer, text: &'a str) -> Self {
        let mut tokenizer = tokenizer;
        let stream = unsafe { std::mem::transmute(tokenizer.token_stream(text)) };
        Self {
            stream,
            _tokenizer: tokenizer,
        }
    }

    pub fn into_boxed_stream(self) -> BoxTokenStream<'a> {
        BoxTokenStream::new(self)
    }
}

impl<'a> TokenStream for OwnedTextAnalyzerStream<'a> {
    fn advance(&mut self) -> bool {
        self.stream.advance()
    }

    fn token(&self) -> &Token {
        self.stream.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        self.stream.token_mut()
    }
}
