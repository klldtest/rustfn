use crate::error::CommandError;

fn matching_closing_quote(open: char) -> Option<char> {
    Some(match open {
        '"' => '"',
        '‘' => '’',
        '‚' => '‛',
        '“' => '”',
        '„' => '‟',
        '⹂' => '⹂',
        '「' => '」',
        '『' => '』',
        '〝' => '〞',
        '﹁' => '﹂',
        '﹃' => '﹄',
        '＂' => '＂',
        '｢' => '｣',
        '«' => '»',
        '‹' => '›',
        '《' => '》',
        '〈' => '〉',
        _ => return None,
    })
}

fn is_any_quote(ch: char) -> bool {
    matching_closing_quote(ch).is_some()
        || matches!(
            ch,
            '"' | '’'
                | '‛'
                | '”'
                | '‟'
                | '⹂'
                | '」'
                | '』'
                | '〞'
                | '﹂'
                | '﹄'
                | '＂'
                | '｣'
                | '»'
                | '›'
                | '》'
                | '〉'
        )
}

/// Cursor-based command lexer supporting quoted arguments and backslash escapes.
#[derive(Debug, Clone)]
pub struct StringView {
    pub index: usize,
    pub previous: usize,
    buffer: String,
}

impl StringView {
    pub fn new(buffer: impl AsRef<str>) -> Self {
        Self {
            index: 0,
            previous: 0,
            buffer: buffer.as_ref().trim().to_owned(),
        }
    }

    pub fn buffer(&self) -> &str {
        &self.buffer
    }

    pub fn eof(&self) -> bool {
        self.index >= self.buffer.len()
    }

    pub fn current(&self) -> Option<char> {
        self.buffer[self.index..].chars().next()
    }

    pub fn undo(&mut self) {
        self.index = self.previous;
    }

    pub fn skip_ws(&mut self) -> bool {
        self.previous = self.index;
        let rest = &self.buffer[self.index..];
        let ws_bytes = rest
            .char_indices()
            .find(|&(_, ch)| !ch.is_whitespace())
            .map(|(idx, _)| idx)
            .unwrap_or(rest.len());
        self.index += ws_bytes;
        self.previous != self.index
    }

    pub fn skip_string(&mut self, prefix: &str) -> bool {
        if self.buffer[self.index..].starts_with(prefix) {
            self.previous = self.index;
            self.index += prefix.len();
            true
        } else {
            false
        }
    }

    pub fn read_rest(&mut self) -> &str {
        self.previous = self.index;
        let rest = &self.buffer[self.index..];
        self.index = self.buffer.len();
        rest
    }

    pub fn get(&mut self) -> Option<char> {
        self.previous = self.index;
        let cur = self.current()?;
        self.index += cur.len_utf8();
        self.current()
    }

    pub fn get_word(&mut self) -> &str {
        self.previous = self.index;
        let rest = &self.buffer[self.index..];
        let word_bytes = rest
            .char_indices()
            .find(|&(_, ch)| ch.is_whitespace())
            .map(|(idx, _)| idx)
            .unwrap_or(rest.len());
        self.index += word_bytes;
        &self.buffer[self.previous..self.index]
    }

    pub fn get_quoted_word(&mut self) -> Result<Option<String>, CommandError> {
        let Some(first) = self.current() else {
            return Ok(None);
        };

        self.previous = self.index;
        let close_quote = matching_closing_quote(first);
        let mut result = String::new();
        if close_quote.is_none() {
            result.push(first);
        }
        self.index += first.len_utf8();

        while let Some(ch) = self.current() {
            self.index += ch.len_utf8();

            if ch == '\\' {
                let Some(next_ch) = self.current() else {
                    if let Some(expected) = close_quote {
                        return Err(CommandError::ExpectedClosingQuote(expected));
                    }
                    return Ok(Some(result));
                };

                let allow_escape = match close_quote {
                    Some(expected) => next_ch == first || next_ch == expected,
                    None => is_any_quote(next_ch),
                };
                if allow_escape {
                    self.index += next_ch.len_utf8();
                    result.push(next_ch);
                } else {
                    result.push('\\');
                }
                continue;
            }

            if close_quote.is_none() && is_any_quote(ch) {
                return Err(CommandError::UnexpectedQuote(ch));
            }

            if let Some(expected) = close_quote
                && ch == expected
            {
                if let Some(after) = self.current()
                    && !after.is_whitespace()
                {
                    return Err(CommandError::InvalidEndOfQuotedString(Some(after)));
                }
                return Ok(Some(result));
            }

            if ch.is_whitespace() && close_quote.is_none() {
                return Ok(Some(result));
            }

            result.push(ch);
        }

        if let Some(expected) = close_quote {
            return Err(CommandError::ExpectedClosingQuote(expected));
        }
        Ok(Some(result))
    }

    pub fn parse_all_quoted_args(&mut self) -> Result<Vec<String>, CommandError> {
        let mut out = Vec::new();
        loop {
            self.skip_ws();
            if self.eof() {
                break;
            }
            if let Some(word) = self.get_quoted_word()? {
                out.push(word);
            }
        }
        Ok(out)
    }
}
