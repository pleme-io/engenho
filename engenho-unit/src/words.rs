//! systemd's word splitting: `extract_first_word(…, EXTRACT_UNQUOTE |
//! EXTRACT_CUNESCAPE)`, the rule `ExecStart=`, `Environment=` and every
//! space-separated list directive are read with.
//!
//! * Words are separated by runs of whitespace (space, tab, CR, LF).
//! * `"…"` and `'…'` quote, and may open or close anywhere in a word:
//!   `a"b c"d` is the one word `ab cd`. An unterminated quote is an error.
//! * A backslash starts a C escape, inside quotes or out: `\a \b \f \n \r
//!   \t \v \\ \" \' \s` (space), `\xHH`, `\NNN` (three octal digits),
//!   `\uHHHH`, `\UHHHHHHHH`. An escape that is none of these, a trailing
//!   backslash, or one that decodes to NUL is an error — never passed
//!   through, because a mis-split argv runs the wrong command.

use std::fmt;

/// Why a value did not split.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WordError {
    /// A quote opened and never closed.
    UnterminatedQuote {
        /// The quote character.
        quote: char,
    },
    /// A backslash as the last character.
    TrailingBackslash,
    /// A backslash followed by something that is not a C escape.
    BadEscape {
        /// The characters after the backslash that failed to decode.
        escape: String,
    },
    /// An escape that decodes to NUL, which no argv or environment can hold.
    NulByte,
}

impl fmt::Display for WordError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnterminatedQuote { quote } => write!(f, "unterminated {quote} quote"),
            Self::TrailingBackslash => f.write_str("a trailing backslash escapes nothing"),
            Self::BadEscape { escape } => write!(f, "\\{escape} is not a C escape systemd accepts"),
            Self::NulByte => f.write_str("an escape decodes to NUL"),
        }
    }
}

impl std::error::Error for WordError {}

/// Split `input` into words, unquoting and C-unescaping each.
///
/// # Errors
///
/// A [`WordError`] for an unterminated quote or an invalid escape.
pub fn split(input: &str) -> Result<Vec<String>, WordError> {
    let mut words = Vec::new();
    let mut chars = input.chars().peekable();
    loop {
        while chars.next_if(|c| is_separator(*c)).is_some() {}
        if chars.peek().is_none() {
            return Ok(words);
        }
        let mut word = String::new();
        let mut quote: Option<char> = None;
        while let Some(c) = chars.next() {
            match (quote, c) {
                (_, '\\') => word.push(unescape_one(&mut chars)?),
                (Some(q), c) if c == q => quote = None,
                (None, '"' | '\'') => quote = Some(c),
                // A separator ends the word only OUTSIDE quotes; inside them
                // it is an ordinary character, which is what quoting is for.
                (None, c) if is_separator(c) => break,
                (_, c) => word.push(c),
            }
        }
        if let Some(quote) = quote {
            return Err(WordError::UnterminatedQuote { quote });
        }
        words.push(word);
    }
}

/// Decode a C escape whose backslash has been consumed.
///
/// # Errors
///
/// A [`WordError`] when the escape is invalid, truncated or NUL.
pub fn unescape_one(chars: &mut impl Iterator<Item = char>) -> Result<char, WordError> {
    let Some(c) = chars.next() else {
        return Err(WordError::TrailingBackslash);
    };
    let simple = match c {
        'a' => Some('\u{7}'),
        'b' => Some('\u{8}'),
        'f' => Some('\u{c}'),
        'n' => Some('\n'),
        'r' => Some('\r'),
        't' => Some('\t'),
        'v' => Some('\u{b}'),
        's' => Some(' '),
        '\\' | '"' | '\'' => Some(c),
        _ => None,
    };
    if let Some(decoded) = simple {
        return Ok(decoded);
    }
    let (radix, digits, first) = match c {
        'x' => (16, 2, None),
        'u' => (16, 4, None),
        'U' => (16, 8, None),
        '0'..='7' => (8, 2, Some(c)),
        _ => {
            return Err(WordError::BadEscape {
                escape: c.to_string(),
            });
        }
    };
    let mut taken = String::new();
    taken.extend(first);
    for _ in 0..digits {
        match chars.next() {
            Some(d) if d.is_digit(radix) => taken.push(d),
            Some(d) => {
                taken.push(d);
                return Err(bad(c, &taken, first.is_some()));
            }
            None => return Err(bad(c, &taken, first.is_some())),
        }
    }
    let code = u32::from_str_radix(&taken, radix).map_err(|_| bad(c, &taken, first.is_some()))?;
    // Octal is three digits whose value fits a byte: `\400` is not a byte.
    if radix == 8 && code > 0o377 {
        return Err(bad(c, &taken, true));
    }
    match char::from_u32(code) {
        Some('\0') => Err(WordError::NulByte),
        Some(decoded) => Ok(decoded),
        None => Err(bad(c, &taken, first.is_some())),
    }
}

fn bad(kind: char, taken: &str, octal: bool) -> WordError {
    let mut escape = String::new();
    if !octal {
        escape.push(kind);
    }
    escape.push_str(taken);
    WordError::BadEscape { escape }
}

/// The separators systemd splits on (`WHITESPACE`).
#[must_use]
pub const fn is_separator(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\r')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(input: &str) -> Vec<String> {
        split(input).unwrap()
    }

    #[test]
    fn whitespace_separates_and_runs_collapse() {
        assert_eq!(s("  a \t b\n c  "), ["a", "b", "c"]);
        assert!(s("   ").is_empty());
    }

    #[test]
    fn quotes_group_and_may_open_mid_word() {
        assert_eq!(s(r#""a b" 'c d'"#), ["a b", "c d"]);
        assert_eq!(s(r#"a"b c"d"#), ["ab cd"]);
        assert_eq!(s(r#""it's" 'say "hi"'"#), ["it's", r#"say "hi""#]);
        assert_eq!(s(r#""" x"#), ["", "x"], "an empty quoted word is a word");
    }

    #[test]
    fn nixpkgs_rendered_exec_lines_split_as_systemd_does() {
        // wyoming-openwakeword: every argument quoted by NixOS's escaping.
        assert_eq!(
            s(r#""/nix/store/x/bin/w" "--uri" "tcp://0.0.0.0:10400" "--threshold" "0.500000""#),
            [
                "/nix/store/x/bin/w",
                "--uri",
                "tcp://0.0.0.0:10400",
                "--threshold",
                "0.500000"
            ]
        );
        // zwave-js: a double-quoted `sh -c` script holding single quotes.
        assert_eq!(
            s(r#"/bin/sh -c "jq -s '.[0] * .[1]' a b > c""#),
            ["/bin/sh", "-c", "jq -s '.[0] * .[1]' a b > c"]
        );
    }

    #[test]
    fn c_escapes_decode_inside_and_outside_quotes() {
        assert_eq!(s(r"a\sb"), ["a b"]);
        assert_eq!(s(r#""a\nb" 'c\td'"#), ["a\nb", "c\td"]);
        assert_eq!(s(r"\x41\101é\U0001F600"), ["AAé😀"]);
        assert_eq!(s(r#"\\ \" \'"#), ["\\", "\"", "'"]);
    }

    #[test]
    fn invalid_escapes_and_quotes_are_errors_not_passthrough() {
        assert_eq!(
            split(r"a\qb"),
            Err(WordError::BadEscape { escape: "q".into() })
        );
        assert_eq!(split("a\\"), Err(WordError::TrailingBackslash));
        assert_eq!(
            split(r"\x4"),
            Err(WordError::BadEscape {
                escape: "x4".into()
            })
        );
        assert_eq!(
            split(r"\xZZ"),
            Err(WordError::BadEscape {
                escape: "xZ".into()
            })
        );
        assert_eq!(
            split(r"\400"),
            Err(WordError::BadEscape {
                escape: "400".into()
            })
        );
        assert_eq!(split(r"\000"), Err(WordError::NulByte));
        assert_eq!(
            split("\"abc"),
            Err(WordError::UnterminatedQuote { quote: '"' })
        );
        assert_eq!(
            split("'abc"),
            Err(WordError::UnterminatedQuote { quote: '\'' })
        );
    }
}
