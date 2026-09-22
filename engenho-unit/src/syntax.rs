//! The unit-file LEXER: sections and `Key=Value` entries, nothing more.
//!
//! The grammar is systemd.syntax(7)'s:
//!
//! * a line is trimmed of surrounding whitespace; an empty line is skipped;
//! * a line whose first character is `#` or `;` is a comment;
//! * `[Name]` opens a section, and a section named twice continues;
//! * `Key=Value` is an entry: the key is trimmed, the value is everything
//!   after the first `=` with leading whitespace removed;
//! * a line ending in `\` continues on the next line, the backslash replaced
//!   by a space; a comment line inside a continuation is skipped.
//!
//! What a value MEANS — quoting, escapes, specifiers, list resets — is left to
//! the directive that reads it ([`crate::unit`]), because it differs per
//! directive.

use std::fmt;

/// One `Key=Value` line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// The key, trimmed.
    pub key: String,
    /// The raw value: leading whitespace removed, nothing else interpreted.
    pub value: String,
    /// The 1-based line the entry started on.
    pub line: usize,
}

/// A section: its name and its entries, in file order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Section {
    /// The name between the brackets.
    pub name: String,
    /// Its entries, in file order.
    pub entries: Vec<Entry>,
}

/// A lexed unit file.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RawUnit {
    /// Sections in first-seen order; a repeated header continues its section.
    pub sections: Vec<Section>,
}

impl RawUnit {
    /// The section called `name`, if the file has one.
    #[must_use]
    pub fn section(&self, name: &str) -> Option<&Section> {
        self.sections.iter().find(|s| s.name == name)
    }
}

/// Why a unit file did not lex.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyntaxError {
    /// An entry appeared before any `[Section]` header.
    EntryOutsideSection {
        /// The 1-based line.
        line: usize,
    },
    /// A line that is not a comment, a header or `Key=Value`.
    NotAnAssignment {
        /// The 1-based line.
        line: usize,
        /// The line's text.
        text: String,
    },
    /// A `[` header without its closing `]`, or an empty name.
    BadSectionHeader {
        /// The 1-based line.
        line: usize,
        /// The line's text.
        text: String,
    },
    /// A `=` with nothing before it.
    EmptyKey {
        /// The 1-based line.
        line: usize,
    },
}

impl fmt::Display for SyntaxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EntryOutsideSection { line } => {
                write!(f, "line {line}: an assignment before any [Section] header")
            }
            Self::NotAnAssignment { line, text } => write!(
                f,
                "line {line}: {text:?} is neither a comment, a [Section] header nor Key=Value"
            ),
            Self::BadSectionHeader { line, text } => {
                write!(
                    f,
                    "line {line}: {text:?} is not a well-formed [Section] header"
                )
            }
            Self::EmptyKey { line } => write!(f, "line {line}: an assignment with an empty key"),
        }
    }
}

impl std::error::Error for SyntaxError {}

/// Lex `text` into sections and entries.
///
/// # Errors
///
/// A [`SyntaxError`] naming the first line that is not part of the grammar.
pub fn lex(text: &str) -> Result<RawUnit, SyntaxError> {
    let mut unit = RawUnit::default();
    let mut current: Option<usize> = None;
    let mut lines = text.lines().enumerate().map(|(i, l)| (i + 1, l));

    while let Some((number, line)) = lines.next() {
        let trimmed = line.trim();
        if trimmed.is_empty() || is_comment(trimmed) {
            continue;
        }
        if trimmed.starts_with('[') {
            let name = trimmed
                .strip_prefix('[')
                .and_then(|rest| rest.strip_suffix(']'))
                .filter(|name| !name.is_empty() && !name.contains(['[', ']']))
                .ok_or_else(|| SyntaxError::BadSectionHeader {
                    line: number,
                    text: trimmed.to_string(),
                })?;
            current = Some(
                if let Some(index) = unit.sections.iter().position(|s| s.name == name) {
                    index
                } else {
                    unit.sections.push(Section {
                        name: name.to_string(),
                        entries: Vec::new(),
                    });
                    unit.sections.len() - 1
                },
            );
            continue;
        }

        // Join continuation lines: a trailing backslash becomes a space.
        let mut logical = trimmed.to_string();
        while logical.ends_with('\\') {
            logical.pop();
            logical.push(' ');
            let Some((_, next)) = next_non_comment(&mut lines) else {
                break;
            };
            logical.push_str(next.trim());
        }
        let logical = logical.trim_end();

        let Some(index) = current else {
            return Err(SyntaxError::EntryOutsideSection { line: number });
        };
        let Some((key, value)) = logical.split_once('=') else {
            return Err(SyntaxError::NotAnAssignment {
                line: number,
                text: logical.to_string(),
            });
        };
        let key = key.trim();
        if key.is_empty() {
            return Err(SyntaxError::EmptyKey { line: number });
        }
        unit.sections[index].entries.push(Entry {
            key: key.to_string(),
            value: value.trim_start().to_string(),
            line: number,
        });
    }
    Ok(unit)
}

fn is_comment(trimmed: &str) -> bool {
    trimmed.starts_with('#') || trimmed.starts_with(';')
}

/// The next line of a continuation, skipping comment lines inside it.
fn next_non_comment<'a>(
    lines: &mut impl Iterator<Item = (usize, &'a str)>,
) -> Option<(usize, &'a str)> {
    lines.find(|(_, line)| !is_comment(line.trim()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sections_entries_and_comments() {
        let unit =
            lex("# c\n[Unit]\nDescription = x y \n; c\n\n[Service]\nUser=a\n[Unit]\nAfter=b\n")
                .unwrap();
        assert_eq!(unit.sections.len(), 2);
        let u = unit.section("Unit").unwrap();
        assert_eq!(
            u.entries.len(),
            2,
            "a repeated header continues its section"
        );
        assert_eq!(u.entries[0].key, "Description");
        assert_eq!(u.entries[0].value, "x y");
        assert_eq!(u.entries[1].line, 9);
        assert_eq!(unit.section("Service").unwrap().entries[0].value, "a");
    }

    #[test]
    fn a_trailing_backslash_continues_the_line_with_a_space() {
        let unit = lex("[Service]\nExecStart=/bin/a \\\n  --b \\\n# skipped\n  --c\n").unwrap();
        let e = &unit.section("Service").unwrap().entries[0];
        assert_eq!(e.value, "/bin/a  --b  --c");
        assert_eq!(e.line, 2);
    }

    #[test]
    fn the_value_keeps_everything_after_the_first_equals() {
        let unit = lex("[Service]\nEnvironment=\"A=b=c\"\n").unwrap();
        assert_eq!(
            unit.section("Service").unwrap().entries[0].value,
            "\"A=b=c\""
        );
    }

    #[test]
    fn malformed_lines_are_typed_errors_naming_the_line() {
        assert_eq!(
            lex("User=a\n"),
            Err(SyntaxError::EntryOutsideSection { line: 1 })
        );
        assert!(matches!(
            lex("[Service]\njust words\n"),
            Err(SyntaxError::NotAnAssignment { line: 2, .. })
        ));
        assert!(matches!(
            lex("[Service\n"),
            Err(SyntaxError::BadSectionHeader { line: 1, .. })
        ));
        assert_eq!(lex("[S]\n=v\n"), Err(SyntaxError::EmptyKey { line: 2 }));
    }
}
