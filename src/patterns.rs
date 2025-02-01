#![allow(dead_code)]

//! A pattern is a glob that knows how to split itself into a prefix and join with a partial prefix

const GLOB_CHARS: &[char] = &['*', '?', '[', '{'];

use anyhow::{bail, Context as _, Result};
use itertools::Itertools as _;
use regex::Regex;

#[derive(Debug)]
pub struct Scanner {
    raw: String,
    parts: Vec<ScannerPart>,
}

impl Scanner {
    pub fn parse(raw: String, delimiter: &str) -> Result<Self> {
        let mut parts = Vec::new();
        let mut remaining = &*raw;
        while !remaining.is_empty() {
            match remaining.find(GLOB_CHARS) {
                Some(idx) => {
                    let next_part = remaining[..idx].to_string();
                    parts.push(ScannerPart::Literal(next_part));
                    let gl =
                        parse_pattern(delimiter, &remaining[idx..]).context("Parsing pattern")?;
                    remaining = &remaining[idx + gl.raw_len()..];
                    parts.push(ScannerPart::Pattern(gl));
                }
                None => {
                    parts.push(ScannerPart::Literal(remaining.to_string()));
                    break;
                }
            }
        }

        Ok(Scanner { raw, parts })
    }
}

/// Convert a single pattern into something useful for searching
fn parse_pattern(delimiter: &str, raw: &str) -> Result<Glob> {
    let mut iter = raw.chars().peekable();
    let mut raw_len = 1;
    Ok(match iter.next().expect("next char must exist") {
        // any patterns
        '?' => Glob::Any {
            raw: "?".to_string(),
            re: Regex::new(".").unwrap(),
        },
        '*' => {
            if matches!(iter.peek(), Some('*')) {
                Glob::Recursive {
                    re: Regex::new(".*").unwrap(),
                }
            } else {
                Glob::Any {
                    raw: "*".to_string(),
                    // TODO: is it correct for this to be anchored to start and end?
                    re: Regex::new(&format!("^[^{delimiter}]*$")).unwrap(),
                }
            }
        }
        // alternations
        '{' => {
            let mut alternatives = Vec::new();
            let mut alt = String::new();
            let mut ended = false;
            for chr in iter.by_ref() {
                raw_len += 1;
                match chr {
                    ',' => {
                        alternatives.push(alt.clone());
                        alt.clear();
                    }
                    '}' => {
                        alternatives.push(alt);
                        ended = true;
                        break;
                    }
                    c => alt.push(c),
                }
            }
            if !ended {
                bail!("Alternation has no closing brace (missing '}}'): {}", raw);
            }
            let re_alts = alternatives.iter().map(|a| regex::escape(a)).join("|");
            Glob::Alternation {
                raw_len,
                allowed: alternatives,
                re: Regex::new(&format!("({})", re_alts)).unwrap(),
            }
        }
        '[' => {
            let mut alternatives = Vec::new();
            let mut ended = false;
            let mut is_negated = false;
            for chr in iter {
                raw_len += 1;
                match chr {
                    ']' if raw_len == 2 || (is_negated && raw_len == 3) => {
                        alternatives.push(chr.to_string());
                    }
                    '!' if raw_len == 2 => {
                        is_negated = true;
                    }
                    ']' => {
                        ended = true;
                        break;
                    }
                    c => alternatives.push(c.to_string()),
                }
            }
            if !ended {
                bail!("Alternation has no closing bracket (missing ']'): {}", raw);
            }
            let re = if is_negated {
                Regex::new(&format!("[^{}]", alternatives.join(""))).unwrap()
            } else {
                Regex::new(&raw[..raw_len]).unwrap()
            };
            Glob::Alternation {
                raw_len,
                allowed: alternatives,
                re,
            }
        }
        // not a pattern
        e => panic!(
            "[internal error] Unexpected pattern character in parse pattern: {e} starting {raw}"
        ),
    })
}

/// A glob pattern, and its compiled regex
///
/// Note that the compiled regexes are designed to match against an _entire_ path segment
#[derive(Debug)]
enum Glob {
    /// A single `*` or `?`
    Any { raw: String, re: Regex },
    /// A group of alternatives, like `{foo,bar}` or `[abc]`
    Alternation {
        raw_len: usize,
        allowed: Vec<String>,
        re: Regex,
    },
    /// A recursive glob, always `**`
    Recursive { re: Regex },
}

impl Glob {
    fn raw_len(&self) -> usize {
        match self {
            Glob::Any { .. } => 1,
            Glob::Recursive { .. } => 2,
            Glob::Alternation { raw_len, .. } => *raw_len,
        }
    }
}

#[derive(Debug)]
enum ScannerPart {
    Literal(String),
    Pattern(Glob),
}

#[cfg(test)]
mod tests {
    use super::*;
    // this is defined in this module, but macro_export puts them in the root
    use crate::assert_scanner_part;
    use assert2::{assert, check};

    #[test]
    fn test_basic_scanner_parsing() -> Result<()> {
        let scanner = Scanner::parse("hello*world".to_string(), "/")?;
        check!(scanner.parts.len() == 3);

        assert_scanner_part!(&scanner.parts[0], Literal("hello"));
        assert_scanner_part!(&scanner.parts[1], Any("*"));
        assert_scanner_part!(&scanner.parts[2], Literal("world"));

        Ok(())
    }

    #[test]
    fn test_alternation_parsing() -> Result<()> {
        let scanner = Scanner::parse("src/{foo,bar}/test".to_string(), "/")?;
        check!(scanner.parts.len() == 3);

        assert_scanner_part!(&scanner.parts[0], Literal("src/"));
        assert_scanner_part!(
            &scanner.parts[1],
            Alternation(vec!["foo".to_string(), "bar".to_string()])
        );
        assert_scanner_part!(&scanner.parts[2], Literal("/test"));

        Ok(())
    }

    #[test]
    fn test_character_class_parsing() -> Result<()> {
        let scanner = Scanner::parse("test[abc]file".to_string(), "/")?;
        check!(scanner.parts.len() == 3);

        assert_scanner_part!(&scanner.parts[0], Literal("test"));
        assert_scanner_part!(
            &scanner.parts[1],
            Alternation(vec!["a".to_string(), "b".to_string(), "c".to_string()])
        );
        assert_scanner_part!(&scanner.parts[2], Literal("file"));

        Ok(())
    }

    #[test]
    fn test_recursive_glob() -> Result<()> {
        let scanner = Scanner::parse("src/**/*.rs".to_string(), "/")?;
        println!("scanner_parts for {}:\n{:?}", scanner.raw, scanner.parts);
        check!(scanner.parts.len() == 5);

        assert_scanner_part!(&scanner.parts[0], Literal("src/"));
        assert_scanner_part!(
            &scanner.parts[1],
            Recursive,
            &["foo/bar", "foo/bar/baz", ""]
        );
        assert_scanner_part!(&scanner.parts[2], Literal("/"));
        assert_scanner_part!(
            &scanner.parts[3],
            Any("*"),
            &["something_long"],
            !&["something/with/slashes"]
        );

        Ok(())
    }

    #[test]
    fn test_character_class_with_bracket() -> Result<()> {
        let scanner = Scanner::parse("test[]a]file".to_string(), "/")?;

        assert_scanner_part!(
            &scanner.parts[1],
            Alternation(vec!["]".to_string(), "a".to_string()], "[]a]"),
            &["]", "a"],
            !&["b", ""]
        );

        Ok(())
    }

    #[test]
    fn test_negated_character_class() -> Result<()> {
        let scanner = Scanner::parse("test[!a]file".to_string(), "/")?;

        assert_scanner_part!(
            &scanner.parts[1],
            Alternation(vec!["a".to_string()], "[^a]"),
            &["b", "z", "]"],
            !&["a", ""]
        );

        Ok(())
    }

    #[test]
    fn test_character_class_with_negation_and_bracket() -> Result<()> {
        let scanner = Scanner::parse("test[!]]file".to_string(), "/")?;

        assert_scanner_part!(
            &scanner.parts[1],
            Alternation(vec!["]".to_string()], "[^]]"),
            &["a", "b", "["],
            !&["]", ""]
        );

        Ok(())
    }

    //
    // Helpers
    //

    #[macro_export]
    macro_rules! assert_scanner_part {
        // Helper rule for match testing
        (@test_matches, $re:expr, $expected_matches:expr, !$expected_does_not_match:expr) => {
            for m in $expected_matches {
                assert!($re.is_match(m), "Expected regex {} to match {m:?}", $re.as_str());
            }
            for m in $expected_does_not_match {
                assert!(!$re.is_match(m), "Expected regex {} to not match {m:?}", $re.as_str());
            }
        };

        ($part:expr, Literal($expected:expr)) => {
            match $part {
                ScannerPart::Literal(s) => assert!(*s == $expected),
                other => panic!("Expected Literal({:?}), got {:?}", $expected, other),
            }
        };
        ($part:expr, Any($expected:expr), $expected_matches:expr, !$expected_does_not_match:expr) => {
            match $part {
                ScannerPart::Pattern(Glob::Any { raw, re }) => {
                    assert!(*raw == $expected);
                    assert_scanner_part!(@test_matches, re, $expected_matches, !$expected_does_not_match);
                }
                other => panic!("Expected Any({:?}), got {:?}", $expected, other),
            }
        };
        ($part:expr, Any($expected:expr)) => {{
            let em: &[&str] = &[];
            let ednm: &[&str] = &[];
            assert_scanner_part!($part, Any($expected), em, !ednm);
        }};
        ($part:expr, Recursive, $expected_matches:expr) => {
            match $part {
                ScannerPart::Pattern(Glob::Recursive { re }) => {
                    for m in $expected_matches {
                        eprintln!("matching {m:?} against {}", re.as_str());
                        check!(re.is_match(m));
                    }
                }
                other => panic!("Expected Recursive, got {:?}", other),
            }
        };
        ($part:expr, Recursive) => {{
            let em: &[&str] = &[];
            assert_scanner_part!($part, Recursive, em);
        }};
        ($part:expr, Alternation($expected:expr), $expected_matches:expr, !$expected_does_not_match:expr) => {
            match $part {
                ScannerPart::Pattern(Glob::Alternation { allowed, re, .. }) => {
                    check!(*allowed == $expected);
                    assert_scanner_part!(@test_matches, re, $expected_matches, !$expected_does_not_match);
                }
                other => panic!("Expected Alternation({:?}), got {:?}", $expected, other),
            }
        };
        ($part:expr, Alternation($expected:expr)) => {{
            let em: &[&str] = &[];
            let ednm: &[&str] = &[];
            assert_scanner_part!($part, Alternation($expected), em, !ednm);
        }};
        ($part:expr, Alternation($expected_allowed:expr, $expected_re:expr), $expected_matches:expr, !$expected_does_not_match:expr) => {
            match $part {
                ScannerPart::Pattern(Glob::Alternation { allowed, re, .. }) => {
                    check!(*allowed == $expected_allowed);
                    check!(re.as_str() == $expected_re);
                    assert_scanner_part!(@test_matches, re, $expected_matches, !$expected_does_not_match);
                }
                other => panic!(
                    "Expected Alternation({:?}, {:?}), got {:?}",
                    $expected_allowed, $expected_re, other
                ),
            }
        };
        ($part:expr, Alternation($expected_allowed:expr, $expected_re:expr)) => {{
            let em: &[&str] = &[];
            let ednm: &[&str] = &[];
            assert_scanner_part!(
                $part,
                Alternation($expected_allowed, $expected_re),
                em,
                !ednm
            );
        }};
    }
}
