use anyhow::{bail, Result};
use itertools::Itertools as _;

use super::prefix_join;

use regex::Regex;

/// A single part of a glob pattern
///
/// Note that the compiled regexes are designed to match against an _entire_ path segment
#[derive(Debug, Clone)]
pub(crate) enum Glob {
    /// A single `*` or `?`, or a negated character class
    Any { raw: String, not: Option<Vec<char>> },
    /// A literal string or group of alternatives, like `foo` or `{foo,bar}` or `[abc]`
    Choice { raw: String, allowed: Vec<String> },
    /// A recursive glob, always `**`
    Recursive,
}

impl Glob {
    pub(crate) fn display(&self) -> String {
        match self {
            Glob::Any { raw, .. } => format!("Any({raw})"),
            Glob::Recursive { .. } => "Recursive(**)".to_string(),
            Glob::Choice { raw, .. } => format!("Choice({raw})"),
        }
    }

    pub(crate) fn pattern_len(&self) -> usize {
        match self {
            Glob::Any { raw, .. } => raw.len(),
            Glob::Recursive { .. } => 2,
            Glob::Choice { raw, .. } => raw.len(),
        }
    }

    /// A part that can be inserted directly by the scanner without needing to
    /// do an api call to find the things that match it.
    pub(crate) fn is_choice(&self) -> bool {
        matches!(self, Glob::Choice { .. })
    }

    /// True if this is a `*`, `?`, or `[abc]`
    pub(crate) fn is_any(&self) -> bool {
        matches!(self, Glob::Any { .. })
    }

    /// True if this is a negated character class `[!abc]`
    pub(crate) fn is_negated(&self) -> bool {
        matches!(self, Glob::Any { not: Some(_), .. })
    }

    pub(crate) fn re_string(&self, delimiter: &str) -> String {
        match self {
            Glob::Any {
                raw,
                not: alternatives,
            } => match (&**raw, alternatives) {
                (_, Some(alts)) => {
                    let chars = alts.iter().collect::<String>();
                    format!("[^{}]", chars)
                }
                ("?", _) => ".".to_string(),
                ("*", _) => format!("[^{delimiter}]*"),
                (_, _) => panic!("invalid any pattern: {raw}"),
            },
            Glob::Choice { allowed, .. } => {
                if allowed.is_empty() {
                    "".to_string()
                } else if allowed.len() == 1 {
                    regex::escape(&allowed[0])
                } else {
                    let re_alts = allowed.iter().map(|a| regex::escape(a)).join("|");
                    format!("({})", re_alts)
                }
            }
            Glob::Recursive { .. } => ".*".to_string(),
        }
    }

    pub(crate) fn re(&self, delimiter: &str) -> Regex {
        Regex::new(&self.re_string(delimiter)).unwrap()
    }

    /// Create the combination of two glob patterns
    ///
    /// This will merge all of other into self
    pub(crate) fn combine_with(&mut self, other: &Glob) {
        match (self, other) {
            (Glob::Choice { allowed: sa, .. }, Glob::Choice { allowed: oa, .. }) => {
                let mut new_allowed = Vec::with_capacity(sa.len() * oa.len());
                for choice in sa.iter() {
                    for alt in oa {
                        new_allowed.push(prefix_join(choice, alt));
                    }
                }
                sa.clear();
                sa.extend(new_allowed);
            }
            _ => panic!("Cannot combine glob with non-choice glob"),
        }
    }

    pub(crate) fn may_have_delimiter(&self, delimiter: char) -> bool {
        match self {
            Glob::Any { .. } => false,
            Glob::Choice { allowed, .. } => allowed.iter().any(|a| a.contains(delimiter)),
            Glob::Recursive { .. } => true,
        }
    }
}

/// Convert a single pattern into something useful for searching
pub(super) fn parse_pattern(raw: &str) -> Result<Glob> {
    let mut iter = raw.chars().peekable();
    let mut raw = String::new();
    Ok(match iter.next().expect("next char must exist") {
        // any patterns
        '?' => Glob::Any {
            raw: "?".to_string(),
            not: None,
        },
        '*' => {
            if matches!(iter.peek(), Some('*')) {
                Glob::Recursive
            } else {
                Glob::Any {
                    raw: "*".to_string(),
                    not: None,
                }
            }
        }
        // alternations
        '{' => {
            raw.push('{');
            let mut alternatives = Vec::new();
            let mut alt = String::new();
            let mut ended = false;
            for chr in iter.by_ref() {
                raw.push(chr);
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
            Glob::Choice {
                raw,
                allowed: alternatives,
            }
        }
        '[' => {
            raw.push('[');
            let mut alts: Vec<char> = Vec::new();
            let mut ended = false;
            let mut is_negated = false;
            for chr in iter {
                raw.push(chr);
                match chr {
                    ']' if raw.len() == 2 || (is_negated && raw.len() == 3) => {
                        alts.push(chr);
                    }
                    '!' if raw.len() == 2 => {
                        is_negated = true;
                    }
                    ']' => {
                        ended = true;
                        break;
                    }
                    c => alts.push(c),
                }
            }
            if !ended {
                bail!("Alternation has no closing bracket (missing ']'): {}", raw);
            }
            if is_negated {
                Glob::Any {
                    raw,
                    not: Some(alts),
                }
            } else {
                Glob::Choice {
                    raw,
                    allowed: alts.iter().map(|c| c.to_string()).collect(),
                }
            }
        }
        // not a pattern
        e => panic!(
            "[internal error] Unexpected pattern character in parse pattern: {e} starting {raw}"
        ),
    })
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::glob_matcher::S3GlobMatcher;
    use crate::{assert_scanner_part, setup_logging};

    //
    // parse tests
    //

    #[test]
    fn test_parse_basic() -> Result<()> {
        let scanner = S3GlobMatcher::parse("hello*world".to_string(), "/")?;

        assert_scanner_part!(&scanner.parts[0], Choice(vec!["hello"]));
        assert_scanner_part!(&scanner.parts[1], Any("*"));
        assert_scanner_part!(&scanner.parts[2], Choice(vec!["world"]));
        check!(scanner.parts.len() == 3);

        Ok(())
    }

    #[test]
    fn test_parse_multiple_glob() -> Result<()> {
        let scanner = S3GlobMatcher::parse("/{a,b}*/".to_string(), "/")?;
        assert_scanner_part!(&scanner.parts[0], Choice(vec!["/a", "/b"]));
        assert_scanner_part!(&scanner.parts[1], Any("*"));
        assert_scanner_part!(&scanner.parts[2], OneChoice("/"));
        check!(scanner.parts.len() == 3);

        Ok(())
    }

    #[test]
    fn test_parse_alternation() -> Result<()> {
        let scanner = S3GlobMatcher::parse("src/{foo,bar}/test".to_string(), "/")?;

        assert_scanner_part!(
            &scanner.parts[0],
            Choice(vec!["src/foo/test", "src/bar/test"])
        );
        check!(scanner.parts.len() == 1);
        Ok(())
    }

    #[test]
    fn test_parse_character_class() -> Result<()> {
        let scanner = S3GlobMatcher::parse("test[abc]file".to_string(), "/")?;

        assert_scanner_part!(
            &scanner.parts[0],
            Choice(vec!["testafile", "testbfile", "testcfile"])
        );
        check!(scanner.parts.len() == 1);

        Ok(())
    }

    #[test]
    fn test_parse_recursive_glob() -> Result<()> {
        let scanner = S3GlobMatcher::parse("src/**/*.rs".to_string(), "/")?;
        println!("scanner_parts for {}:\n{:?}", scanner.raw, scanner.parts);
        check!(scanner.parts.len() == 5);

        assert_scanner_part!(&scanner.parts[0], OneChoice("src/"));
        assert_scanner_part!(
            &scanner.parts[1],
            Recursive,
            &["foo/bar", "foo/bar/baz", ""]
        );
        assert_scanner_part!(&scanner.parts[2], OneChoice("/"));
        let e: &[&str] = &[];
        assert_scanner_part!(&scanner.parts[3], Any("*"), &["something_long"], !e);

        Ok(())
    }

    #[test]
    fn test_parse_character_class_with_bracket() -> Result<()> {
        let scanner = S3GlobMatcher::parse("test[]a]file".to_string(), "/")?;

        assert_scanner_part!(
            &scanner.parts[0],
            Choice(vec!["test]file", "testafile"]),
            &["test]file", "testafile"],
            !&["test-file", "b", ""]
        );

        Ok(())
    }

    #[test]
    fn test_parse_negated_character_class() -> Result<()> {
        setup_logging(Some("s3glob=trace"));
        let scanner = S3GlobMatcher::parse("test[!a]file".to_string(), "/")?;

        assert_scanner_part!(&scanner.parts[1], Any("[!a]"), &["/", "B"], !&["a"]);

        Ok(())
    }

    #[test]
    fn test_parse_character_class_with_negation_and_bracket() -> Result<()> {
        let scanner = S3GlobMatcher::parse("test[!]]file".to_string(), "/")?;

        assert_scanner_part!(
            &scanner.parts[1],
            Any("[!]]"),
            &["a", "b", "["],
            !&["]", ""]
        );

        Ok(())
    }

    #[test]
    fn test_parse_choice_after_any() -> Result<()> {
        let scanner = S3GlobMatcher::parse("literal/*{foo,bar}/baz".to_string(), "/")?;

        assert_scanner_part!(&scanner.parts[0], OneChoice("literal/"));
        assert_scanner_part!(&scanner.parts[1], Any("*"));
        assert_scanner_part!(&scanner.parts[2], Choice(vec!["foo/baz", "bar/baz"]));
        check!(scanner.parts.len() == 3);

        Ok(())
    }

    #[test]
    fn test_parse_literal_after_any_with_delimiter() -> Result<()> {
        setup_logging(Some("s3glob=trace"));
        let scanner = S3GlobMatcher::parse("literal/*foo/baz".to_string(), "/")?;
        check!(scanner.parts.len() == 3);

        assert_scanner_part!(&scanner.parts[0], OneChoice("literal/"));
        assert_scanner_part!(&scanner.parts[1], Any("*"));
        assert_scanner_part!(&scanner.parts[2], Choice(vec!["foo/baz"]));

        Ok(())
    }
}
