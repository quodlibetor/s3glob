use anyhow::{Result, anyhow, bail};
use itertools::Itertools as _;

use super::prefix_join;

/// A single part of a glob pattern
///
/// Note that the compiled regexes are designed to match against an _entire_ path segment
#[derive(Debug, Clone)]
pub(crate) enum Glob {
    /// A single `*` or `?`, or a negated character class
    Any { raw: String, not: Option<Vec<char>> },
    /// A synthetic `*` that is used to represent the fact that a glob pattern
    /// ends with a delimiter.
    ///
    /// If a user asks for foo/*/ then they actually want all the things within /, not just foo/*
    SyntheticAny,
    /// A literal string or group of alternatives, like `foo` or `{foo,bar}` or `[abc]`
    Choice { raw: String, allowed: Vec<String> },
    /// A recursive glob, always `**`
    Recursive,
}

impl Glob {
    pub(crate) fn display(&self) -> String {
        match self {
            Glob::Any { raw, .. } => format!("Any({raw})"),
            Glob::Recursive => "Recursive(**)".to_string(),
            Glob::Choice { raw, .. } => format!("Choice({raw})"),
            Glob::SyntheticAny => "SyntheticAny".to_string(),
        }
    }

    pub(crate) fn raw(&self) -> &str {
        match self {
            Glob::Any { raw, .. } => raw,
            Glob::Recursive => "**",
            Glob::Choice { raw, .. } => raw,
            Glob::SyntheticAny => "",
        }
    }

    pub(crate) fn pattern_len(&self) -> usize {
        match self {
            Glob::Any { raw, .. } => raw.len(),
            Glob::Recursive => 2,
            Glob::Choice { raw, .. } => raw.len(),
            Glob::SyntheticAny => 0,
        }
    }

    /// A part that can be inserted directly by the scanner without needing to
    /// do an api call to find the things that match it.
    pub(crate) fn is_choice(&self) -> bool {
        matches!(self, Glob::Choice { .. })
    }

    /// True if this is a negated character class `[!abc]`
    pub(crate) fn is_negated(&self) -> bool {
        matches!(self, Glob::Any { not: Some(_), .. })
    }

    pub(crate) fn is_recursive(&self) -> bool {
        matches!(self, Glob::Recursive)
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
            Glob::Recursive => ".*".to_string(),
            Glob::SyntheticAny => format!("[^{delimiter}]*"),
        }
    }

    /// True if this glob is a literal part and ends with the delimiter
    pub(crate) fn ends_with(&self, delimiter: &str) -> bool {
        match self {
            Glob::Choice { allowed, .. } => allowed.iter().any(|a| a.ends_with(delimiter)),
            _ => false,
        }
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

    #[cfg(test)]
    pub(crate) fn re(&self, delimiter: &str) -> regex::Regex {
        use regex::Regex;

        Regex::new(&self.re_string(delimiter)).unwrap()
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
            while let Some(chr) = iter.next() {
                raw.push(chr);
                match chr {
                    '!' if raw.len() == 2 => {
                        is_negated = true;
                    }
                    ']' if raw.len() == 2 || (is_negated && raw.len() == 3) => {
                        alts.push(chr);
                    }
                    '-' if (!is_negated && raw.len() != 2) || (is_negated && raw.len() != 3) => {
                        // collect the range
                        let next_char = iter
                            .next()
                            .ok_or_else(|| anyhow!("Character class is not closed: {}", raw))?;
                        if next_char == ']' {
                            raw.push(next_char);
                            bail!("Range is not closed: {}", raw);
                        }
                        // the first character is the start of the range and
                        // will be re-inserted next in the coming loop
                        let start = alts.pop().unwrap();
                        let end = next_char;
                        raw.push(end);
                        if end <= start {
                            bail!("Range is invalid (end <= start): {start}-{end} in {raw}");
                        }
                        for c in start..=end {
                            alts.push(c);
                        }
                        continue;
                    }
                    ']' => {
                        ended = true;
                        break;
                    }
                    c => alts.push(c),
                }
            }
            if !ended {
                if raw == "[]" {
                    bail!("Empty character class: {}", raw);
                } else {
                    bail!("Alternation has no closing bracket (missing ']'): {}", raw);
                }
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
        assert_scanner_part!(&scanner.parts[3], SyntheticAny);
        check!(scanner.parts.len() == 4);

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

    #[test]
    fn test_parse_character_range() -> Result<()> {
        let scanner = S3GlobMatcher::parse("[a-c]".to_string(), "/")?;

        assert_scanner_part!(&scanner.parts[0], Choice(vec!["a", "b", "c"]));
        Ok(())
    }

    #[test]
    fn test_parse_numeric_range() -> Result<()> {
        let scanner = S3GlobMatcher::parse("[0-2]".to_string(), "/")?;

        assert_scanner_part!(&scanner.parts[0], Choice(vec!["0", "1", "2"]));
        Ok(())
    }

    #[test]
    fn test_parse_multiple_ranges() -> Result<()> {
        let scanner = S3GlobMatcher::parse("[a-c0-2]".to_string(), "/")?;

        assert_scanner_part!(
            &scanner.parts[0],
            Choice(vec!["a", "b", "c", "0", "1", "2"])
        );
        Ok(())
    }

    #[test]
    fn test_parse_range_with_single_chars() -> Result<()> {
        let scanner = S3GlobMatcher::parse("[a-cx]".to_string(), "/")?;

        assert_scanner_part!(&scanner.parts[0], Choice(vec!["a", "b", "c", "x"]));
        Ok(())
    }

    #[test]
    fn test_parse_range_with_dash_at_start() -> Result<()> {
        let scanner = S3GlobMatcher::parse("[-a-c]".to_string(), "/")?;

        assert_scanner_part!(&scanner.parts[0], Choice(vec!["-", "a", "b", "c"]));
        Ok(())
    }

    #[test]
    fn test_parse_range_with_dash_at_end() -> Result<()> {
        let result = S3GlobMatcher::parse("[a-c-]".to_string(), "/");

        assert!(result.is_err());
        let err_msg = format!("{:#?}", result.unwrap_err());
        println!("err_msg: {err_msg}");
        assert!(err_msg.contains("Range is not closed: [a-c-"));
        Ok(())
    }

    #[test]
    fn test_parse_range_missing_end() {
        let result = S3GlobMatcher::parse("[a-]".to_string(), "/");
        assert!(result.is_err());
        let err_msg = format!("{:#?}", result.unwrap_err());
        println!("err_msg: {err_msg}");
        assert!(err_msg.contains("Range is not closed: [a-"));
    }

    #[test]
    fn test_parse_range_with_negation() -> Result<()> {
        let scanner = S3GlobMatcher::parse("[!a-c]".to_string(), "/")?;

        assert_scanner_part!(
            &scanner.parts[0],
            Any("[!a-c]"),
            &["d", "x", "0"],
            !&["a", "b", "c"]
        );
        Ok(())
    }

    #[test]
    fn test_parse_range_end_less_than_start() {
        let result = S3GlobMatcher::parse("[c-a]".to_string(), "/");
        assert!(result.is_err());
        let err_msg = format!("{:#?}", result.unwrap_err());
        println!("err_msg: {err_msg}");
        assert!(err_msg.contains("Range is invalid (end <= start): c-a in [c-a"));
    }

    #[test]
    fn test_parse_negated_dash() -> Result<()> {
        let scanner = S3GlobMatcher::parse("[!-]".to_string(), "/")?;

        assert_scanner_part!(
            &scanner.parts[0],
            Any("[!-]"),
            &["a", "b", "1", "[", "]"], // should match any character
            !&["-"]                     // should not match dash
        );
        Ok(())
    }

    #[test]
    fn test_parse_unicode_range() -> Result<()> {
        let scanner = S3GlobMatcher::parse("[α-γ]".to_string(), "/")?;

        assert_scanner_part!(
            &scanner.parts[0],
            Choice(vec!["α", "β", "γ"]) // Greek letters alpha through gamma
        );
        Ok(())
    }

    #[test]
    fn test_parse_unicode_with_ascii_range() -> Result<()> {
        let scanner = S3GlobMatcher::parse("[A-Cα-γ]".to_string(), "/")?;

        assert_scanner_part!(
            &scanner.parts[0],
            Choice(vec!["A", "B", "C", "α", "β", "γ"])
        );
        Ok(())
    }

    #[test]
    fn test_parse_emoji_range() -> Result<()> {
        let scanner = S3GlobMatcher::parse("[😀-😃]".to_string(), "/")?;

        assert_scanner_part!(&scanner.parts[0], Choice(vec!["😀", "😁", "😂", "😃"]));
        Ok(())
    }

    #[test]
    fn test_parse_unclosed_character_class() {
        let result = S3GlobMatcher::parse("[a-c".to_string(), "/");
        assert!(result.is_err());
        let err_msg = format!("{:#?}", result.unwrap_err());
        println!("err_msg: {err_msg}");
        assert!(err_msg.contains("Alternation has no closing bracket (missing ']'): [a-c"));
    }

    #[test]
    fn test_parse_empty_character_class() {
        let result = S3GlobMatcher::parse("[]".to_string(), "/");
        assert!(result.is_err());
        let err_msg = format!("{:#?}", result.unwrap_err());
        println!("err_msg: {err_msg}");
        assert!(err_msg.contains("Empty character class: []"));
    }

    #[test]
    fn test_parse_range_dash_only() -> Result<()> {
        let scanner = S3GlobMatcher::parse("[-]".to_string(), "/")?;
        assert_scanner_part!(&scanner.parts[0], Choice(vec!["-"]));
        Ok(())
    }
}
