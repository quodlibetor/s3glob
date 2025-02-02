#![allow(dead_code)]

//! A pattern is a glob that knows how to split itself into a prefix and join with a partial prefix

const GLOB_CHARS: &[char] = &['*', '?', '[', '{'];

use anyhow::{bail, Context as _, Result};
use itertools::Itertools as _;
use regex::Regex;
use tracing::debug;

#[derive(Debug)]
pub struct Scanner {
    raw: String,
    delimiter: char,
    parts: Vec<ScannerPart>,
}

trait Engine {
    fn scan_prefixes(&mut self, prefix: &str, delimiter: &str) -> Result<Vec<String>>;
    fn check_prefixes(&mut self, prefixes: &[String]) -> Result<Vec<String>>;
}

#[cfg(test)]
struct TestEngine {
    prefixes: Vec<String>,
}

/// A scanner takes a glob pattern and can efficiently generate a list of S3
/// prefixes based on it.
impl Scanner {
    pub fn parse(raw: String, delimiter: &str) -> Result<Self> {
        let mut parts = Vec::new();
        let mut remaining = &*raw;
        while !remaining.is_empty() {
            match remaining.find(GLOB_CHARS) {
                Some(idx) => {
                    let next_part = remaining[..idx].to_string();
                    if !next_part.is_empty() {
                        parts.push(ScannerPart::Literal(next_part));
                    }
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

        Ok(Scanner {
            raw,
            delimiter: delimiter.chars().next().unwrap(),
            parts,
        })
    }

    /// Find all S3 prefixes that could match this pattern
    ///
    /// This method works by incrementally building up prefixes and filtering them based on
    /// the pattern parts:
    ///
    /// - For literal parts, it appends them to existing prefixes and queries S3 for matches
    /// - For pattern parts:
    ///   - `*` matches everything, but continues prefix generation
    ///   - `{a,b}` or `[ab]` either filters existing prefixes or generates new ones by appending each alternative
    ///   - `**` stops prefix generation (since it matches any number of path components)
    ///
    /// # Arguments
    ///
    /// * `engine` - Implementation that generates more prefixes given a list of prefixes
    ///
    /// # Returns
    ///
    /// A list of S3 prefixes that could contain matches for this pattern
    ///
    /// # Example
    ///
    /// For pattern "foo/{bar,baz}/qux*":
    /// 1. Start with [""]
    /// 2. Append "foo/" -> ["foo/"]
    /// 3. Append alternatives -> ["foo/bar/", "foo/baz/"]
    /// 4. Append "qux" -> ["foo/bar/qux", "foo/baz/qux"]
    /// 5. Filter by "*" -> keep prefixes whose last component starts with "qux"
    fn find_prefixes(&self, engine: &mut impl Engine) -> Result<Vec<String>> {
        debug!("finding prefixes for {}", self.raw);
        let mut prefixes = vec!["".to_string()];
        let delimiter = self.delimiter.to_string();
        let mut last_part: Option<&ScannerPart> = None;
        let mut regex_so_far = Regex::new("^").unwrap();
        let mut iter = self.parts.iter().peekable();
        while let Some(part) = iter.next() {
            match part {
                ScannerPart::Literal(s) => {
                    // if let Some(ScannerPart::Pattern(Glob::Alternation { .. })) = iter.peek() {
                    for prefix in &mut prefixes {
                        if prefix.ends_with(self.delimiter) && s.starts_with(self.delimiter) {
                            prefix.pop();
                        }
                        prefix.push_str(s);
                    }
                    prefixes = engine.check_prefixes(&prefixes)?;
                    // } else {
                    //     let mut new_prefixes = Vec::new();
                    //     for prefix in &prefixes {
                    //         let np = engine.scan_prefixes(&format!("{prefix}{s}"), &delimiter)?;
                    //         debug!(prefix, literal = s, found = ?np, "prefix scanned");
                    //         new_prefixes.extend(np);
                    //     }
                    //     prefixes = new_prefixes;
                }
                // }
                // filter part
                ScannerPart::Pattern(gl) => {
                    match &gl {
                        Glob::Any { .. } => {
                            // nothing to do, everything matches an any and
                            // querying the api only happens after literals
                            //
                            // this part of the pattern will get added to the
                            // regex_so_far, though
                            // if matches!(
                            //     last_part,
                            //     Some(ScannerPart::Pattern(Glob::Alternation { .. }))
                            // ) {
                            let mut new_prefixes = Vec::new();
                            for prefix in &prefixes {
                                let mut np = engine.scan_prefixes(prefix, &delimiter)?;
                                debug!(prefix, found = ?np, "scanning prefixes for any after alternation");
                                for p in &mut np {
                                    if p.ends_with(self.delimiter) {
                                        p.pop();
                                    }
                                }
                                new_prefixes.extend(np);
                            }
                            prefixes = new_prefixes;
                            // }
                        }
                        Glob::Recursive { .. } => {
                            // exit prefix generation
                            break;
                        }
                        Glob::Alternation { allowed, re, .. } => {
                            // In an alternation we need to check for two cases:
                            // - we are verifying that the middle of the path matches
                            //   one of the alternatives -- this is just a regex filter
                            //
                            //   This happens when the previous part does _not_
                            //   with the delimiter.
                            //
                            // - we are constructing a new path component from the list of alternaives,
                            //   where we just join each alternative with the
                            //   delimiter.
                            //
                            //   This happens when the previous part ended with
                            //   the delimiter, or the pattern starts with an
                            //   alternation

                            let mut is_simple_append = prefixes.is_empty();
                            if let Some(ScannerPart::Literal(s)) = last_part {
                                if s.ends_with(self.delimiter) {
                                    is_simple_append = true;
                                }
                            }

                            if is_simple_append {
                                debug!(
                                    "appending alternatives for pattern: {:?}",
                                    allowed.join(",")
                                );
                                let mut new_prefixes =
                                    Vec::with_capacity(prefixes.len() * allowed.len());
                                for prefix in prefixes {
                                    for alt in allowed {
                                        new_prefixes.push(format!("{prefix}{alt}"));
                                    }
                                }
                                prefixes = new_prefixes;
                            } else {
                                debug!("filtering prefixes for pattern: {:?}", re.as_str());
                                let mut new_prefixes = Vec::with_capacity(prefixes.len());
                                let matcher = Regex::new(&format!(
                                    "{}{}",
                                    regex_so_far.as_str(),
                                    re.as_str()
                                ))
                                .unwrap();
                                for prefix in prefixes {
                                    if matcher.is_match(&prefix) {
                                        new_prefixes.push(prefix);
                                    }
                                }
                                prefixes = new_prefixes;
                            }
                        }
                    }
                }
            }

            // clean up state-tracking
            last_part = Some(part);
            regex_so_far = match part {
                ScannerPart::Literal(s) => {
                    Regex::new(&format!("{}{}", regex_so_far.as_str(), regex::escape(s))).unwrap()
                }
                ScannerPart::Pattern(
                    Glob::Any { re, .. }
                    | Glob::Recursive { re, .. }
                    | Glob::Alternation { re, .. },
                ) => Regex::new(&format!("{}{}", regex_so_far.as_str(), re.as_str())).unwrap(),
            };
        }
        Ok(prefixes)
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
    // assert_scanner_part is defined in this module, but macro_export puts them in the root
    use crate::{assert_scanner_part, setup_logging};
    use assert2::{assert, check};
    use tracing::info;
    //
    // parse tests
    //

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
    fn test_multiple_glob_parsing() -> Result<()> {
        let scanner = Scanner::parse("/{a,b}*/".to_string(), "/")?;
        assert_scanner_part!(&scanner.parts[0], Literal("/"));
        assert_scanner_part!(
            &scanner.parts[1],
            Alternation(vec!["a".to_string(), "b".to_string()])
        );
        assert_scanner_part!(&scanner.parts[2], Any("*"));
        assert_scanner_part!(&scanner.parts[3], Literal("/"));
        check!(scanner.parts.len() == 4);

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
    // find_prefixes tests
    //

    /// A test engine that simulates a real S3 bucket with a set of paths
    struct MockS3Engine {
        paths: Vec<String>,
        calls: Vec<(String, String)>, // (prefix, delimiter) pairs
    }

    impl MockS3Engine {
        fn new(paths: Vec<String>) -> Self {
            Self {
                paths,
                calls: Vec::new(),
            }
        }

        fn assert_calls(&self, expected: &[(impl AsRef<str>, impl AsRef<str>)]) {
            for (i, ((actual_prefix, actual_delim), (expected_prefix, expected_delim))) in
                self.calls.iter().zip(expected).enumerate()
            {
                let i = i + 1;
                assert!(
                    actual_prefix == expected_prefix.as_ref(),
                    "Call {i}: Got prefix {:?}, expected {:?}. Actual calls: {:#?}",
                    actual_prefix,
                    expected_prefix.as_ref(),
                    self.calls
                );
                assert!(
                    actual_delim == expected_delim.as_ref(),
                    "Call {i}: Got delimiter {:?}, expected {:?}. Actual calls: {:#?}",
                    actual_delim,
                    expected_delim.as_ref(),
                    self.calls
                );
            }
            assert!(
                self.calls.len() == expected.len(),
                "Got {} calls, expected {}. Actual calls: {:#?}",
                self.calls.len(),
                expected.len(),
                self.calls
            );
        }

        fn scan_prefixes_inner(&mut self, prefix: &str, delimiter: &str) -> Result<Vec<String>> {
            let result = self
                .paths
                .iter()
                .filter(|p| p.starts_with(prefix))
                .map(|p| {
                    if let Some(end) = p[prefix.len()..].find(delimiter) {
                        // only return the prefix up to the delimiter
                        p[..prefix.len() + end + 1].to_string()
                    } else {
                        p.to_string()
                    }
                })
                .collect();

            info!(prefix, found = ?result, "mocks3 found prefixes  ");
            Ok(result)
        }
    }

    impl Engine for MockS3Engine {
        fn scan_prefixes(&mut self, prefix: &str, delimiter: &str) -> Result<Vec<String>> {
            self.calls.push((prefix.to_string(), delimiter.to_string()));
            self.scan_prefixes_inner(prefix, delimiter)
        }

        fn check_prefixes(&mut self, prefixes: &[String]) -> Result<Vec<String>> {
            let mut valid_prefixes = Vec::new();

            for prefix in prefixes {
                // Use ListObjectsV2 with max-keys=1 to efficiently check existence
                let response = self.scan_prefixes_inner(prefix, "/")?;

                // If there are any objects (or common prefixes), this prefix is valid
                if !response.is_empty() {
                    valid_prefixes.push(prefix.clone());
                }
            }

            Ok(valid_prefixes)
        }
    }

    #[test]
    fn test_find_prefixes_literal() -> Result<()> {
        setup_logging(Some("s3glob=debug"));
        let scanner = Scanner::parse("src/foo/bar".to_string(), "/")?;
        assert_scanner_part!(&scanner.parts[0], Literal("src/foo/bar"));
        let mut engine = MockS3Engine::new(vec!["src/foo/bar".to_string()]);

        let prefixes = scanner.find_prefixes(&mut engine)?;
        assert!(prefixes == vec!["src/foo/bar"]);
        let e: &[(&str, &str)] = &[];
        engine.assert_calls(e);
        Ok(())
    }

    #[test]
    fn test_find_prefixes_alternation_no_any() -> Result<()> {
        setup_logging(Some("s3glob=debug"));
        let scanner = Scanner::parse("src/{foo,bar}/baz".to_string(), "/")?;
        assert_scanner_part!(&scanner.parts[0], Literal("src/"));
        assert_scanner_part!(
            &scanner.parts[1],
            Alternation(vec!["foo".to_string(), "bar".to_string()])
        );
        assert_scanner_part!(&scanner.parts[2], Literal("/baz"));
        let mut engine = MockS3Engine::new(vec![
            "src/foo/baz".to_string(),
            "src/bar/baz".to_string(),
            "src/qux/baz".to_string(), // Should be filtered out
        ]);

        let prefixes = scanner.find_prefixes(&mut engine)?;
        assert!(prefixes == vec!["src/foo/baz", "src/bar/baz"]);
        let e: &[(&str, &str)] = &[];
        engine.assert_calls(e);
        Ok(())
    }

    #[test]
    fn test_find_prefixes_alternation_with_any() -> Result<()> {
        setup_logging(Some("s3glob=debug"));
        let scanner = Scanner::parse("src/{foo,bar}*/baz".to_string(), "/")?;
        println!("scanner_parts for {}:\n{:?}", scanner.raw, scanner.parts);
        assert_scanner_part!(&scanner.parts[0], Literal("src/"));
        assert_scanner_part!(
            &scanner.parts[1],
            Alternation(vec!["foo".to_string(), "bar".to_string()])
        );
        assert_scanner_part!(&scanner.parts[2], Any("*"));
        assert_scanner_part!(&scanner.parts[3], Literal("/baz"));
        let mut engine = MockS3Engine::new(vec![
            "src/foo/baz".to_string(),
            "src/bar/baz".to_string(),
            "src/foo-quux/baz".to_string(),
            "src/qux/baz".to_string(), // Should be filtered out
        ]);

        let prefixes = scanner.find_prefixes(&mut engine)?;
        engine.assert_calls(&[("src/foo", "/"), ("src/bar", "/")]);
        assert!(prefixes == vec!["src/foo/baz", "src/foo-quux/baz", "src/bar/baz"]);
        Ok(())
    }

    #[test]
    fn test_find_prefixes_star() -> Result<()> {
        setup_logging(Some("s3glob=debug"));
        let scanner = Scanner::parse("src/*/main.rs".to_string(), "/")?;
        let mut engine = MockS3Engine::new(vec![
            "src/foo/main.rs".to_string(),
            "src/bar/main.rs".to_string(),
            "src/baz/other.rs".to_string(),
        ]);

        let prefixes = scanner.find_prefixes(&mut engine)?;
        assert!(prefixes == vec!["src/foo/main.rs", "src/bar/main.rs"]);
        engine.assert_calls(&[("src/", "/")]);
        Ok(())
    }

    #[test]
    fn test_find_prefixes_recursive() -> Result<()> {
        let scanner = Scanner::parse("src/**/test.rs".to_string(), "/")?;
        let mut engine = MockS3Engine::new(vec![
            "src/test.rs".to_string(),
            "src/foo/test.rs".to_string(),
            "src/foo/bar/test.rs".to_string(),
            "src/other.rs".to_string(),
        ]);

        let prefixes = scanner.find_prefixes(&mut engine)?;
        // Should stop at src/ since ** matches anything after
        assert!(prefixes == vec!["src/"]);
        let e: &[(&str, &str)] = &[];
        engine.assert_calls(e);
        Ok(())
    }

    #[test]
    fn test_find_prefixes_character_class() -> Result<()> {
        let scanner = Scanner::parse("src/[abc]*.rs".to_string(), "/")?;
        assert_scanner_part!(&scanner.parts[0], Literal("src/"));
        assert_scanner_part!(
            &scanner.parts[1],
            Alternation(vec!["a".to_string(), "b".to_string(), "c".to_string()])
        );
        assert_scanner_part!(&scanner.parts[2], Any("*"));
        assert_scanner_part!(&scanner.parts[3], Literal(".rs"));
        let mut engine = MockS3Engine::new(vec![
            "src/abc.rs".to_string(),
            "src/baz.rs".to_string(),
            "src/cat.rs".to_string(),
            "src/dog.rs".to_string(), // Should be filtered out
        ]);

        let prefixes = scanner.find_prefixes(&mut engine)?;
        assert!(prefixes == vec!["src/abc.rs", "src/baz.rs", "src/cat.rs"]);
        engine.assert_calls(&[("src/", "/")]);
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
                assert!($re.is_match(m), "Regex {} failed to match {m:?}", $re.as_str());
            }
            for m in $expected_does_not_match {
                assert!(!$re.is_match(m), "Regex {} unexpectedly matched {m:?}", $re.as_str());
            }
        };

        ($part:expr, Literal($expected:expr)) => {
            match $part {
                ScannerPart::Literal(s) => assert!(*s == $expected),
                other => panic!("Got {:?}, expected Literal({:?})", other, $expected),
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
