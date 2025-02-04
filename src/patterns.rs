#![allow(dead_code)]

//! A pattern is a glob that knows how to split itself into a prefix and join with a partial prefix

const GLOB_CHARS: &[char] = &['*', '?', '[', '{'];

use std::collections::BTreeSet;

use anyhow::{bail, Context as _, Result};
use itertools::Itertools as _;
use regex::Regex;
use tracing::{debug, trace};

/// A glob pattern, and its compiled regex
///
/// Note that the compiled regexes are designed to match against an _entire_ path segment
#[derive(Debug)]
enum Glob {
    /// A single `*` or `?`, or a negated character class
    Any {
        raw: String,
        alternatives: Option<Vec<String>>,
    },
    /// A literal string or group of alternatives, like `foo` or `{foo,bar}` or `[abc]`
    Choice {
        raw_len: usize,
        allowed: Vec<String>,
    },
    /// A recursive glob, always `**`
    Recursive,
}

impl Glob {
    fn pattern_len(&self) -> usize {
        match self {
            Glob::Any { .. } => 1,
            Glob::Recursive { .. } => 2,
            Glob::Choice { raw_len, .. } => *raw_len,
        }
    }

    /// A part that can be inserted directly by the scanner without needing to
    /// do an api call to find the things that match it.
    fn is_choice(&self) -> bool {
        matches!(self, Glob::Choice { .. })
    }

    fn is_any(&self) -> bool {
        matches!(self, Glob::Any { .. })
    }

    fn re_string(&self, delimiter: &str) -> String {
        match self {
            Glob::Any { raw, alternatives } => match (&**raw, alternatives) {
                (_, Some(alts)) => {
                    let chars = alts.join("");
                    format!("[^{}]", chars)
                }
                ("?", _) => ".".to_string(),
                ("*", _) => format!("[^{delimiter}]*"),
                (_, _) => panic!("invalid any pattern: {raw}"),
            },
            Glob::Choice { allowed, .. } => {
                let re_alts = allowed.iter().map(|a| regex::escape(a)).join("|");
                format!("({})", re_alts)
            }
            Glob::Recursive { .. } => ".*".to_string(),
        }
    }

    fn re(&self, delimiter: &str) -> Regex {
        Regex::new(&self.re_string(delimiter)).unwrap()
    }

    /// Create the combination of two glob patterns
    ///
    /// This will merge all of other into self
    fn combine_with(&mut self, other: &Glob) {
        match (self, other) {
            (Glob::Choice { allowed: sa, .. }, Glob::Choice { allowed: oa, .. }) => {
                let mut new_allowed = Vec::with_capacity(sa.len() * oa.len());
                for choice in sa.iter() {
                    for alt in oa {
                        new_allowed.push(format!("{choice}{alt}"));
                    }
                }
                sa.clear();
                sa.extend(new_allowed);
            }
            _ => panic!("Cannot combine glob with non-choice glob"),
        }
    }
}

#[derive(Debug)]
pub struct Scanner {
    raw: String,
    delimiter: char,
    parts: Vec<Glob>,
}

trait Engine {
    fn scan_prefixes(&mut self, prefix: &str, delimiter: &str) -> Result<Vec<String>>;
    fn check_prefixes(&mut self, prefixes: &[String]) -> Result<Vec<String>>;
}

/// A scanner takes a glob pattern and can efficiently generate a list of S3
/// prefixes based on it.
impl Scanner {
    pub fn parse(raw: String, delimiter: &str) -> Result<Self> {
        let mut parts = Vec::new();
        let mut remaining = &*raw;
        while !remaining.is_empty() {
            let next_idx = remaining.find(GLOB_CHARS);
            match next_idx {
                Some(idx) => {
                    let next_part = remaining[..idx].to_string();
                    if !next_part.is_empty() {
                        parts.push(Glob::Choice {
                            raw_len: next_part.len(),
                            allowed: vec![next_part.clone()],
                        });
                    }
                    let gl = parse_pattern(&remaining[idx..]).context("Parsing pattern")?;
                    remaining = &remaining[idx + gl.pattern_len()..];
                    parts.push(gl);
                }
                None => {
                    parts.push(Glob::Choice {
                        raw_len: remaining.len(),
                        allowed: vec![remaining.to_string()],
                    });
                    break;
                }
            }
        }

        let mut new_parts: Vec<Glob> = Vec::new();
        for part in parts {
            if let Some(last) = new_parts.last_mut() {
                if last.is_choice() && part.is_choice() {
                    last.combine_with(&part);
                } else {
                    new_parts.push(part);
                }
            } else {
                new_parts.push(part);
            }
        }

        debug!(pattern = %raw, parsed = ?new_parts, "parsed pattern");

        Ok(Scanner {
            raw,
            delimiter: delimiter.chars().next().unwrap(),
            parts: new_parts,
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
    /// # Returns
    ///
    /// A list of S3 prefixes that could contain matches for this pattern
    ///
    /// # Example
    ///
    /// For pattern "foo/{bar,baz}*jook/qux*{alpha,beta}/**":
    /// 1. Start with [""]
    /// 2. Append "foo/" -> ["foo/"]
    /// 3. Append alternatives -> ["foo/bar", "foo/baz"]
    /// 4. Search for all folders in ["foo/bar", "foo/baz"]
    /// 4. Append "qux" -> ["foo/bar/qux", "foo/baz/qux"]
    /// 5. Filter by "*" -> keep prefixes whose last component starts with "qux"
    fn find_prefixes(&self, engine: &mut impl Engine) -> Result<Vec<String>> {
        debug!("finding prefixes for {}", self.raw);
        let mut prefixes = vec!["".to_string()];
        let delimiter = self.delimiter.to_string();
        let mut regex_so_far = Regex::new("^").unwrap();
        for part in &self.parts {
            match part {
                Glob::Recursive { .. } => {
                    debug!("found recursive glob, stopping prefix generation");
                    // exit prefix generation
                    break;
                }
                // Any is the only place where we actually need to hit the
                // engine to scan for prefixes, everything else is either a
                // literal append or a regex filter
                Glob::Any { .. } => {
                    let mut new_prefixes = Vec::new();
                    for prefix in &prefixes {
                        let np = engine.scan_prefixes(prefix, &delimiter)?;
                        trace!(prefix, found = ?np, "scanned prefixes for Any");
                        new_prefixes.extend(np);
                    }
                    prefixes = new_prefixes;
                }
                Glob::Choice { allowed, .. } => {
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

                    // if there is no previous part, or the previous part is a literal,
                    // then we can just append the alternatives to the prefixes
                    let is_simple_append = prefixes.len() == 1;

                    if is_simple_append {
                        let mut new_prefixes = Vec::with_capacity(prefixes.len() * allowed.len());
                        for prefix in prefixes {
                            for alt in allowed {
                                new_prefixes.push(format!("{prefix}{alt}"));
                            }
                        }
                        prefixes = engine.check_prefixes(&new_prefixes)?;
                    } else {
                        let mut new_prefixes = Vec::with_capacity(prefixes.len());
                        let mut filters = BTreeSet::new();
                        let mut appends = BTreeSet::new();
                        for choice in allowed {
                            if choice.starts_with(self.delimiter) {
                                let c = choice.chars().skip(1).collect::<String>();
                                if !c.is_empty() {
                                    appends.insert(c);
                                }
                                filters.insert(self.delimiter.to_string());
                            } else if choice.contains(self.delimiter) {
                                let up_to_delim = choice
                                    .chars()
                                    .take_while_inclusive(|c| *c != self.delimiter)
                                    .collect::<String>();
                                filters.insert(regex::escape(&up_to_delim));

                                let after_delim = choice[up_to_delim.len()..].to_string();
                                if !after_delim.is_empty() {
                                    appends.insert(regex::escape(&after_delim));
                                }
                            } else {
                                filters.insert(regex::escape(choice));
                            }
                        }
                        let filters = filters.iter().join("|");
                        let matcher = if !filters.is_empty() {
                            &Regex::new(&format!("{}({})", regex_so_far.as_str(), filters)).unwrap()
                        } else {
                            &regex_so_far
                        };
                        debug!(filters, ?appends, regex = ?matcher, ?prefixes, "filtering and appending to prefixes");
                        for prefix in prefixes {
                            if matcher.is_match(&prefix) {
                                if !appends.is_empty() {
                                    for alt in &appends {
                                        new_prefixes.push(format!("{prefix}{alt}"));
                                    }
                                } else {
                                    new_prefixes.push(prefix);
                                }
                            }
                        }

                        if !appends.is_empty() {
                            prefixes = engine.check_prefixes(&new_prefixes)?;
                        } else {
                            prefixes = new_prefixes;
                        }
                    }
                }
            }

            // clean up state-tracking
            regex_so_far = Regex::new(&format!(
                "{}{}",
                regex_so_far.as_str(),
                part.re_string(&self.delimiter.to_string())
            ))
            .unwrap();
        }
        Ok(prefixes)
    }
}

/// Convert a single pattern into something useful for searching
fn parse_pattern(raw: &str) -> Result<Glob> {
    let mut iter = raw.chars().peekable();
    let mut raw_len = 1;
    Ok(match iter.next().expect("next char must exist") {
        // any patterns
        '?' => Glob::Any {
            raw: "?".to_string(),
            alternatives: None,
        },
        '*' => {
            if matches!(iter.peek(), Some('*')) {
                Glob::Recursive
            } else {
                Glob::Any {
                    raw: "*".to_string(),
                    alternatives: None,
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
            Glob::Choice {
                raw_len,
                allowed: alternatives,
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
            if is_negated {
                Glob::Any {
                    raw: raw[..raw_len].to_string(),
                    alternatives: Some(alternatives),
                }
            } else {
                Glob::Choice {
                    raw_len,
                    allowed: alternatives,
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
    use super::*;
    // assert_scanner_part is defined in this module, but macro_export puts them in the root
    use crate::{assert_scanner_part, setup_logging};
    use assert2::{assert, check};
    use tracing::info;
    //
    // parse tests
    //

    #[test]
    fn test_parse_basic() -> Result<()> {
        let scanner = Scanner::parse("hello*world".to_string(), "/")?;

        assert_scanner_part!(&scanner.parts[0], Choice(vec!["hello"]));
        assert_scanner_part!(&scanner.parts[1], Any("*"));
        assert_scanner_part!(&scanner.parts[2], Choice(vec!["world"]));
        check!(scanner.parts.len() == 3);

        Ok(())
    }

    #[test]
    fn test_parse_multiple_glob() -> Result<()> {
        let scanner = Scanner::parse("/{a,b}*/".to_string(), "/")?;
        assert_scanner_part!(&scanner.parts[0], Choice(vec!["/a", "/b"]));
        assert_scanner_part!(&scanner.parts[1], Any("*"));
        assert_scanner_part!(&scanner.parts[2], OneChoice("/"));
        check!(scanner.parts.len() == 3);

        Ok(())
    }

    #[test]
    fn test_parse_alternation() -> Result<()> {
        let scanner = Scanner::parse("src/{foo,bar}/test".to_string(), "/")?;

        assert_scanner_part!(
            &scanner.parts[0],
            Choice(vec!["src/foo/test", "src/bar/test"])
        );
        check!(scanner.parts.len() == 1);
        Ok(())
    }

    #[test]
    fn test_parse_character_class() -> Result<()> {
        let scanner = Scanner::parse("test[abc]file".to_string(), "/")?;

        assert_scanner_part!(
            &scanner.parts[0],
            Choice(vec!["testafile", "testbfile", "testcfile"])
        );
        check!(scanner.parts.len() == 1);

        Ok(())
    }

    #[test]
    fn test_parse_recursive_glob() -> Result<()> {
        let scanner = Scanner::parse("src/**/*.rs".to_string(), "/")?;
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
        let scanner = Scanner::parse("test[]a]file".to_string(), "/")?;

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
        let scanner = Scanner::parse("test[!a]file".to_string(), "/")?;

        assert_scanner_part!(&scanner.parts[1], Any("[!a]"), &["/", "B"], !&["a"]);

        Ok(())
    }

    #[test]
    fn test_parse_character_class_with_negation_and_bracket() -> Result<()> {
        let scanner = Scanner::parse("test[!]]file".to_string(), "/")?;

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
        let scanner = Scanner::parse("literal/*{foo,bar}/baz".to_string(), "/")?;

        assert_scanner_part!(&scanner.parts[0], OneChoice("literal/"));
        assert_scanner_part!(&scanner.parts[1], Any("*"));
        assert_scanner_part!(&scanner.parts[2], Choice(vec!["foo/baz", "bar/baz"]));
        check!(scanner.parts.len() == 3);

        Ok(())
    }

    #[test]
    fn test_parse_literal_after_any_with_delimiter() -> Result<()> {
        setup_logging(Some("s3glob=trace"));
        let scanner = Scanner::parse("literal/*foo/baz".to_string(), "/")?;
        check!(scanner.parts.len() == 3);

        assert_scanner_part!(&scanner.parts[0], OneChoice("literal/"));
        assert_scanner_part!(&scanner.parts[1], Any("*"));
        assert_scanner_part!(&scanner.parts[2], Choice(vec!["foo/baz"]));

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
        setup_logging(Some("s3glob=trace"));
        let scanner = Scanner::parse("src/foo/bar".to_string(), "/")?;
        // assert_scanner_part!(&scanner.parts[0], OneChoice("src/foo/bar"));
        let mut engine = MockS3Engine::new(vec!["src/foo/bar".to_string()]);

        let prefixes = scanner.find_prefixes(&mut engine)?;
        assert!(prefixes == vec!["src/foo/bar"]);
        let e: &[(&str, &str)] = &[];
        engine.assert_calls(e);
        Ok(())
    }

    #[test]
    fn test_find_prefixes_alternation_no_any() -> Result<()> {
        setup_logging(Some("s3glob=trace"));
        let scanner = Scanner::parse("src/{foo,bar}/baz".to_string(), "/")?;
        // assert_scanner_part!(&scanner.parts[0], OneChoice("src/"));
        // assert_scanner_part!(&scanner.parts[1], Choice(vec!["foo", "bar"]));
        // assert_scanner_part!(&scanner.parts[2], OneChoice("/baz"));
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
        setup_logging(Some("s3glob=trace"));
        let scanner = Scanner::parse("src/{foo,bar}*/baz".to_string(), "/")?;
        println!("scanner_parts for {}:\n{:?}", scanner.raw, scanner.parts);
        // assert_scanner_part!(&scanner.parts[0], OneChoice("src/"));
        // assert_scanner_part!(&scanner.parts[1], Choice(vec!["foo", "bar"]));
        // assert_scanner_part!(&scanner.parts[2], Any("*"));
        // assert_scanner_part!(&scanner.parts[3], OneChoice("/baz"));
        let mut engine = MockS3Engine::new(vec![
            "src/foo/baz".to_string(),
            "src/bar/baz".to_string(),
            "src/foo-quux/baz".to_string(),
            "src/qux/baz".to_string(), // Should be filtered out
        ]);

        let prefixes = scanner.find_prefixes(&mut engine)?;
        // engine.assert_calls(&[("src/foo", "/"), ("src/bar", "/")]);
        assert!(prefixes == vec!["src/foo/baz", "src/foo-quux/baz", "src/bar/baz"]);
        Ok(())
    }

    #[test]
    fn test_find_prefixes_star() -> Result<()> {
        setup_logging(Some("s3glob=trace"));
        let scanner = Scanner::parse("src/*/main.rs".to_string(), "/")?;
        let mut engine = MockS3Engine::new(vec![
            "src/foo/main.rs".to_string(),
            "src/bar/main.rs".to_string(),
            "src/baz/other.rs".to_string(),
        ]);
        info!(?engine.paths);

        let prefixes = scanner.find_prefixes(&mut engine)?;
        assert!(prefixes == vec!["src/foo/main.rs", "src/bar/main.rs"]);
        // engine.assert_calls(&[("src/", "/")]);
        Ok(())
    }

    #[test]
    fn test_find_prefixes_recursive() -> Result<()> {
        setup_logging(Some("s3glob=trace"));
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
        setup_logging(Some("s3glob=trace"));
        let scanner = Scanner::parse("src/[abc]*.rs".to_string(), "/")?;
        assert_scanner_part!(&scanner.parts[0], Choice(vec!["src/a", "src/b", "src/c"]));
        assert_scanner_part!(&scanner.parts[1], Any("*"));
        assert_scanner_part!(&scanner.parts[2], OneChoice(".rs"));
        let mut engine = MockS3Engine::new(vec![
            "src/abc.rs".to_string(),
            "src/baz.rs".to_string(),
            "src/cat.rs".to_string(),
            "src/dog.rs".to_string(), // Should be filtered out
        ]);

        let prefixes = scanner.find_prefixes(&mut engine)?;
        engine.assert_calls(&[("src/a", "/"), ("src/b", "/"), ("src/c", "/")]);
        assert!(prefixes == vec!["src/abc.rs", "src/baz.rs", "src/cat.rs"]);
        Ok(())
    }

    #[test]
    fn test_find_prefixes_alternation_then_any() -> Result<()> {
        let scanner = Scanner::parse("literal/{foo,bar}*/baz".to_string(), "/")?;
        println!("scanner_parts for {}:\n{:#?}", scanner.raw, scanner.parts);

        assert_scanner_part!(
            &scanner.parts[0],
            Choice(vec!["literal/foo", "literal/bar"])
        );
        assert_scanner_part!(&scanner.parts[1], Any("*"));
        assert_scanner_part!(&scanner.parts[2], OneChoice("/baz"));

        let mut engine = MockS3Engine::new(vec![
            "literal/foo/baz".to_string(),
            "literal/foo-extra/baz".to_string(),
            "literal/bar-stuff/baz".to_string(),
            "literal/other/baz".to_string(), // Should be filtered out
        ]);

        let prefixes = scanner.find_prefixes(&mut engine)?;
        engine.assert_calls(&[("literal/foo", "/"), ("literal/bar", "/")]);
        assert!(
            prefixes
                == vec![
                    "literal/foo/baz",
                    "literal/foo-extra/baz",
                    "literal/bar-stuff/baz"
                ]
        );
        Ok(())
    }

    #[test]
    fn test_find_prefixes_alternation_any_literal() -> Result<()> {
        setup_logging(Some("s3glob=trace"));
        let scanner = Scanner::parse("literal/{foo,bar}*quux/baz".to_string(), "/")?;

        assert_scanner_part!(
            &scanner.parts[0],
            Choice(vec!["literal/foo", "literal/bar"])
        );
        assert_scanner_part!(&scanner.parts[1], Any("*"));
        assert_scanner_part!(&scanner.parts[2], OneChoice("quux/baz"));

        let mut engine = MockS3Engine::new(vec![
            "literal/foo-quux/baz".to_string(),
            "literal/bar-quux/baz".to_string(),
            // Should be filtered out
            "literal/foo-something-bar/baz".to_string(),
            "literal/other-quux/baz".to_string(),
            "literal/foo-quux-bar/baz".to_string(),
        ]);

        let prefixes = scanner.find_prefixes(&mut engine)?;
        assert!(prefixes == vec!["literal/foo-quux/baz", "literal/bar-quux/baz"]);
        engine.assert_calls(&[("literal/foo", "/"), ("literal/bar", "/")]);
        Ok(())
    }

    #[test]
    fn test_find_prefixes_any_then_alternation() -> Result<()> {
        let scanner = Scanner::parse("literal/*{foo,bar}/baz".to_string(), "/")?;

        assert_scanner_part!(&scanner.parts[0], OneChoice("literal/"));
        assert_scanner_part!(&scanner.parts[1], Any("*"));
        assert_scanner_part!(&scanner.parts[2], Choice(vec!["foo/baz", "bar/baz"]));

        let mut engine = MockS3Engine::new(vec![
            "literal/something-foo/baz".to_string(),
            "literal/other-bar/baz".to_string(),
            "literal/not-match/baz".to_string(), // Should be filtered out
        ]);

        let prefixes = scanner.find_prefixes(&mut engine)?;
        engine.assert_calls(&[("literal/", "/")]);
        assert!(prefixes == vec!["literal/something-foo/baz", "literal/other-bar/baz"]);
        Ok(())
    }

    #[test]
    fn test_find_prefixes_literal_any_alternation() -> Result<()> {
        setup_logging(Some("s3glob=trace"));
        let scanner = Scanner::parse("literal/quux*{foo,bar}/baz".to_string(), "/")?;

        assert_scanner_part!(&scanner.parts[0], OneChoice("literal/quux"));
        assert_scanner_part!(&scanner.parts[1], Any("*"));
        assert_scanner_part!(&scanner.parts[2], Choice(vec!["foo/baz", "bar/baz"]));

        let mut engine = MockS3Engine::new(vec![
            "literal/quux-foo/baz".to_string(),
            "literal/quux-something-bar/baz".to_string(),
            "literal/quux-other/baz".to_string(), // Should be filtered out
        ]);

        let prefixes = scanner.find_prefixes(&mut engine)?;
        engine.assert_calls(&[("literal/quux", "/")]);
        assert!(prefixes == vec!["literal/quux-foo/baz", "literal/quux-something-bar/baz"]);
        Ok(())
    }

    #[test]
    fn test_find_prefixes_any_after_last_delimiter() -> Result<()> {
        let scanner = Scanner::parse("literal/baz*.rs".to_string(), "/")?;

        assert_scanner_part!(&scanner.parts[0], OneChoice("literal/baz"));
        assert_scanner_part!(&scanner.parts[1], Any("*"));
        assert_scanner_part!(&scanner.parts[2], OneChoice(".rs"));

        let mut engine = MockS3Engine::new(vec![
            "literal/baz.rs".to_string(),
            "literal/baz-extra.rs".to_string(),
            "literal/bazinga.rs".to_string(),
            "literal/other.rs".to_string(), // Should be filtered out
        ]);

        let prefixes = scanner.find_prefixes(&mut engine)?;
        engine.assert_calls(&[("literal/baz", "/")]);
        assert!(
            prefixes
                == vec![
                    "literal/baz.rs",
                    "literal/baz-extra.rs",
                    "literal/bazinga.rs"
                ]
        );
        Ok(())
    }

    #[test]
    fn test_find_prefixes_any_and_character_class() -> Result<()> {
        setup_logging(Some("s3glob=trace"));
        let scanner = Scanner::parse("literal/baz*[ab].rs".to_string(), "/")?;

        assert_scanner_part!(&scanner.parts[0], OneChoice("literal/baz"));
        assert_scanner_part!(&scanner.parts[1], Any("*"));
        assert_scanner_part!(&scanner.parts[2], Choice(vec!["a.rs", "b.rs"]));

        let mut engine = MockS3Engine::new(vec![
            "literal/baz-a.rs".to_string(),
            "literal/baz-extra-b.rs".to_string(),
            "literal/baz-c.rs".to_string(), // Should be filtered out
        ]);

        let prefixes = scanner.find_prefixes(&mut engine)?;
        engine.assert_calls(&[("literal/baz", "/")]);
        assert!(prefixes == vec!["literal/baz-a.rs", "literal/baz-extra-b.rs"]);
        Ok(())
    }

    #[test]
    fn test_find_prefixes_empty_alternative() -> Result<()> {
        setup_logging(Some("s3glob=trace"));
        let scanner = Scanner::parse("src/{,tmp}/file".to_string(), "/")?;
        let mut engine = MockS3Engine::new(vec![
            "src/file".to_string(),
            "src/tmp/file".to_string(),
            "src/other/file".to_string(), // Should be filtered out
        ]);

        let prefixes = scanner.find_prefixes(&mut engine)?;
        assert!(prefixes == vec!["src/tmp/file"]);
        let e: &[(&str, &str)] = &[];
        engine.assert_calls(e);
        Ok(())
    }

    #[test]
    fn test_find_prefixes_empty_alternative_with_delimiter() -> Result<()> {
        setup_logging(Some("s3glob=trace"));
        let scanner = Scanner::parse("src/{,tmp/}file".to_string(), "/")?;
        let mut engine = MockS3Engine::new(vec![
            "src/file".to_string(),
            "src/tmp/file".to_string(),
            "src/other/file".to_string(), // Should be filtered out
        ]);

        let prefixes = scanner.find_prefixes(&mut engine)?;
        assert!(prefixes == vec!["src/file", "src/tmp/file"]);
        let e: &[(&str, &str)] = &[];
        engine.assert_calls(e);
        Ok(())
    }

    #[test]
    fn test_find_prefixes_alternation_with_delimiter() -> Result<()> {
        let scanner = Scanner::parse("src/{foo/bar,baz}/test".to_string(), "/")?;

        assert_scanner_part!(
            &scanner.parts[0],
            Choice(vec!["src/foo/bar/test", "src/baz/test"])
        );

        let mut engine = MockS3Engine::new(vec![
            "src/foo/bar/test".to_string(),
            "src/baz/test".to_string(),
            "src/foo/test".to_string(),     // Should be filtered out
            "src/foo/baz/test".to_string(), // Should be filtered out
        ]);

        let prefixes = scanner.find_prefixes(&mut engine)?;
        assert!(prefixes == vec!["src/foo/bar/test", "src/baz/test"]);
        let e: &[(&str, &str)] = &[]; // No API calls needed since alternation is static
        engine.assert_calls(e);
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

        ($part:expr, Any($expected:expr), $expected_matches:expr, !$expected_does_not_match:expr) => {
            match $part {
                Glob::Any { raw, .. } => {
                    assert!(*raw == $expected);
                    assert_scanner_part!(@test_matches, $part.re("/"), $expected_matches, !$expected_does_not_match);
                }
                other => panic!("Expected Any({:?}), got {:?}", $expected, other),
            }
        };
        ($part:expr, Any($expected:expr)) => {{
            let em: &[&str] = &[];
            let ednm: &[&str] = &[];
            assert_scanner_part!($part, Any($expected), em, !ednm);
        }};

        ($part:expr, NegatedAny($expected:expr), $expected_matches:expr, !$expected_does_not_match:expr) => {
            match $part {
                Glob::Any { raw } => {
                    assert!(*raw == $expected);
                    assert_scanner_part!(@test_matches, $part.re("/"), $expected_matches, !$expected_does_not_match);
                }
                other => panic!("Expected Any({:?}), got {:?}", $expected, other),
            }
        };
        ($part:expr, Recursive, $expected_matches:expr) => {
            match $part {
                Glob::Recursive => {
                    let re = $part.re("/");
                    for m in $expected_matches {
                        check!(re.is_match(m), "matching {m:?} against {}", $part.re_string("/"));
                    }
                }
                other => panic!("Expected Recursive, got {:?}", other),
            }
        };
        ($part:expr, Recursive) => {{
            let em: &[&str] = &[];
            assert_scanner_part!($part, Recursive, em);
        }};
        // convenience rule for testing one-choice patterns
        ($part:expr, OneChoice($expected:expr)) => {
            match $part {
                Glob::Choice{ allowed, .. } => {
                    assert!(allowed.len() == 1);
                    assert!(allowed[0] == $expected);
                },
                other => panic!("Got {:?}, expected Choice([{:?}])", other, $expected),
            }
        };
        ($part:expr, Choice($expected:expr), $expected_matches:expr, !$expected_does_not_match:expr) => {
            match $part {
                Glob::Choice { allowed, .. } => {
                    check!(*allowed == $expected);
                    assert_scanner_part!(@test_matches, $part.re("/"), $expected_matches, !$expected_does_not_match);
                }
                other => panic!("Expected Choice({:?}), got {:?}", $expected, other),
            }
        };
        ($part:expr, Choice($expected:expr)) => {{
            let em: &[&str] = &[];
            let ednm: &[&str] = &[];
            assert_scanner_part!($part, Choice($expected), em, !ednm);
        }};
    }
}
