#![allow(dead_code)]

//! A pattern is a glob that knows how to split itself into a prefix and join with a partial prefix

const GLOB_CHARS: &[char] = &['*', '?', '[', '{'];

use std::collections::BTreeSet;

use anyhow::{bail, Context as _, Result};
use globset::GlobMatcher;
use itertools::Itertools as _;
use regex::Regex;
use tracing::{debug, enabled, trace, Level};

mod engine;
pub use engine::{Engine, S3Engine};

/// A thing that knows how to generate and filter S3 prefixes based on a glob pattern
#[derive(Debug, Clone)]
pub struct S3GlobMatcher {
    raw: String,
    delimiter: char,
    parts: Vec<Glob>,
    glob: GlobMatcher,
}

/// A scanner takes a glob pattern and can efficiently generate a list of S3
/// prefixes based on it.
impl S3GlobMatcher {
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
                            raw: next_part.clone(),
                            allowed: vec![next_part.clone()],
                        });
                    }
                    let gl = parse_pattern(&remaining[idx..]).context("Parsing pattern")?;
                    remaining = &remaining[idx + gl.pattern_len()..];
                    parts.push(gl);
                }
                None => {
                    parts.push(Glob::Choice {
                        raw: remaining.to_string(),
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
        let glob = globset::Glob::new(&raw)?;

        Ok(S3GlobMatcher {
            raw,
            delimiter: delimiter.chars().next().unwrap(),
            parts: new_parts,
            glob: glob.compile_matcher(),
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
    pub async fn find_prefixes(&self, engine: &mut impl Engine) -> Result<Vec<String>> {
        debug!("finding prefixes for {}", self.raw);
        let mut prefixes = vec!["".to_string()];
        let delimiter = self.delimiter.to_string();
        let mut regex_so_far = "^".to_string();
        let mut prev_part = None;
        let mut part_iter = self.parts.iter().enumerate();
        for (i, part) in &mut part_iter {
            // only included prefixes in trace logs
            if enabled!(Level::TRACE) {
                trace!(new_part = %part.display(), %regex_so_far, ?prefixes, "scanning for part");
            } else {
                debug!(new_part = %part.display(), %regex_so_far, "scanning for part");
            }
            // We always want to scan for things including the last part,
            // finding more prefixes in it is guaranteed to be slower than
            // just searching because we have to do an api call to check each
            // prefix, instead of allowing aws to list them for us.
            match part {
                Glob::Recursive { .. } => {
                    // we can also skip the last part if it's not a negated character class
                    debug!("found recursive glob, stopping prefix generation");
                    break;
                }
                // Any is the only place where we actually need to hit the
                // engine to scan for prefixes, everything else is either a
                // literal append or a regex filter
                Glob::Any { .. } => {
                    // never scan if the previous part was an any, because the last scan will have
                    // already found all of the prefixes that match the any
                    let is_last_part = i == self.parts.len() - 1;
                    let scan_might_help =
                        !matches!(prev_part, Some(&Glob::Any { .. })) && !is_last_part;
                    if scan_might_help {
                        debug!(part = %part.display(), "scanning for keys in an Any");
                        let mut new_prefixes = Vec::new();
                        for prefix in &prefixes {
                            let np = engine.scan_prefixes(prefix, &delimiter).await?;
                            trace!(%prefix, found = ?np, pattern = %part.display(), "extending prefixes for Any");
                            new_prefixes.extend(np);
                        }
                        prefixes = new_prefixes;
                    }
                    if part.is_negated() {
                        // if this part is a negated character class then we should filter
                        let matcher = Regex::new(&format!(
                            "{regex_so_far}{}",
                            part.re_string(&self.delimiter.to_string())
                        ))
                        .unwrap();
                        debug!(regex = %matcher.as_str(), "filtering for negated Any");
                        prefixes.retain(|p| matcher.is_match(p));
                    }
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
                        debug!(allowed = %allowed.join(","), "simple append");
                        let mut new_prefixes = Vec::with_capacity(prefixes.len() * allowed.len());
                        for prefix in prefixes {
                            for alt in allowed {
                                new_prefixes.push(prefix_join(&prefix, alt));
                            }
                        }
                        prefixes = engine.check_prefixes(&new_prefixes).await?;
                    } else {
                        // Build up the filters and appends
                        let mut filters = BTreeSet::new();
                        let mut appends = BTreeSet::new();
                        for choice in allowed {
                            // the last part is guaranteed to be an Any,
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
                        let filter =
                            Regex::new(&format!("{}({})", regex_so_far.as_str(), filters)).unwrap();
                        let append_matcher =
                            Regex::new(&format!("{}{}", regex_so_far, part.re_string(&delimiter)))
                                .unwrap();
                        trace!(filters, ?appends, regex = ?filter.as_str(), append_regex = %append_matcher.as_str(), ?prefixes, "filtering and appending to prefixes");

                        let new_prefixes = if filters.is_empty() {
                            debug!("no filters, appending");
                            let mut new_prefixes =
                                Vec::with_capacity(prefixes.len() * appends.len());
                            for prefix in prefixes {
                                for alt in &appends {
                                    new_prefixes.push(prefix_join(&prefix, alt));
                                }
                            }
                            new_prefixes
                        } else {
                            debug!("filtering and appending");
                            let mut new_prefixes = Vec::with_capacity(prefixes.len());
                            for prefix in prefixes {
                                if filter.is_match(&prefix) {
                                    // we only need to append if it's not already matched
                                    if !appends.is_empty() && !append_matcher.is_match(&prefix) {
                                        for alt in &appends {
                                            new_prefixes.push(prefix_join(&prefix, alt));
                                        }
                                    } else {
                                        new_prefixes.push(prefix);
                                    }
                                }
                            }
                            trace!(prefixes = ?new_prefixes, "filtered and appended prefixes");
                            new_prefixes
                        };

                        if !appends.is_empty() {
                            trace!(prefixes = ?new_prefixes, "checking appended prefixes");
                            prefixes = engine.check_prefixes(&new_prefixes).await?;
                        } else {
                            debug!("no appends, using new prefixes");
                            prefixes = new_prefixes;
                        }
                    }
                }
            }

            // clean up state-tracking
            regex_so_far = format!(
                "{}{}",
                regex_so_far.as_str(),
                part.re_string(&self.delimiter.to_string())
            );

            prev_part = Some(part);
        }

        Ok(prefixes)
    }

    pub fn is_match(&self, path: &str) -> bool {
        self.glob.is_match(path)
    }
}

fn prefix_join(prefix: &str, alt: &str) -> String {
    // minio doesn't support double forward slashes in the path
    // https://github.com/minio/minio/issues/5874
    // TODO: make this something the user can configure?
    if prefix.ends_with('/') && alt.starts_with('/') {
        format!("{prefix}{}", &alt[1..])
    } else {
        format!("{prefix}{alt}")
    }
}

/// A single part of a glob pattern
///
/// Note that the compiled regexes are designed to match against an _entire_ path segment
#[derive(Debug, Clone)]
enum Glob {
    /// A single `*` or `?`, or a negated character class
    Any { raw: String, not: Option<Vec<char>> },
    /// A literal string or group of alternatives, like `foo` or `{foo,bar}` or `[abc]`
    Choice { raw: String, allowed: Vec<String> },
    /// A recursive glob, always `**`
    Recursive,
}

impl Glob {
    fn display(&self) -> String {
        match self {
            Glob::Any { raw, .. } => format!("Any({raw})"),
            Glob::Recursive { .. } => "Recursive(**)".to_string(),
            Glob::Choice { raw, .. } => format!("Choice({raw})"),
        }
    }

    fn pattern_len(&self) -> usize {
        match self {
            Glob::Any { raw, .. } => raw.len(),
            Glob::Recursive { .. } => 2,
            Glob::Choice { raw, .. } => raw.len(),
        }
    }

    /// A part that can be inserted directly by the scanner without needing to
    /// do an api call to find the things that match it.
    fn is_choice(&self) -> bool {
        matches!(self, Glob::Choice { .. })
    }

    /// True if this is a `*`, `?`, or `[abc]`
    fn is_any(&self) -> bool {
        matches!(self, Glob::Any { .. })
    }

    /// True if this is a negated character class `[!abc]`
    fn is_negated(&self) -> bool {
        matches!(self, Glob::Any { not: Some(_), .. })
    }

    fn re_string(&self, delimiter: &str) -> String {
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
                        new_allowed.push(prefix_join(choice, alt));
                    }
                }
                sa.clear();
                sa.extend(new_allowed);
            }
            _ => panic!("Cannot combine glob with non-choice glob"),
        }
    }
}

/// Convert a single pattern into something useful for searching
fn parse_pattern(raw: &str) -> Result<Glob> {
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
    use tracing::info;

    use super::*;
    // assert_scanner_part is defined in this module, but macro_export puts them in the root
    use crate::glob_matcher::engine::MockS3Engine;
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

    //
    // find_prefixes tests
    //

    #[tokio::test]
    async fn test_find_prefixes_literal() -> Result<()> {
        setup_logging(Some("s3glob=trace"));
        let scanner = S3GlobMatcher::parse("src/foo/bar".to_string(), "/")?;
        let mut engine = MockS3Engine::new(vec!["src/foo/bar".to_string()]);

        let prefixes = scanner.find_prefixes(&mut engine).await?;
        assert!(prefixes == vec!["src/foo/bar"]);
        let e: &[(&str, &str)] = &[];
        engine.assert_calls(e);
        Ok(())
    }

    #[tokio::test]
    async fn test_find_prefixes_alternation_no_any() -> Result<()> {
        setup_logging(Some("s3glob=trace"));
        let scanner = S3GlobMatcher::parse("src/{foo,bar}/baz".to_string(), "/")?;
        let mut engine = MockS3Engine::new(vec![
            "src/foo/baz".to_string(),
            "src/bar/baz".to_string(),
            "src/qux/baz".to_string(), // Should be filtered out
        ]);

        let prefixes = scanner.find_prefixes(&mut engine).await?;
        assert!(prefixes == vec!["src/foo/baz", "src/bar/baz"]);
        let e: &[(&str, &str)] = &[];
        engine.assert_calls(e);
        Ok(())
    }

    #[tokio::test]
    async fn test_find_prefixes_alternation_with_any() -> Result<()> {
        setup_logging(Some("s3glob=trace"));
        let scanner = S3GlobMatcher::parse("src/{foo,bar}*/baz".to_string(), "/")?;
        println!("scanner_parts for {}:\n{:?}", scanner.raw, scanner.parts);
        let mut engine = MockS3Engine::new(vec![
            "src/foo/baz".to_string(),
            "src/bar/baz".to_string(),
            "src/foo-quux/baz".to_string(),
            "src/qux/baz".to_string(), // Should be filtered out
        ]);

        let prefixes = scanner.find_prefixes(&mut engine).await?;
        engine.assert_calls(&[("src/foo", "/"), ("src/bar", "/")]);
        assert!(prefixes == vec!["src/foo-quux/baz", "src/foo/baz", "src/bar/baz",]);
        Ok(())
    }

    #[tokio::test]
    async fn test_find_prefixes_star() -> Result<()> {
        setup_logging(Some("s3glob=trace"));
        let scanner = S3GlobMatcher::parse("src/*/main.rs".to_string(), "/")?;
        let mut engine = MockS3Engine::new(vec![
            "src/foo/main.rs".to_string(),
            "src/bar/main.rs".to_string(),
            "src/baz/other.rs".to_string(),
        ]);
        info!(?engine.paths);

        let prefixes = scanner.find_prefixes(&mut engine).await?;
        assert!(prefixes == vec!["src/bar/main.rs", "src/foo/main.rs"]);
        engine.assert_calls(&[("src/", "/")]);
        Ok(())
    }

    #[tokio::test]
    async fn test_find_prefixes_recursive() -> Result<()> {
        setup_logging(Some("s3glob=trace"));
        let scanner = S3GlobMatcher::parse("src/**/test.rs".to_string(), "/")?;
        let mut engine = MockS3Engine::new(vec![
            "src/test.rs".to_string(),
            "src/foo/test.rs".to_string(),
            "src/foo/bar/test.rs".to_string(),
            "src/other.rs".to_string(),
        ]);

        let prefixes = scanner.find_prefixes(&mut engine).await?;
        // Should stop at src/ since ** matches anything after
        assert!(prefixes == vec!["src/"]);
        let e: &[(&str, &str)] = &[];
        engine.assert_calls(e);
        Ok(())
    }

    #[tokio::test]
    async fn test_find_prefixes_character_class() -> Result<()> {
        setup_logging(Some("s3glob=trace"));
        let scanner = S3GlobMatcher::parse("src/[abc]*.rs".to_string(), "/")?;
        assert_scanner_part!(&scanner.parts[0], Choice(vec!["src/a", "src/b", "src/c"]));
        assert_scanner_part!(&scanner.parts[1], Any("*"));
        assert_scanner_part!(&scanner.parts[2], OneChoice(".rs"));
        let mut engine = MockS3Engine::new(vec![
            "src/abc.rs".to_string(),
            "src/baz.rs".to_string(),
            "src/cat.rs".to_string(),
            "src/dog.rs".to_string(), // Should be filtered out
        ]);

        let prefixes = scanner.find_prefixes(&mut engine).await?;
        engine.assert_calls(&[("src/a", "/"), ("src/b", "/"), ("src/c", "/")]);
        assert!(prefixes == vec!["src/abc.rs", "src/baz.rs", "src/cat.rs"]);
        Ok(())
    }

    #[tokio::test]
    async fn test_find_prefixes_alternation_then_any() -> Result<()> {
        let scanner = S3GlobMatcher::parse("literal/{foo,bar}*/baz".to_string(), "/")?;
        println!("scanner_parts for {}:\n{:#?}", scanner.raw, scanner.parts);

        assert_scanner_part!(
            &scanner.parts[0],
            Choice(vec!["literal/foo", "literal/bar"])
        );
        assert_scanner_part!(&scanner.parts[1], Any("*"));
        assert_scanner_part!(&scanner.parts[2], OneChoice("/baz"));

        let mut engine = MockS3Engine::new(vec![
            "literal/bar-stuff/baz".to_string(),
            "literal/foo-extra/baz".to_string(),
            "literal/foo/baz".to_string(),
            "literal/other/baz".to_string(), // Should be filtered out
        ]);

        let prefixes = scanner.find_prefixes(&mut engine).await?;
        engine.assert_calls(&[("literal/foo", "/"), ("literal/bar", "/")]);
        assert!(
            prefixes
                == vec![
                    "literal/foo-extra/baz",
                    "literal/foo/baz",
                    "literal/bar-stuff/baz",
                ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_find_prefixes_alternation_any_literal() -> Result<()> {
        setup_logging(Some("s3glob=trace"));
        let scanner = S3GlobMatcher::parse("literal/{foo,bar}*quux/baz".to_string(), "/")?;

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

        let prefixes = scanner.find_prefixes(&mut engine).await?;
        assert!(prefixes == vec!["literal/foo-quux/baz", "literal/bar-quux/baz"]);
        engine.assert_calls(&[("literal/foo", "/"), ("literal/bar", "/")]);
        Ok(())
    }

    #[tokio::test]
    async fn test_find_prefixes_any_then_alternation() -> Result<()> {
        let scanner = S3GlobMatcher::parse("literal/*{foo,bar}/baz".to_string(), "/")?;

        assert_scanner_part!(&scanner.parts[0], OneChoice("literal/"));
        assert_scanner_part!(&scanner.parts[1], Any("*"));
        assert_scanner_part!(&scanner.parts[2], Choice(vec!["foo/baz", "bar/baz"]));

        let mut engine = MockS3Engine::new(vec![
            "literal/something-foo/baz".to_string(),
            "literal/other-bar/baz".to_string(),
            "literal/not-match/baz".to_string(), // Should be filtered out
        ]);

        let prefixes = scanner.find_prefixes(&mut engine).await?;
        engine.assert_calls(&[("literal/", "/")]);
        assert!(prefixes == vec!["literal/other-bar/baz", "literal/something-foo/baz"]);
        Ok(())
    }

    #[tokio::test]
    async fn test_find_prefixes_literal_any_alternation() -> Result<()> {
        setup_logging(Some("s3glob=trace"));
        let scanner = S3GlobMatcher::parse("literal/quux*{foo,bar}/baz".to_string(), "/")?;

        assert_scanner_part!(&scanner.parts[0], OneChoice("literal/quux"));
        assert_scanner_part!(&scanner.parts[1], Any("*"));
        assert_scanner_part!(&scanner.parts[2], Choice(vec!["foo/baz", "bar/baz"]));

        let mut engine = MockS3Engine::new(vec![
            "literal/quux-foo/baz".to_string(),
            "literal/quux-something-bar/baz".to_string(),
            "literal/quux-other/baz".to_string(), // Should be filtered out
        ]);

        let prefixes = scanner.find_prefixes(&mut engine).await?;
        engine.assert_calls(&[("literal/quux", "/")]);
        assert!(prefixes == vec!["literal/quux-foo/baz", "literal/quux-something-bar/baz"]);
        Ok(())
    }

    #[tokio::test]
    async fn test_find_prefixes_any_after_last_delimiter() -> Result<()> {
        let scanner = S3GlobMatcher::parse("literal/baz*.rs".to_string(), "/")?;

        assert_scanner_part!(&scanner.parts[0], OneChoice("literal/baz"));
        assert_scanner_part!(&scanner.parts[1], Any("*"));
        assert_scanner_part!(&scanner.parts[2], OneChoice(".rs"));

        let mut engine = MockS3Engine::new(vec![
            "literal/baz.rs".to_string(),
            "literal/baz-extra.rs".to_string(),
            "literal/bazinga.rs".to_string(),
            "literal/other.rs".to_string(), // Should be filtered out
        ]);

        let prefixes = scanner.find_prefixes(&mut engine).await?;
        engine.assert_calls(&[("literal/baz", "/")]);
        assert!(
            prefixes
                == vec![
                    "literal/baz-extra.rs",
                    "literal/baz.rs",
                    "literal/bazinga.rs"
                ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_find_prefixes_any_and_character_class() -> Result<()> {
        setup_logging(Some("s3glob=trace"));
        let scanner = S3GlobMatcher::parse("literal/baz*[ab].rs".to_string(), "/")?;

        assert_scanner_part!(&scanner.parts[0], OneChoice("literal/baz"));
        assert_scanner_part!(&scanner.parts[1], Any("*"));
        assert_scanner_part!(&scanner.parts[2], Choice(vec!["a.rs", "b.rs"]));

        let mut engine = MockS3Engine::new(vec![
            "literal/baz-a.rs".to_string(),
            "literal/baz-extra-b.rs".to_string(),
            "literal/baz-c.rs".to_string(), // Should be filtered out
        ]);

        let prefixes = scanner.find_prefixes(&mut engine).await?;
        engine.assert_calls(&[("literal/baz", "/")]);
        assert!(prefixes == vec!["literal/baz-a.rs", "literal/baz-extra-b.rs"]);
        Ok(())
    }

    #[tokio::test]
    async fn test_find_prefixes_empty_alternative() -> Result<()> {
        setup_logging(Some("s3glob=trace"));
        let scanner = S3GlobMatcher::parse("src/{,tmp}/file".to_string(), "/")?;
        let mut engine = MockS3Engine::new(vec![
            "src/file".to_string(),
            "src/tmp/file".to_string(),
            "src/other/file".to_string(), // Should be filtered out
        ]);

        let prefixes = scanner.find_prefixes(&mut engine).await?;
        // TODO: there is a legitimate case that this should only be
        // src/tmp/file, but we strip double forward slashes to work around
        // minio
        assert!(prefixes == vec!["src/file", "src/tmp/file"]);
        let e: &[(&str, &str)] = &[];
        engine.assert_calls(e);
        Ok(())
    }

    #[tokio::test]
    async fn test_find_prefixes_empty_alternative_with_delimiter() -> Result<()> {
        setup_logging(Some("s3glob=trace"));
        let scanner = S3GlobMatcher::parse("src/{,tmp/}file".to_string(), "/")?;
        let mut engine = MockS3Engine::new(vec![
            "src/file".to_string(),
            "src/tmp/file".to_string(),
            "src/other/file".to_string(), // Should be filtered out
        ]);

        let prefixes = scanner.find_prefixes(&mut engine).await?;
        assert!(prefixes == vec!["src/file", "src/tmp/file"]);
        let e: &[(&str, &str)] = &[];
        engine.assert_calls(e);
        Ok(())
    }

    #[tokio::test]
    async fn test_find_prefixes_alternation_with_delimiter() -> Result<()> {
        let scanner = S3GlobMatcher::parse("src/{foo/bar,baz}/test".to_string(), "/")?;

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

        let prefixes = scanner.find_prefixes(&mut engine).await?;
        assert!(prefixes == vec!["src/foo/bar/test", "src/baz/test"]);
        let e: &[(&str, &str)] = &[]; // No API calls needed since alternation is static
        engine.assert_calls(e);
        Ok(())
    }

    #[tokio::test]
    async fn test_find_prefixes_negative_class_start() -> Result<()> {
        setup_logging(Some("s3glob=trace"));
        let scanner = S3GlobMatcher::parse("[!a]*/foo".to_string(), "/")?;
        let mut engine = MockS3Engine::new(vec![
            "b/foo".to_string(),
            "c/foo".to_string(),
            "xyz/foo".to_string(),
            "a/foo".to_string(), // Should be filtered out
        ]);

        let prefixes = scanner.find_prefixes(&mut engine).await?;
        assert!(prefixes == vec!["b/foo", "c/foo", "xyz/foo"]);
        engine.assert_calls(&[("", "/")]);
        Ok(())
    }

    #[tokio::test]
    async fn test_find_prefixes_negative_class_after_wildcard() -> Result<()> {
        setup_logging(Some("s3glob=trace"));
        let scanner = S3GlobMatcher::parse("*[!f]oo".to_string(), "/")?;
        let mut engine = MockS3Engine::new(vec![
            "zoo".to_string(),
            "boo".to_string(),
            "foo".to_string(),           // Should be filtered out
            "something/foo".to_string(), // Should be filtered out
        ]);

        let prefixes = scanner.find_prefixes(&mut engine).await?;
        assert!(prefixes == vec!["boo", "zoo"]);
        // TODO: this could be improved to only call the engine once
        Ok(())
    }

    #[tokio::test]
    async fn test_find_prefixes_negative_class_between_alternations() -> Result<()> {
        setup_logging(Some("s3glob=trace"));
        let scanner = S3GlobMatcher::parse("{foo,bar}[!z]*/baz".to_string(), "/")?;
        let mut engine = MockS3Engine::new(vec![
            "foo-abc/baz".to_string(),
            "bar-def/baz".to_string(),
            "fooz/baz".to_string(),  // Should be filtered out
            "barz/baz".to_string(),  // Should be filtered out
            "other/baz".to_string(), // Should be filtered out
        ]);

        let prefixes = scanner.find_prefixes(&mut engine).await?;
        assert!(prefixes == vec!["foo-abc/baz", "bar-def/baz"]);
        engine.assert_calls(&[("foo", "/"), ("bar", "/")]);
        Ok(())
    }

    #[tokio::test]
    async fn test_find_prefixes_multiple_negative_classes() -> Result<()> {
        setup_logging(Some("s3glob=trace"));
        let scanner = S3GlobMatcher::parse("[!a]*[!b]/foo".to_string(), "/")?;
        let mut engine = MockS3Engine::new(vec![
            "c-x/foo".to_string(),
            "d-y/foo".to_string(),
            // filtered out
            "a-b/foo".to_string(), // (both conditions fail)
            "a-x/foo".to_string(), // (first char is a)
            "c-b/foo".to_string(), // (second part starts with b)
        ]);

        let prefixes = scanner.find_prefixes(&mut engine).await?;
        assert!(prefixes == vec!["c-x/foo", "d-y/foo"]);
        engine.assert_calls(&[("", "/")]);
        Ok(())
    }

    #[tokio::test]
    async fn test_find_prefixes_negative_class_with_delimiter() -> Result<()> {
        setup_logging(Some("s3glob=trace"));
        let scanner = S3GlobMatcher::parse("foo/[!/]/bar".to_string(), "/")?;
        assert_scanner_part!(&scanner.parts[0], OneChoice("foo/"));
        assert_scanner_part!(&scanner.parts[1], Any("[!/]"));
        assert_scanner_part!(&scanner.parts[2], OneChoice("/bar"));

        let mut engine = MockS3Engine::new(vec![
            "foo/x/bar".to_string(),
            "foo/a/bar".to_string(),
            "foo//bar".to_string(),    // Should be filtered out
            "foo/a/b/bar".to_string(), // Should be filtered out (too many segments)
            "foo///bar".to_string(),   // Should be filtered out (the excluded char)
        ]);

        let prefixes = scanner.find_prefixes(&mut engine).await?;
        assert!(prefixes == vec!["foo/a/bar", "foo/x/bar"]);
        engine.assert_calls(&[("foo/", "/")]);
        Ok(())
    }

    #[tokio::test]
    async fn test_find_prefixes_complex_negative_pattern() -> Result<()> {
        setup_logging(Some("s3glob=trace"));
        let scanner = S3GlobMatcher::parse("*{foo,bar}*[!Z]/baz".to_string(), "/")?;
        let mut engine = MockS3Engine::new(vec![
            "x-foo-a/baz".to_string(),
            "y-bar-b/baz".to_string(),
            // filtered out
            "x-foo-Z/baz".to_string(), // (ends with Z)
            "y-bar-Z/baz".to_string(), // (ends with Z)
            "x-baz-a/baz".to_string(), // (middle not foo/bar)
        ]);

        let prefixes = scanner.find_prefixes(&mut engine).await?;
        assert!(prefixes == vec!["x-foo-a/baz", "y-bar-b/baz"]);
        // TODO: this could be improved to only call the engine once
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
