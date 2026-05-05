//! Property tests for the prefix-enumeration matcher.
//!
//! The asserted property: for any pattern P and bucket B, every entity in
//! `keys ∪ logical_prefixes(keys)` that the matcher's compiled regex says
//! matches must appear in the matcher's output. Missing matches are bugs;
//! a missed key is silently invisible to the user.
//!
//! Comparisons in the find-prefixes test normalize trailing slashes
//! (`"b"` ≡ `"b/"`) and strip the empty string. `find_prefixes` returns
//! intentionally loose internal data — `simple_append` produces `"b"`
//! when its append doesn't end with the delimiter and `"b/"` when it
//! does, and patterns whose regex matches `""` can leak the empty
//! prefix. Both are user-equivalent; the strict canonicalization
//! happens later in `get_exact`.
//!
//! The find-prefixes test asserts no false negatives only. Asserting
//! equality on `find_prefixes` directly would require either an oracle
//! that mimics the matcher's enumeration (circular) or a stricter
//! contract on which prefix variants the matcher emits — see
//! `full_pipeline_equals_oracle` for the equality property.
//!
//! The oracle's universe is `keys ∪ logical_prefixes(keys)`: every
//! delimiter-suffixed proper prefix of a bucket key is a first-class S3
//! entity that the matcher legitimately surfaces (see the integration
//! test in `tests/integration.rs` covering pattern
//! `SARS-CoV-1/*/*/*RUN0/*CLONE997`).
//!
//! `find_prefixes` alone cannot resolve `**` (it returns partial prefixes
//! that the engine's `get_all_children` is responsible for expanding).
//! The find-prefixes test skips recursive patterns; the end-to-end test
//! drives the full `get_objects` pipeline through `MockS3Engine` and
//! covers them.
//!
//! Limitations:
//!
//! * **`MockS3Engine` is the system under test, not real S3.** Its
//!   listing semantics (`scan_prefixes_inner`, `check_prefixes`) are an
//!   independent implementation; if it diverges from real S3 in some
//!   edge case, the proptests are blind to that. Integration tests in
//!   `tests/integration.rs` are what catches mock/real divergence.
//! * **The oracle uses the matcher's own compiled regex** (via
//!   `matches_key`). A bug in the regex builder that's symmetric on
//!   both sides won't be caught here — only example-based unit tests
//!   guard the regex builder.
//!
//! Failure regressions are persisted to
//! `proptest-regressions/glob_matcher/proptests.txt` when a property
//! fails. Commit any new file produced by a failing run so the seed
//! is replayed.

use std::collections::BTreeSet;

use proptest::prelude::*;

use super::engine::MockS3Engine;
use super::{PrefixResult, S3GlobMatcher};
use crate::messaging;

const ALPHA: &[char] = &['a', 'b', 'c', 'd'];

/// A single rendered chunk of a glob pattern. Generating tokens (rather than
/// raw bytes) means every produced pattern is parseable.
#[derive(Debug, Clone)]
enum Tok {
    Literal(String),
    Star,
    Question,
    Class(Vec<char>),
    NegClass(Vec<char>),
    Brace(Vec<String>),
    Recursive,
    Delim(char),
}

impl Tok {
    fn render(&self) -> String {
        match self {
            Tok::Literal(s) => s.clone(),
            Tok::Star => "*".to_string(),
            Tok::Question => "?".to_string(),
            Tok::Class(cs) => format!("[{}]", cs.iter().collect::<String>()),
            Tok::NegClass(cs) => format!("[!{}]", cs.iter().collect::<String>()),
            Tok::Brace(alts) => format!("{{{}}}", alts.join(",")),
            Tok::Recursive => "**".to_string(),
            Tok::Delim(c) => c.to_string(),
        }
    }
}

fn alpha_char_strategy() -> impl Strategy<Value = char> {
    prop::sample::select(ALPHA.to_vec())
}

fn literal_strategy() -> impl Strategy<Value = String> {
    prop::collection::vec(alpha_char_strategy(), 1..=3)
        .prop_map(|cs| cs.into_iter().collect::<String>())
}

fn class_chars_strategy() -> impl Strategy<Value = Vec<char>> {
    prop::collection::vec(alpha_char_strategy(), 1..=3).prop_map(|mut v| {
        v.sort();
        v.dedup();
        v
    })
}

fn brace_alts_strategy() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec(literal_strategy(), 2..=3)
}

/// Delimiters chosen to avoid clashing with the alphabet, glob metacharacters
/// (`*?[{`), the brace separator (`,`), or anything that needs escaping inside
/// a regex character class.
fn delimiter_strategy() -> impl Strategy<Value = char> {
    prop::sample::select(vec!['/', ':'])
}

fn tok_strategy(delim: char) -> impl Strategy<Value = Tok> {
    prop_oneof![
        4 => literal_strategy().prop_map(Tok::Literal),
        2 => Just(Tok::Star),
        1 => Just(Tok::Question),
        2 => class_chars_strategy().prop_map(Tok::Class),
        1 => class_chars_strategy().prop_map(Tok::NegClass),
        2 => brace_alts_strategy().prop_map(Tok::Brace),
        1 => Just(Tok::Recursive),
        3 => Just(Tok::Delim(delim)),
    ]
}

fn pattern_strategy(delim: char) -> impl Strategy<Value = String> {
    prop::collection::vec(tok_strategy(delim), 1..=6)
        .prop_map(|toks| toks.iter().map(Tok::render).collect::<String>())
        .prop_filter("non-empty pattern", |s| !s.is_empty())
}

fn key_strategy(delim: char) -> impl Strategy<Value = String> {
    let segment = prop::collection::vec(alpha_char_strategy(), 1..=3)
        .prop_map(|cs| cs.into_iter().collect::<String>());
    prop::collection::vec(segment, 1..=4).prop_map(move |segs| segs.join(&delim.to_string()))
}

fn bucket_strategy(delim: char) -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec(key_strategy(delim), 1..=20).prop_map(|mut keys| {
        keys.sort();
        keys.dedup();
        keys
    })
}

/// Generate `(delimiter, pattern, bucket)` such that the pattern and bucket
/// are constructed using the same delimiter. Patterns may include `**`.
fn delim_pattern_bucket() -> impl Strategy<Value = (char, String, Vec<String>)> {
    delimiter_strategy().prop_flat_map(|d| (Just(d), pattern_strategy(d), bucket_strategy(d)))
}

fn run_async<F: std::future::Future>(fut: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(fut)
}

/// Bucket keys plus every delimiter-suffixed proper prefix of each key — the
/// set of S3 entities the matcher could legitimately return.
fn logical_universe(bucket: &[String], delimiter: char) -> BTreeSet<String> {
    let mut universe = BTreeSet::new();
    for key in bucket {
        universe.insert(key.clone());
        for (idx, ch) in key.char_indices() {
            if ch == delimiter {
                universe.insert(key[..idx + ch.len_utf8()].to_string());
            }
        }
    }
    universe
}

/// Strip a single trailing delimiter so `"b"` and `"b/"` compare equal.
fn normalize(s: &str, delimiter: char) -> &str {
    s.strip_suffix(delimiter).unwrap_or(s)
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        max_global_rejects: 8192,
        ..ProptestConfig::default()
    })]

    /// Every entity in `keys ∪ logical_prefixes` that the regex says matches
    /// must appear in `find_prefixes`' output (no false negatives).
    ///
    /// We deliberately do *not* assert equality here. `find_prefixes` is
    /// internal and intentionally returns loose results — `simple_append`'s
    /// `check_prefixes` uses string-`starts_with` which keeps prefixes
    /// alive that may not be real S3 entities (e.g. `"c"` for bucket
    /// `["ca"]`). The strict check happens later in `get_exact`, which is
    /// where the equality property does hold (see
    /// `full_pipeline_equals_oracle`).
    #[test]
    fn find_prefixes_returns_every_oracle_match(
        (delim, pattern, bucket) in delim_pattern_bucket(),
    ) {
        messaging::silence_for_tests();

        // The Tok generator is constructed to always render to a
        // parseable string; a parse failure is a renderer regression.
        //
        // We intentionally use `cross_delim = false` (strict mode)
        // regardless of the user-facing default. The proptest invariants
        // assume per-segment matching for `?` and `[!...]`; flipping
        // them to lax would let the regex oracle accept entities the
        // prefix-enumeration algorithm intentionally does not surface.
        let mut matcher =
            S3GlobMatcher::parse(pattern.clone(), &delim.to_string(), false)
                .map_err(|e| TestCaseError::fail(format!("parse failed: {e}")))?;
        matcher.set_min_prefixes(0);

        // Recursive patterns (`**`) are skipped here: `find_prefixes`
        // alone returns partial prefixes that the engine's
        // `get_all_children` is responsible for expanding. The
        // end-to-end test (`full_pipeline_equals_oracle`) covers them.
        if !matcher.is_complete() {
            return Ok(());
        }

        let universe = logical_universe(&bucket, delim);
        let oracle: BTreeSet<String> = universe
            .iter()
            .filter(|s| matcher.matches_key(s))
            .map(|s| normalize(s, delim).to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let engine = MockS3Engine::new(bucket.clone());
        let result = run_async(matcher.find_prefixes(engine))
            .map_err(|e| TestCaseError::fail(format!("find_prefixes errored: {e}")))?;

        let actual: BTreeSet<String> = result
            .prefixes
            .iter()
            .cloned()
            .chain(result.objects.iter().filter_map(|o| o.key.clone()))
            .map(|s| normalize(&s, delim).to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let missing: BTreeSet<&String> = oracle.difference(&actual).collect();
        prop_assert!(
            missing.is_empty(),
            "find_prefixes dropped entities the regex oracle says match\n  delim:   {:?}\n  pattern: {}\n  bucket:  {:?}\n  actual:  {:?}\n  oracle:  {:?}\n  missing: {:?}",
            delim, pattern, bucket, actual, oracle, missing,
        );
    }

    /// End-to-end equality: drive `get_objects` through the full pipeline
    /// (`find_prefixes` + `get_exact` / `get_all_children`) against a
    /// `MockS3Engine` bucket, and assert the output stream's keys exactly
    /// equal the regex oracle.
    ///
    /// Oracle branches by `is_complete()`. The branch must mirror
    /// `S3GlobMatcher::get_objects`' dispatch (see the
    /// `is_complete()`/`else` block in `glob_matcher.rs`'s
    /// `get_objects`); if the dispatch ever changes, update both:
    /// * Complete patterns go through `get_exact`, which emits Object for
    ///   real keys and canonical Prefix (delim-suffixed) for logical
    ///   prefixes. Oracle: `keys ∪ logical_prefixes` filtered by regex.
    /// * Recursive patterns go through `get_all_children`, which emits
    ///   only Objects (matching keys under each prefix). Oracle: bucket
    ///   keys filtered by regex.
    #[test]
    fn full_pipeline_equals_oracle(
        (delim, pattern, bucket) in delim_pattern_bucket(),
    ) {
        messaging::silence_for_tests();

        // The Tok generator always renders to a parseable string;
        // parse failure here is a renderer regression.
        //
        // As in `find_prefixes_returns_every_oracle_match`, we force
        // `cross_delim = false` so the per-segment invariants hold,
        // independent of the user-facing default.
        let mut matcher =
            S3GlobMatcher::parse(pattern.clone(), &delim.to_string(), false)
                .map_err(|e| TestCaseError::fail(format!("parse failed: {e}")))?;
        matcher.set_min_prefixes(0);

        let oracle: BTreeSet<String> = if matcher.is_complete() {
            logical_universe(&bucket, delim)
                .iter()
                .filter(|s| matcher.matches_key(s))
                .cloned()
                .collect()
        } else {
            bucket
                .iter()
                .filter(|k| matcher.matches_key(k))
                .cloned()
                .collect()
        };

        let engine = MockS3Engine::new(bucket.clone());
        let collected = run_async(async {
            let list = matcher.get_objects(engine).await?;
            let mut rx = list.rx;
            let mut out: Vec<PrefixResult> = Vec::new();
            while let Some(batch) = rx.recv().await {
                out.extend(batch);
            }
            Ok::<_, anyhow::Error>(out)
        })
        .map_err(|e| TestCaseError::fail(format!("get_objects errored: {e}")))?;

        let actual: BTreeSet<String> = collected.iter().map(|r| r.key()).collect();

        prop_assert_eq!(
            &actual,
            &oracle,
            "full pipeline output disagrees with oracle\n  delim:   {:?}\n  pattern: {}\n  bucket:  {:?}\n  actual:  {:?}\n  oracle:  {:?}\n  missing: {:?}\n  extra:   {:?}",
            delim, pattern, bucket, actual, oracle,
            oracle.difference(&actual).collect::<Vec<_>>(),
            actual.difference(&oracle).collect::<Vec<_>>(),
        );
    }
}
