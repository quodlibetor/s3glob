use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context as _, Result};
use aws_sdk_s3::Client;
use aws_sdk_s3::types::Object;
use num_format::{Locale, ToFormattedString as _};
use tokio::sync::Semaphore;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, trace, warn};

#[cfg(test)]
use std::sync::Mutex;
#[cfg(test)]
use tracing::info;

use crate::{S3Object, add_atomic, progressln};

use super::{LiveStatus, PrefixResult, PrefixSearchResult};

#[async_trait::async_trait]
pub trait Engine: Send + Sync + 'static {
    /// List the immediate children of `prefix` using `delimiter`.
    ///
    /// If `max_prefixes` is `Some(n)`, pagination may stop early —
    /// either once at least `n` common prefixes have accumulated, or
    /// once the implementation concludes the parent is flat-dense and
    /// further pagination is unlikely to yield prefixes. Both
    /// early-exit paths set `ScanResult::truncated`; callers should
    /// treat a truncated result as "fall back to listing this prefix"
    /// rather than relying on a partial expansion.
    async fn scan_prefixes(
        &mut self,
        prefix: &str,
        delimiter: &str,
        max_prefixes: Option<usize>,
    ) -> Result<ScanResult>;

    /// Single-page delimiter-less list of `prefix`, capped at `max_keys`.
    ///
    /// Returns up to `max_keys` objects under `prefix`. `truncated` is true
    /// if S3 indicates more results exist. When `truncated` is false the
    /// returned objects are the complete content under `prefix`.
    async fn probe_prefix(&mut self, prefix: &str, max_keys: i32) -> Result<ScanResult>;

    async fn check_prefixes<P>(
        &mut self,
        prefixes: P,
        max_parallelism: usize,
    ) -> Result<BTreeSet<String>>
    where
        P: IntoIterator<Item = String> + Send + Sync + 'static,
        P::IntoIter: Send + Sync + 'static;

    /// Verify and categorize objects in `presult`.
    ///
    /// For each prefix in `presult`, emit either an Object (if it is a real
    /// key) or a Prefix (if it is a logical prefix only). Prefix output is
    /// canonicalized to end with `delimiter`. Empty prefixes are skipped.
    /// Already-matched objects in `presult.objects` are filtered through
    /// `matcher` and emitted as Objects. Used when the pattern is
    /// "complete" (no `**`).
    async fn get_exact(
        &self,
        presult: PrefixSearchResult,
        delimiter: char,
        status: &LiveStatus,
        matcher: &regex::Regex,
        tx: &UnboundedSender<Vec<PrefixResult>>,
        permit: Arc<Semaphore>,
    ) -> Result<()>;

    /// List all objects under each `presult` prefix which match matcher.
    ///
    ///  Used when the pattern contains `**`.
    async fn get_all_children(
        &self,
        presult: PrefixSearchResult,
        matcher: Arc<regex::Regex>,
        status: &LiveStatus,
        tx: &UnboundedSender<Vec<PrefixResult>>,
        permit: Arc<Semaphore>,
    ) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct S3Engine {
    client: Client,
    bucket: String,
}

impl S3Engine {
    pub fn new(client: Client, bucket: String) -> Self {
        Self { client, bucket }
    }
}

async fn list_matching_objects(
    client: Client,
    bucket: String,
    prefix: String,
    matcher: Arc<regex::Regex>,
    total_objects: Arc<AtomicUsize>,
    tx: UnboundedSender<Vec<PrefixResult>>,
) -> Result<()> {
    let mut paginator = client
        .list_objects_v2()
        .bucket(bucket.clone())
        .prefix(prefix)
        .into_paginator()
        .send();

    while let Some(page) = paginator.next().await {
        let page = page?;
        if let Some(contents) = page.contents {
            let mut matching_objects = Vec::new();
            total_objects.fetch_add(contents.len(), Ordering::Relaxed);
            for obj in contents {
                if let Some(key) = &obj.key
                    && matcher.is_match(key)
                {
                    matching_objects.push(obj);
                }
            }
            tx.send(
                matching_objects
                    .into_iter()
                    .map(|o| PrefixResult::Object(S3Object::from(o)))
                    .collect::<Vec<_>>(),
            )?;
        }
    }
    Ok(())
}

#[derive(Default)]
pub struct ScanResult {
    pub prefixes: Vec<String>,
    pub objects: Vec<Object>,
    /// True if pagination was halted before the underlying listing was
    /// exhausted. The caller should not assume `prefixes`/`objects` is
    /// the complete content under the scanned prefix.
    pub truncated: bool,
}

impl std::fmt::Debug for ScanResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScanResult")
            .field("prefixes", &self.prefixes)
            .field(
                "objects",
                &self
                    .objects
                    .iter()
                    .map(|o| o.key.as_ref().unwrap())
                    .collect::<Vec<_>>(),
            )
            .field("truncated", &self.truncated)
            .finish()
    }
}

impl ScanResult {
    pub fn len(&self) -> usize {
        self.prefixes.len() + self.objects.len()
    }

    pub fn for_prefix(prefix: String) -> Self {
        Self {
            prefixes: vec![prefix],
            objects: vec![],
            truncated: false,
        }
    }
}

#[async_trait::async_trait]
impl Engine for S3Engine {
    async fn scan_prefixes(
        &mut self,
        prefix: &str,
        delimiter: &str,
        max_prefixes: Option<usize>,
    ) -> Result<ScanResult> {
        trace!(prefix, ?max_prefixes, "scanning for prefixes within");
        let mut result = ScanResult::default();
        let mut paginator = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(prefix)
            .delimiter(delimiter)
            .into_paginator()
            .send();

        let mut warning_count = 0;
        let mut warning_inc = 50_000;
        let mut pages_seen = 0usize;
        while let Some(page) = paginator.next().await {
            let page = page?;
            pages_seen += 1;
            let page_is_truncated = page.is_truncated.unwrap_or(false);
            if result.len() >= warning_count + warning_inc {
                if warning_count == 0 {
                    progressln!(); // create a new line after the "discovering.." message
                }
                warn!(
                    "found {} objects and {} prefixes in {prefix} and still discovering more",
                    result.objects.len().to_formatted_string(&Locale::en),
                    result.prefixes.len().to_formatted_string(&Locale::en),
                );
                warning_count += warning_inc;
                if warning_count >= 100_000 {
                    warning_inc = 100_000;
                }
            }
            if let Some(common_prefixes) = page.common_prefixes {
                result
                    .prefixes
                    .extend(common_prefixes.into_iter().filter_map(|p| p.prefix));
            }
            if let Some(contents) = page.contents {
                result.objects.extend(contents);
            }
            if let Some(max) = max_prefixes {
                if result.prefixes.len() >= max {
                    result.prefixes.truncate(max);
                    result.truncated = true;
                    break;
                }
                // Bail out of flat-dense parents: if we've paged through
                // enough entries to plausibly surface `max` prefixes and
                // none have appeared, further pagination won't help.
                // Only fire when S3 still has more pages — if `page` was
                // the last page, this is the complete listing and the
                // caller can use `result.objects` directly.
                let plausible_pages = max.div_ceil(1000);
                if page_is_truncated && result.prefixes.is_empty() && pages_seen >= plausible_pages
                {
                    result.truncated = true;
                    break;
                }
            }
        }
        Ok(result)
    }

    async fn probe_prefix(&mut self, prefix: &str, max_keys: i32) -> Result<ScanResult> {
        trace!(prefix, max_keys, "probing prefix for direct content");
        let response = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(prefix)
            .max_keys(max_keys)
            .send()
            .await?;
        Ok(ScanResult {
            prefixes: Vec::new(),
            objects: response.contents.unwrap_or_default(),
            truncated: response.is_truncated.unwrap_or(false),
        })
    }

    // TODO: convert this to take &mut prefixes so that we don't have to
    // reallocate the vector on each call
    async fn check_prefixes<P>(
        &mut self,
        prefixes: P,
        max_parallelism: usize,
    ) -> Result<BTreeSet<String>>
    where
        P: IntoIterator<Item = String> + Send + Sync + 'static,
        P::IntoIter: Send + Sync + 'static,
    {
        debug!("checking prefixes");
        let prefixes = prefixes.into_iter();
        let (tx, mut rx) = tokio::sync::mpsc::channel(prefixes.size_hint().0);

        let permit = Arc::new(tokio::sync::Semaphore::new(max_parallelism));

        for prefix in prefixes {
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            let tx = tx.clone();
            let prefix = prefix.clone();
            let permit = permit.clone().acquire_owned().await;

            tokio::spawn(async move {
                let result = client
                    .list_objects_v2()
                    .bucket(bucket)
                    .prefix(prefix.clone())
                    .max_keys(1)
                    .send()
                    .await;
                drop(permit);

                match result {
                    Ok(response) => {
                        if response.key_count.unwrap_or(0) > 0 {
                            let _ = tx.send(Ok(prefix)).await;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                    }
                }
            });
        }

        drop(tx);

        let mut new_prefixes = BTreeSet::new();
        while let Some(result) = rx.recv().await {
            new_prefixes.insert(result.context("checking prefix exists")?);
        }
        debug!(valid_prefix_count = new_prefixes.len(), "checked prefixes");

        Ok(new_prefixes)
    }

    async fn get_exact(
        &self,
        presult: PrefixSearchResult,
        delimiter: char,
        status: &LiveStatus,
        matcher: &regex::Regex,
        tx: &UnboundedSender<Vec<PrefixResult>>,
        permit: Arc<Semaphore>,
    ) -> Result<()> {
        for prefix in &presult.prefixes {
            if prefix.is_empty() {
                continue;
            }
            // just get the object info for each prefix
            let permit = permit.clone().acquire_owned().await;
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            let prefix = prefix.clone();
            let tx = tx.clone();

            status.total_objects.fetch_add(1, Ordering::Relaxed);
            tokio::spawn(async move {
                // Prefixes that already end with the delimiter came from
                // Engine::scan_prefixes and are verified directories.
                if prefix.ends_with(delimiter) {
                    drop(permit);
                    let _ = tx.send(vec![PrefixResult::Prefix(prefix)]);
                    return;
                }

                // Check whether the prefix is an exact key, and whether it's
                // also a directory.
                // simple_append's loose verification can produce phantom
                // prefixes which are neither.
                let head = client
                    .head_object()
                    .bucket(bucket.clone())
                    .key(prefix.clone())
                    .send()
                    .await;
                let directory_form = format!("{prefix}{delimiter}");
                let dir_check = client
                    .list_objects_v2()
                    .bucket(bucket)
                    .prefix(directory_form.clone())
                    .max_keys(1)
                    .send()
                    .await;
                drop(permit);

                let mut out: Vec<PrefixResult> = Vec::new();
                if let Ok(o) = head {
                    trace!(prefix, "prefix is actually an object");
                    out.push(PrefixResult::Object(S3Object::from_head_object(
                        prefix.clone(),
                        o,
                    )));
                }
                if let Ok(resp) = dir_check
                    && resp.key_count.unwrap_or(0) > 0
                {
                    out.push(PrefixResult::Prefix(directory_form));
                }
                if !out.is_empty() {
                    let _ = tx.send(out);
                }
            });
        }
        debug!(
            total_prefixes = presult.prefixes.len(),
            "filtered prefixes from the result set"
        );
        tx.send(
            presult
                .objects
                .into_iter()
                .filter(|o| matcher.is_match(o.key.as_ref().unwrap()))
                .map(|o| PrefixResult::Object(S3Object::from(o)))
                .collect(),
        )?;
        Ok(())
    }

    async fn get_all_children(
        &self,
        presult: PrefixSearchResult,
        matcher: Arc<regex::Regex>,
        status: &LiveStatus,
        tx: &UnboundedSender<Vec<PrefixResult>>,
        permit: Arc<Semaphore>,
    ) -> Result<()> {
        for prefix in presult.prefixes {
            let client = self.client.clone();
            let total_objects = Arc::clone(&status.total_objects);
            let seen_prefixes = Arc::clone(&status.seen_prefixes);
            let matcher = matcher.clone();
            let bucket = self.bucket.clone();
            let tx = tx.clone();
            let permit = permit.clone().acquire_owned().await;

            tokio::spawn(async move {
                list_matching_objects(client, bucket, prefix.clone(), matcher, total_objects, tx)
                    .await?;
                drop(permit);

                add_atomic(&seen_prefixes, 1);
                Ok::<_, anyhow::Error>(())
            });
        }
        tx.send(
            presult
                .objects
                .into_iter()
                .filter(|o| matcher.is_match(o.key.as_ref().unwrap()))
                .map(|o| PrefixResult::Object(S3Object::from(o)))
                .collect(),
        )?;
        Ok(())
    }
}

/// A test engine that simulates a real S3 bucket with a set of paths
#[cfg(test)]
#[derive(Debug, Clone)]
pub(super) struct MockS3Engine {
    pub paths: Arc<Vec<String>>,
    pub calls: Arc<Mutex<Vec<(String, String)>>>, // (prefix, delimiter) pairs
    pub probe_calls: Arc<Mutex<Vec<(String, i32)>>>, // (prefix, max_keys) pairs
    /// Prefixes for which `scan_prefixes` should simulate the real
    /// `S3Engine`'s page-budget guard firing — i.e. return
    /// `truncated=true` with no sub-prefixes, as if the engine had
    /// paged through enough flat content to give up.
    pub force_truncate_prefixes: Arc<BTreeSet<String>>,
}

#[cfg(test)]
#[async_trait::async_trait]
impl Engine for MockS3Engine {
    async fn scan_prefixes(
        &mut self,
        prefix: &str,
        delimiter: &str,
        max_prefixes: Option<usize>,
    ) -> Result<ScanResult> {
        self.calls
            .lock()
            .unwrap()
            .push((prefix.to_string(), delimiter.to_string()));
        if self.force_truncate_prefixes.contains(prefix) {
            // Simulate the real engine's flat-dense page-budget guard:
            // empty sub-prefix list with `truncated=true`.
            let result = ScanResult {
                prefixes: Vec::new(),
                objects: Vec::new(),
                truncated: true,
            };
            info!(prefix, ?result, "MockS3 forced truncated scan");
            return Ok(result);
        }
        let mut found = self.scan_prefixes_inner(prefix, delimiter)?;
        if let Some(max) = max_prefixes
            && found.prefixes.len() > max
        {
            found.prefixes.truncate(max);
            found.truncated = true;
        }

        info!(prefix, ?found, "MockS3 found prefixes");

        Ok(found)
    }

    async fn probe_prefix(&mut self, prefix: &str, max_keys: i32) -> Result<ScanResult> {
        self.probe_calls
            .lock()
            .unwrap()
            .push((prefix.to_string(), max_keys));
        let max = max_keys as usize;
        let mut matched: Vec<&String> = self
            .paths
            .iter()
            .filter(|p| p.starts_with(prefix))
            .collect();
        let truncated = matched.len() > max;
        matched.truncate(max);
        let objects = matched
            .into_iter()
            .map(|k| Object::builder().key(k).build())
            .collect();
        let result = ScanResult {
            prefixes: Vec::new(),
            objects,
            truncated,
        };
        info!(prefix, max_keys, ?result, "MockS3 probed prefix");
        Ok(result)
    }

    async fn check_prefixes<P>(
        &mut self,
        prefixes: P,
        _max_parallelism: usize,
    ) -> Result<BTreeSet<String>>
    where
        P: IntoIterator<Item = String> + Send + Sync + 'static,
        P::IntoIter: Send + Sync + 'static,
    {
        let prefixes = prefixes.into_iter().collect::<Vec<_>>();
        info!(?prefixes, "MockS3 checking prefixes");
        let mut valid_prefixes = BTreeSet::new();

        // a prefix is "valid" if any key in the bucket starts with it.
        // Independent of delimiter.
        for prefix in &prefixes {
            if self.paths.iter().any(|k| k.starts_with(prefix)) {
                valid_prefixes.insert(prefix.to_string());
            }
        }

        info!(requested = ?prefixes, existing = ?valid_prefixes, "mocks3 checked prefixes for existence");

        Ok(valid_prefixes)
    }

    async fn get_exact(
        &self,
        presult: PrefixSearchResult,
        delimiter: char,
        _status: &LiveStatus,
        matcher: &regex::Regex,
        tx: &UnboundedSender<Vec<PrefixResult>>,
        _permit: Arc<Semaphore>,
    ) -> Result<()> {
        for prefix in &presult.prefixes {
            if prefix.is_empty() {
                continue;
            }
            // Prefixes ending with delimiter are verified directories
            // from Engine::scan_prefixes
            if prefix.ends_with(delimiter) {
                tx.send(vec![PrefixResult::Prefix(prefix.clone())])?;
                continue;
            }
            // For non-delim-suffixed prefixes, emit Object if it's an
            // exact key, Prefix(directory_form) if any key starts with
            // prefix+delim. Both can apply (a key plus a "directory" at
            // the same name, S3 is not a filesystem).
            let mut out: Vec<PrefixResult> = Vec::new();
            if self.paths.iter().any(|k| k == prefix) {
                out.push(PrefixResult::Object(S3Object::from(
                    Object::builder().key(prefix).build(),
                )));
            }
            let directory_form = format!("{prefix}{delimiter}");
            if self.paths.iter().any(|k| k.starts_with(&directory_form)) {
                out.push(PrefixResult::Prefix(directory_form));
            }
            if !out.is_empty() {
                tx.send(out)?;
            }
        }
        tx.send(
            presult
                .objects
                .into_iter()
                .filter(|o| matcher.is_match(o.key.as_ref().unwrap()))
                .map(|o| PrefixResult::Object(S3Object::from(o)))
                .collect(),
        )?;
        Ok(())
    }

    async fn get_all_children(
        &self,
        presult: PrefixSearchResult,
        matcher: Arc<regex::Regex>,
        _status: &LiveStatus,
        tx: &UnboundedSender<Vec<PrefixResult>>,
        _permit: Arc<Semaphore>,
    ) -> Result<()> {
        for prefix in &presult.prefixes {
            let matching: Vec<PrefixResult> = self
                .paths
                .iter()
                .filter(|k| k.starts_with(prefix.as_str()) && matcher.is_match(k))
                .map(|k| PrefixResult::Object(S3Object::from(Object::builder().key(k).build())))
                .collect();
            tx.send(matching)?;
        }
        tx.send(
            presult
                .objects
                .into_iter()
                .filter(|o| matcher.is_match(o.key.as_ref().unwrap()))
                .map(|o| PrefixResult::Object(S3Object::from(o)))
                .collect(),
        )?;
        Ok(())
    }
}

#[cfg(test)]
impl MockS3Engine {
    pub fn new(paths: Vec<String>) -> Self {
        Self {
            paths: Arc::new(paths),
            calls: Arc::new(Mutex::new(Vec::new())),
            probe_calls: Arc::new(Mutex::new(Vec::new())),
            force_truncate_prefixes: Arc::new(BTreeSet::new()),
        }
    }

    /// Make `scan_prefixes` return `truncated=true` with no sub-prefixes
    /// for any of `prefixes`, simulating the real `S3Engine`'s
    /// flat-dense page-budget early exit.
    pub fn with_forced_truncation<I>(mut self, prefixes: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<String>,
    {
        self.force_truncate_prefixes = Arc::new(prefixes.into_iter().map(Into::into).collect());
        self
    }

    /// Assert the unordered set of (prefix, delimiter) calls equals
    /// `expected`. Use when calls happen in parallel within a round and
    /// ordering is not part of the contract.
    pub fn assert_call_set(&self, expected: &[(impl AsRef<str>, impl AsRef<str>)]) {
        let mut actual: Vec<(String, String)> = self.calls.lock().unwrap().clone();
        actual.sort();
        let mut expected_sorted: Vec<(String, String)> = expected
            .iter()
            .map(|(p, d)| (p.as_ref().to_string(), d.as_ref().to_string()))
            .collect();
        expected_sorted.sort();
        assert!(
            actual == expected_sorted,
            "call set mismatch:\n  got:      {actual:?}\n  expected: {expected_sorted:?}",
        );
    }

    pub fn assert_calls(&self, expected: &[(impl AsRef<str>, impl AsRef<str>)]) {
        info!("MockS3 asserting calls");
        let calls = &self.calls.lock().unwrap();
        for (i, ((actual_prefix, actual_delim), (expected_prefix, expected_delim))) in
            calls.iter().zip(expected).enumerate()
        {
            info!("MockS3 asserting call {i}");
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
            calls.len() == expected.len(),
            "Got {} calls, expected {}. Actual calls: {:?}",
            calls.len(),
            expected.len(),
            calls
        );
    }

    pub fn scan_prefixes_inner(&mut self, prefix: &str, delimiter: &str) -> Result<ScanResult> {
        let mut objects = Vec::new();
        // Real S3 returns each `CommonPrefix` once; multiple keys
        // sharing a parent dir don't multiply the listing.
        let mut prefix_set: BTreeSet<String> = BTreeSet::new();
        self.paths
            .iter()
            .filter(|p| p.starts_with(prefix))
            .for_each(|p| {
                let matched_prefix = if let Some(end) = p[prefix.len()..].find(delimiter) {
                    // only return the prefix up to the delimiter
                    p[..prefix.len() + end + 1].to_string()
                } else {
                    p.to_string()
                };
                if matched_prefix.len() == p.len() {
                    objects.push(Object::builder().key(matched_prefix).build());
                } else {
                    prefix_set.insert(matched_prefix);
                }
            });

        Ok(ScanResult {
            prefixes: prefix_set.into_iter().collect(),
            objects,
            truncated: false,
        })
    }
}
