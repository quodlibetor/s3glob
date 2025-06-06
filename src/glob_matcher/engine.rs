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
    async fn scan_prefixes(&mut self, prefix: &str, delimiter: &str) -> Result<ScanResult>;
    async fn check_prefixes<P>(
        &mut self,
        prefixes: P,
        max_parallelism: usize,
    ) -> Result<BTreeSet<String>>
    where
        P: IntoIterator<Item = String> + Send + Sync + 'static,
        P::IntoIter: Send + Sync + 'static;
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

    pub(crate) async fn get_all_children(
        &self,
        presult: PrefixSearchResult,
        matcher: Arc<regex::Regex>,
        status: &LiveStatus,
        tx: &tokio::sync::mpsc::UnboundedSender<Vec<PrefixResult>>,
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

    pub(crate) async fn get_exact(
        &self,
        presult: PrefixSearchResult,
        status: &LiveStatus,
        matcher: &regex::Regex,
        tx: &tokio::sync::mpsc::UnboundedSender<Vec<PrefixResult>>,
        permit: Arc<Semaphore>,
    ) -> Result<()> {
        for prefix in &presult.prefixes {
            // just get the object info for each prefix
            let permit = permit.clone().acquire_owned().await;
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            let prefix = prefix.clone();
            let tx = tx.clone();

            status.total_objects.fetch_add(1, Ordering::Relaxed);
            tokio::spawn(async move {
                // Check if the "prefix" is a real object
                let r = client
                    .head_object()
                    .bucket(bucket)
                    .key(prefix.clone())
                    .send()
                    .await;
                drop(permit);

                match r {
                    Ok(o) => {
                        trace!(prefix, "prefix is actually an object");

                        tx.send(vec![PrefixResult::Object(S3Object::from_head_object(
                            prefix, o,
                        ))])
                    }
                    Err(_) => tx.send(vec![PrefixResult::Prefix(prefix)]),
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
                if let Some(key) = &obj.key {
                    if matcher.is_match(key) {
                        matching_objects.push(obj);
                    }
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
        }
    }
}

#[async_trait::async_trait]
impl Engine for S3Engine {
    async fn scan_prefixes(&mut self, prefix: &str, delimiter: &str) -> Result<ScanResult> {
        trace!(prefix, "scanning for prefixes within");
        let mut result = ScanResult {
            prefixes: Vec::new(),
            objects: Vec::new(),
        };
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
        while let Some(page) = paginator.next().await {
            let page = page?;
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
        }
        Ok(result)
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
}

/// A test engine that simulates a real S3 bucket with a set of paths
#[cfg(test)]
#[derive(Debug, Clone)]
pub(super) struct MockS3Engine {
    pub paths: Arc<Vec<String>>,
    pub calls: Arc<Mutex<Vec<(String, String)>>>, // (prefix, delimiter) pairs
}

#[cfg(test)]
#[async_trait::async_trait]
impl Engine for MockS3Engine {
    async fn scan_prefixes(&mut self, prefix: &str, delimiter: &str) -> Result<ScanResult> {
        self.calls
            .lock()
            .unwrap()
            .push((prefix.to_string(), delimiter.to_string()));
        let found = self.scan_prefixes_inner(prefix, delimiter);

        info!(prefix, ?found, "MockS3 found prefixes");

        Ok(found?)
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

        for prefix in &prefixes {
            // Use ListObjectsV2 with max-keys=1 to efficiently check existence
            let response = self.scan_prefixes_inner(prefix, "/")?;

            if !response.prefixes.is_empty() {
                valid_prefixes.insert(prefix.to_string());
            } else if !response.objects.is_empty() {
                valid_prefixes.insert(prefix.to_string());
            }
        }

        info!(requested = ?prefixes, existing = ?valid_prefixes, "mocks3 checked prefixes for existence");

        Ok(valid_prefixes)
    }
}

#[cfg(test)]
impl MockS3Engine {
    pub fn new(paths: Vec<String>) -> Self {
        Self {
            paths: Arc::new(paths),
            calls: Arc::new(Mutex::new(Vec::new())),
        }
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
        let mut result = ScanResult::default();
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
                    result
                        .objects
                        .push(Object::builder().key(matched_prefix).build());
                } else {
                    result.prefixes.push(matched_prefix);
                }
            });

        Ok(result)
    }
}
