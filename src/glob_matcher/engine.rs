use std::collections::BTreeSet;

use anyhow::{Context as _, Result};
use aws_sdk_s3::Client;
use num_format::{Locale, ToFormattedString as _};
use tracing::{debug, trace, warn};

#[cfg(test)]
use std::sync::{Arc, Mutex};
#[cfg(test)]
use tracing::info;

use crate::progressln;

#[async_trait::async_trait]
pub trait Engine: Send + Sync + 'static {
    async fn scan_prefixes(&mut self, prefix: &str, delimiter: &str) -> Result<Vec<String>>;
    async fn check_prefixes<P>(&mut self, prefixes: P) -> Result<BTreeSet<String>>
    where
        P: IntoIterator<Item = String> + Send + Sync + 'static,
        P::IntoIter: Send + Sync + 'static;
}

#[derive(Debug, Clone)]
pub struct S3Engine {
    client: Client,
    bucket: String,
    delimiter: String,
}

impl S3Engine {
    pub fn new(client: Client, bucket: String, delimiter: String) -> Self {
        Self {
            client,
            bucket,
            delimiter,
        }
    }
}

#[async_trait::async_trait]
impl Engine for S3Engine {
    async fn scan_prefixes(&mut self, prefix: &str, delimiter: &str) -> Result<Vec<String>> {
        trace!(prefix, "scanning for prefixes within");
        let mut prefixes = Vec::new();
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
            if prefixes.len() >= warning_count + warning_inc {
                if warning_count == 0 {
                    progressln!(); // create a new line after the "discovering.." message
                }
                warn!(
                    "found {} objects in {prefix} and still discovering more",
                    prefixes.len().to_formatted_string(&Locale::en)
                );
                warning_count += warning_inc;
                if warning_count >= 100_000 {
                    warning_inc = 100_000;
                }
            }
            if let Some(common_prefixes) = page.common_prefixes {
                prefixes.extend(common_prefixes.into_iter().filter_map(|p| p.prefix));
            }
            if let Some(contents) = page.contents {
                prefixes.extend(contents.into_iter().filter_map(|c| c.key));
            }
        }
        Ok(prefixes)
    }

    // TODO: convert this to take &mut prefixes so that we don't have to
    // reallocate the vector on each call
    async fn check_prefixes<P>(&mut self, prefixes: P) -> Result<BTreeSet<String>>
    where
        P: IntoIterator<Item = String> + Send + Sync + 'static,
        P::IntoIter: Send + Sync + 'static,
    {
        debug!("checking prefixes");
        let prefixes = prefixes.into_iter();
        let (tx, mut rx) = tokio::sync::mpsc::channel(prefixes.size_hint().0);

        for prefix in prefixes {
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            let tx = tx.clone();
            let prefix = prefix.clone();

            tokio::spawn(async move {
                let result = client
                    .list_objects_v2()
                    .bucket(bucket)
                    .prefix(prefix.clone())
                    .max_keys(1)
                    .send()
                    .await;

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
    async fn scan_prefixes(&mut self, prefix: &str, delimiter: &str) -> Result<Vec<String>> {
        self.calls
            .lock()
            .unwrap()
            .push((prefix.to_string(), delimiter.to_string()));
        let found = self.scan_prefixes_inner(prefix, delimiter);

        info!(prefix, ?found, "MockS3 found prefixes");

        found
    }

    async fn check_prefixes<P>(&mut self, prefixes: P) -> Result<BTreeSet<String>>
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

            if !response.is_empty() {
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

    pub fn scan_prefixes_inner(&mut self, prefix: &str, delimiter: &str) -> Result<Vec<String>> {
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
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();

        Ok(result)
    }
}
