use anyhow::{Context as _, Result};
use aws_sdk_s3::Client;
use tracing::{debug, trace};

#[cfg(test)]
use std::collections::BTreeSet;
#[cfg(test)]
use tracing::info;

#[async_trait::async_trait]
pub trait Engine {
    async fn scan_prefixes(&mut self, prefix: &str, delimiter: &str) -> Result<Vec<String>>;
    async fn check_prefixes(&mut self, prefixes: &[String]) -> Result<Vec<String>>;
}

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

        while let Some(page) = paginator.next().await {
            let page = page?;
            if let Some(common_prefixes) = page.common_prefixes {
                prefixes.extend(common_prefixes.into_iter().filter_map(|p| p.prefix));
            }
        }
        Ok(prefixes)
    }

    // TODO: convert this to take &mut prefixes so that we don't have to
    // reallocate the vector on each call
    async fn check_prefixes(&mut self, prefixes: &[String]) -> Result<Vec<String>> {
        debug!(prefix_count = prefixes.len(), "checking prefixes");
        let (tx, mut rx) = tokio::sync::mpsc::channel(prefixes.len());

        for prefix in prefixes {
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            let delimiter = self.delimiter.clone();
            let tx = tx.clone();
            let prefix = prefix.clone();

            tokio::spawn(async move {
                let result = client
                    .list_objects_v2()
                    .bucket(bucket)
                    .prefix(prefix.clone())
                    .delimiter(delimiter)
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

        let mut new_prefixes = Vec::new();
        while let Some(result) = rx.recv().await {
            new_prefixes.push(result.context("checking prefix exists")?);
        }
        debug!(valid_prefix_count = new_prefixes.len(), "checked prefixes");

        Ok(new_prefixes)
    }
}

/// A test engine that simulates a real S3 bucket with a set of paths
#[cfg(test)]
pub(super) struct MockS3Engine {
    pub paths: Vec<String>,
    pub calls: Vec<(String, String)>, // (prefix, delimiter) pairs
}

#[cfg(test)]
#[async_trait::async_trait]
impl Engine for MockS3Engine {
    async fn scan_prefixes(&mut self, prefix: &str, delimiter: &str) -> Result<Vec<String>> {
        self.calls.push((prefix.to_string(), delimiter.to_string()));
        let found = self.scan_prefixes_inner(prefix, delimiter);

        info!(prefix, ?found, "mocks3 found prefixes");

        found
    }

    async fn check_prefixes(&mut self, prefixes: &[String]) -> Result<Vec<String>> {
        let mut valid_prefixes = Vec::new();

        for prefix in prefixes {
            // Use ListObjectsV2 with max-keys=1 to efficiently check existence
            let response = self.scan_prefixes_inner(prefix, "/")?;

            if !response.is_empty() {
                valid_prefixes.push(prefix.clone());
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
            paths,
            calls: Vec::new(),
        }
    }

    pub fn assert_calls(&self, expected: &[(impl AsRef<str>, impl AsRef<str>)]) {
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
