//! The S3 bucket s3glob works on.
//!
//! [`Bucket`] is the only holder of the S3 [`Client`] after startup. Its
//! methods route every request through the shared [`Retrier`]; the SDK's
//! own retries are disabled (see `build_s3_client`), so a request sent any
//! other way would get no retries at all. Add new S3 operations here.

use anyhow::{Context as _, Result, anyhow};
use aws_sdk_s3::Client;
use aws_sdk_s3::operation::get_object::{GetObjectError, GetObjectOutput};
use aws_sdk_s3::operation::head_object::{HeadObjectError, HeadObjectOutput};
use aws_sdk_s3::operation::list_objects_v2::{ListObjectsV2Error, ListObjectsV2Output};

use crate::retry::{Retrier, RetryError, Slot};

#[derive(Debug, Clone)]
pub(crate) struct Bucket {
    client: Client,
    retrier: Retrier,
    name: String,
}

impl Bucket {
    pub(crate) fn new(client: Client, retrier: Retrier, name: String) -> Self {
        Self {
            client,
            retrier,
            name,
        }
    }

    /// `s3://<bucket>/<key>`, for messages.
    pub(crate) fn uri(&self, key: &str) -> String {
        format!("s3://{}/{key}", self.name)
    }

    /// One ListObjectsV2 call: the first page under `prefix`.
    pub(crate) async fn list_first_page(
        &self,
        prefix: &str,
        delimiter: Option<&str>,
        max_keys: Option<i32>,
    ) -> Result<ListObjectsV2Output, RetryError<ListObjectsV2Error>> {
        self.list_page(prefix, delimiter, max_keys, None).await
    }

    /// Every page under `prefix`, each retried on its own so a throttled
    /// page doesn't restart the listing.
    pub(crate) fn list_pages(&self, prefix: &str, delimiter: Option<&str>) -> Pages {
        Pages {
            bucket: self.clone(),
            prefix: prefix.to_owned(),
            delimiter: delimiter.map(str::to_owned),
            cursor: Cursor::More(None),
        }
    }

    pub(crate) async fn head_object(
        &self,
        key: &str,
    ) -> Result<HeadObjectOutput, RetryError<HeadObjectError>> {
        self.retrier
            .send(|| self.client.head_object().bucket(&self.name).key(key).send())
            .await
    }

    /// Start downloading `key`. The returned [`Slot`] counts against the
    /// concurrency limit; hold it until the body has been read.
    pub(crate) async fn get_object(
        &self,
        key: &str,
    ) -> Result<(GetObjectOutput, Slot), RetryError<GetObjectError>> {
        self.retrier
            .send_holding(|| self.client.get_object().bucket(&self.name).key(key).send())
            .await
    }

    async fn list_page(
        &self,
        prefix: &str,
        delimiter: Option<&str>,
        max_keys: Option<i32>,
        continuation_token: Option<String>,
    ) -> Result<ListObjectsV2Output, RetryError<ListObjectsV2Error>> {
        self.retrier
            .send(|| {
                self.client
                    .list_objects_v2()
                    .bucket(&self.name)
                    .prefix(prefix)
                    .set_delimiter(delimiter.map(str::to_owned))
                    .set_max_keys(max_keys)
                    .set_continuation_token(continuation_token.clone())
                    .send()
            })
            .await
    }
}

/// A ListObjectsV2 listing, fetched one page at a time.
pub(crate) struct Pages {
    bucket: Bucket,
    prefix: String,
    delimiter: Option<String>,
    cursor: Cursor,
}

enum Cursor {
    /// More to fetch, starting from this continuation token.
    More(Option<String>),
    /// S3 claimed there was more but gave no usable token.
    Stuck,
    Done,
}

impl Pages {
    /// The next page, or `None` once the listing is complete.
    ///
    /// Like the SDK paginator this follows `NextContinuationToken`, but
    /// where the paginator quietly stops on a repeated token, or on
    /// `IsTruncated` without a token, this returns an error after the last
    /// good page: either would otherwise cut the listing short unnoticed.
    pub(crate) async fn next(&mut self) -> Option<Result<ListObjectsV2Output>> {
        let token = match std::mem::replace(&mut self.cursor, Cursor::Done) {
            Cursor::Done => return None,
            Cursor::Stuck => {
                return Some(Err(anyhow!(
                    "listing {}: S3 said there were more results but gave no new \
                     continuation token",
                    self.bucket.uri(&self.prefix)
                )));
            }
            Cursor::More(token) => token,
        };
        let page = match self
            .bucket
            .list_page(&self.prefix, self.delimiter.as_deref(), None, token.clone())
            .await
        {
            Ok(page) => page,
            Err(e) => {
                return Some(
                    Err(e).with_context(|| format!("listing {}", self.bucket.uri(&self.prefix))),
                );
            }
        };
        let next = page
            .next_continuation_token
            .clone()
            .filter(|t| !t.is_empty());
        self.cursor = match next {
            None if page.is_truncated == Some(true) => Cursor::Stuck,
            None => Cursor::Done,
            Some(next) if Some(&next) == token.as_ref() => Cursor::Stuck,
            Some(next) => Cursor::More(Some(next)),
        };
        Some(Ok(page))
    }

    /// True if [`Self::next`] may return more pages.
    pub(crate) fn has_more(&self) -> bool {
        !matches!(self.cursor, Cursor::Done)
    }
}
