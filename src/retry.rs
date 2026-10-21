//! Retrying S3 requests, and backing off when S3 asks us to slow down.
//!
//! Every S3 call goes through a [`Retrier`] (normally via
//! [`crate::bucket::Bucket`], which is the only owner of the S3 client).
//! It follows the AWS SDKs' retry rules (error classification and
//! full-jitter exponential backoff, see
//! <https://docs.aws.amazon.com/sdkref/latest/guide/feature-retry-behavior.html>)
//! with two additions:
//!
//! - Concurrency adapts to throttling. All requests share one limit that
//!   starts at `--max-parallelism`. When S3 throttles a request the limit
//!   drops to half the number of requests then in flight, and it grows by
//!   one after a full limit's worth of successes (AIMD, as in TCP
//!   congestion control).
//! - A server-requested delay (`x-amz-retry-after`) is waited out in full,
//!   up to [`MAX_SERVER_DELAY`]. The SDKs clamp it to their own backoff
//!   window; S3 knows better than we do when it will have capacity again.
//!
//! The SDK's own retries are disabled (see `build_s3_client`) so that
//! throttling is visible here rather than absorbed inside the SDK.

use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aws_runtime::retries::classifiers::{THROTTLING_ERRORS, TRANSIENT_ERRORS};
use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::debug;

use crate::progress;

/// Backoff bases from the AWS retry spec.
const THROTTLING_BASE: Duration = Duration::from_millis(1000);
const TRANSIENT_BASE: Duration = Duration::from_millis(50);

/// Longest server-requested wait that is honoured as-is. Longer requests
/// are most likely a misbehaving proxy, and would look like a hang.
pub(crate) const MAX_SERVER_DELAY: Duration = Duration::from_secs(5 * 60);

/// Minimum gap between "S3 asked us to wait" notices, so hundreds of
/// concurrently throttled requests print one line rather than hundreds.
const NOTICE_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Class {
    Throttling { retry_after: Option<Duration> },
    Transient,
    Fatal,
}

fn classify<E: ProvideErrorMetadata>(err: &SdkError<E, HttpResponse>) -> Class {
    match err {
        SdkError::ConstructionFailure(_) => Class::Fatal,
        SdkError::TimeoutError(_) | SdkError::ResponseError(_) => Class::Transient,
        SdkError::DispatchFailure(d) if d.is_io() || d.is_timeout() => Class::Transient,
        SdkError::DispatchFailure(_) => Class::Fatal,
        SdkError::ServiceError(_) => {
            let raw = err.raw_response();
            let status = raw.map(|r| r.status().as_u16());
            let code = err.code();
            // S3 uses 503 as its slow-down signal whatever the code (`SlowDown`,
            // `ServiceUnavailable`, or none at all on a HEAD, whose response
            // has no body), so every 503 is throttling rather than AWS's
            // generic "transient 5xx".
            if code.is_some_and(|c| THROTTLING_ERRORS.contains(&c)) || status == Some(503) {
                let retry_after = raw
                    .and_then(|r| r.headers().get("x-amz-retry-after"))
                    .and_then(|v| v.parse::<u64>().ok())
                    .map(Duration::from_millis);
                Class::Throttling { retry_after }
            } else if code.is_some_and(|c| TRANSIENT_ERRORS.contains(&c))
                || matches!(status, Some(500 | 502 | 504))
            {
                Class::Transient
            } else {
                Class::Fatal
            }
        }
        _ => Class::Fatal,
    }
}

/// Full-jitter exponential backoff: `random(0, 1) * min(cap, base * 2^retry)`.
fn backoff(base: Duration, retry: u32, cap: Duration) -> Duration {
    let ceiling = base
        .checked_mul(2u32.saturating_pow(retry))
        .unwrap_or(cap)
        .min(cap);
    ceiling.mul_f64(fastrand::f64())
}

/// An S3 request that failed for good.
#[derive(Debug)]
pub(crate) struct RetryError<E> {
    retries: u32,
    /// Set when the error was retryable and retries ran out.
    exhausted: Option<Class>,
    source: Box<SdkError<E, HttpResponse>>,
}

impl<E> RetryError<E> {
    /// The underlying SDK error. It is deliberately not exposed through
    /// `Error::source` (see below), so inspect it here.
    pub(crate) fn sdk_error(&self) -> &SdkError<E, HttpResponse> {
        &self.source
    }
}

impl<E: std::error::Error + 'static> fmt::Display for RetryError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", DisplayErrorContext(&*self.source))?;
        match self.exhausted {
            Some(Class::Throttling { .. }) => {
                write!(f, " (still throttled after {} retries)", self.retries)
            }
            Some(_) => write!(f, " (still failing after {} retries)", self.retries),
            None => Ok(()),
        }
    }
}

// The source chain is already in Display via DisplayErrorContext, so
// `source()` is left as None to keep `{:#}` from repeating it.
impl<E: std::error::Error + 'static> std::error::Error for RetryError<E> {}

/// One slot of the shared concurrency limit, returned when dropped.
pub(crate) struct Slot {
    retrier: Retrier,
    permit: Option<OwnedSemaphorePermit>,
}

impl Drop for Slot {
    fn drop(&mut self) {
        if let Some(permit) = self.permit.take() {
            self.retrier.release(permit);
        }
    }
}

#[derive(Clone)]
pub(crate) struct Retrier {
    inner: Arc<Inner>,
}

struct Inner {
    max_retries: u32,
    max_backoff: Duration,
    /// The concurrency limit never grows past this (`--max-parallelism`).
    ceiling: usize,
    permits: Arc<Semaphore>,
    state: Mutex<State>,
}

struct State {
    limit: usize,
    /// Slots currently held.
    in_flight: usize,
    /// Permits to discard as they are returned, after a decrease that
    /// found them checked out.
    excess: usize,
    /// Bumped on every decrease. A throttle only lowers the limit if its
    /// request was issued under the current generation, so a burst of
    /// requests throttled together lowers the limit once, not once each.
    generation: u64,
    successes_since_increase: usize,
    last_notice: Option<Instant>,
    throttled_requests: usize,
    throttled_responses: usize,
    lowest_limit: usize,
}

impl fmt::Debug for Retrier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Retrier")
            .field("max_retries", &self.inner.max_retries)
            .field("max_backoff", &self.inner.max_backoff)
            .field("ceiling", &self.inner.ceiling)
            .finish_non_exhaustive()
    }
}

impl Retrier {
    pub(crate) fn new(max_parallelism: usize, max_retries: u32, max_backoff: Duration) -> Self {
        let ceiling = max_parallelism.max(1);
        Self {
            inner: Arc::new(Inner {
                max_retries,
                max_backoff,
                ceiling,
                permits: Arc::new(Semaphore::new(ceiling)),
                state: Mutex::new(State {
                    limit: ceiling,
                    in_flight: 0,
                    excess: 0,
                    generation: 0,
                    successes_since_increase: 0,
                    last_notice: None,
                    throttled_requests: 0,
                    throttled_responses: 0,
                    lowest_limit: ceiling,
                }),
            }),
        }
    }

    /// Run `op` (one S3 request) until it succeeds, fails with an error
    /// that is not worth retrying, or runs out of retries.
    ///
    /// `op` is called once per attempt; each attempt holds one slot of the
    /// shared concurrency limit, which is released while backing off.
    pub(crate) async fn send<T, E, F, Fut>(&self, op: F) -> Result<T, RetryError<E>>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, SdkError<E, HttpResponse>>>,
        E: ProvideErrorMetadata,
    {
        self.send_holding(op).await.map(|(v, _slot)| v)
    }

    /// Like [`Self::send`], but the successful attempt's slot is handed to
    /// the caller, for responses whose body is still to be streamed: the
    /// slot should count against the limit until the body is read.
    pub(crate) async fn send_holding<T, E, F, Fut>(
        &self,
        mut op: F,
    ) -> Result<(T, Slot), RetryError<E>>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, SdkError<E, HttpResponse>>>,
        E: ProvideErrorMetadata,
    {
        let mut retries = 0;
        let mut throttled = false;
        loop {
            // Read before waiting for a slot: a decrease that lands while
            // this request is queued must not count as "before" it.
            let generation = self.inner.state.lock().unwrap().generation;
            let slot = self.acquire().await;
            let err = match op().await {
                Ok(v) => {
                    self.on_success();
                    return Ok((v, slot));
                }
                Err(err) => err,
            };
            drop(slot);
            let class = classify(&err);
            let delay = match class {
                Class::Fatal => {
                    return Err(RetryError {
                        retries,
                        exhausted: None,
                        source: Box::new(err),
                    });
                }
                Class::Throttling { retry_after } => {
                    let first = !throttled;
                    throttled = true;
                    let notice = self.on_throttle(generation, first, err.code(), retry_after);
                    if let Some(notice) = notice {
                        progress::get().error(notice);
                    }
                    retry_after
                        .map(|d| d.min(MAX_SERVER_DELAY))
                        .unwrap_or_else(|| {
                            backoff(THROTTLING_BASE, retries, self.inner.max_backoff)
                        })
                }
                Class::Transient => backoff(TRANSIENT_BASE, retries, self.inner.max_backoff),
            };
            if retries >= self.inner.max_retries {
                return Err(RetryError {
                    retries,
                    exhausted: Some(class),
                    source: Box::new(err),
                });
            }
            retries += 1;
            debug!(?class, retries, ?delay, code = ?err.code(), "retrying S3 request");
            tokio::time::sleep(delay).await;
        }
    }

    /// A one-line summary of throttling during this run, if there was any.
    pub(crate) fn summary(&self) -> Option<String> {
        let state = self.inner.state.lock().unwrap();
        let plural = |n: usize, what: &str| format!("{n} {what}{}", if n == 1 { "" } else { "s" });
        (state.throttled_requests > 0).then(|| {
            format!(
                "S3 throttled {} ({} in all); parallelism went as low as {} \
                 (--max-parallelism {})",
                plural(state.throttled_requests, "request"),
                plural(state.throttled_responses, "throttled response"),
                state.lowest_limit,
                self.inner.ceiling,
            )
        })
    }

    async fn acquire(&self) -> Slot {
        let permit = self
            .inner
            .permits
            .clone()
            .acquire_owned()
            .await
            .expect("retry semaphore is never closed");
        self.inner.state.lock().unwrap().in_flight += 1;
        Slot {
            retrier: self.clone(),
            permit: Some(permit),
        }
    }

    fn release(&self, permit: OwnedSemaphorePermit) {
        let mut state = self.inner.state.lock().unwrap();
        state.in_flight -= 1;
        if state.excess > 0 {
            state.excess -= 1;
            permit.forget();
        }
    }

    fn on_success(&self) {
        let mut state = self.inner.state.lock().unwrap();
        if state.limit >= self.inner.ceiling {
            return;
        }
        state.successes_since_increase += 1;
        if state.successes_since_increase >= state.limit {
            state.successes_since_increase = 0;
            state.limit += 1;
            if state.excess > 0 {
                state.excess -= 1;
            } else {
                self.inner.permits.add_permits(1);
            }
        }
    }

    /// Record a throttled response and maybe lower the limit. Returns the
    /// notice to print, which the caller prints after the lock is released
    /// so a slow stderr can't stall every other request.
    fn on_throttle(
        &self,
        generation: u64,
        first_for_request: bool,
        code: Option<&str>,
        retry_after: Option<Duration>,
    ) -> Option<String> {
        let mut state = self.inner.state.lock().unwrap();
        state.throttled_responses += 1;
        if first_for_request {
            state.throttled_requests += 1;
        }
        let code = code.unwrap_or("HTTP 503");
        let waiting = match retry_after {
            Some(d) if d > MAX_SERVER_DELAY => format!(
                "; S3 asked for a {d:?} wait before retrying, waiting the {MAX_SERVER_DELAY:?} maximum"
            ),
            Some(d) => format!("; S3 asked for a {d:?} wait before retrying"),
            None => String::new(),
        };
        let now = Instant::now();
        if generation == state.generation && state.limit > 1 {
            // Halve what was actually in flight (including this request,
            // whose slot is already released), not the nominal limit:
            // halving an unused limit would slow nothing down.
            let in_flight = state.in_flight + 1;
            let new = (state.limit.min(in_flight) / 2).max(1);
            let reduce = state.limit - new;
            // Permits sitting idle in the semaphore go now; the rest are
            // discarded as in-flight requests return them.
            let forgotten = self.inner.permits.forget_permits(reduce);
            state.excess += reduce - forgotten;
            state.limit = new;
            state.lowest_limit = state.lowest_limit.min(new);
            state.generation += 1;
            state.successes_since_increase = 0;
            state.last_notice = Some(now);
            Some(format!(
                "S3 is throttling requests ({code}): {in_flight} were in flight, \
                 reducing parallelism to {new}{waiting}"
            ))
        } else if retry_after.is_some()
            && state
                .last_notice
                .is_none_or(|t| now.duration_since(t) >= NOTICE_INTERVAL)
        {
            state.last_notice = Some(now);
            Some(format!("S3 is throttling requests ({code}){waiting}"))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use aws_sdk_s3::error::ErrorMetadata;
    use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
    use aws_smithy_runtime_api::http::StatusCode;
    use aws_smithy_types::body::SdkBody;

    use super::*;

    fn service_error(
        status: u16,
        code: Option<&str>,
        retry_after: Option<&str>,
    ) -> SdkError<ListObjectsV2Error, HttpResponse> {
        let mut meta = ErrorMetadata::builder();
        if let Some(code) = code {
            meta = meta.code(code);
        }
        let mut raw = HttpResponse::new(StatusCode::try_from(status).unwrap(), SdkBody::empty());
        if let Some(ms) = retry_after {
            raw.headers_mut().insert("x-amz-retry-after", ms.to_owned());
        }
        SdkError::service_error(ListObjectsV2Error::generic(meta.build()), raw)
    }

    #[test]
    fn classifies_like_the_aws_retry_spec() {
        check!(
            classify(&service_error(503, Some("SlowDown"), None))
                == Class::Throttling { retry_after: None }
        );
        check!(
            classify(&service_error(503, Some("SlowDown"), Some("5000")))
                == Class::Throttling {
                    retry_after: Some(Duration::from_secs(5))
                }
        );
        // A throttled HEAD has no body, so no code.
        check!(
            classify(&service_error(503, None, None)) == Class::Throttling { retry_after: None }
        );
        // Any 503 is S3 asking us to slow down.
        check!(
            classify(&service_error(503, Some("ServiceUnavailable"), None))
                == Class::Throttling { retry_after: None }
        );
        check!(classify(&service_error(500, Some("InternalError"), None)) == Class::Transient);
        // 5xx are retried whatever the code, as the SDK does.
        check!(classify(&service_error(500, Some("SomethingNew"), None)) == Class::Transient);
        check!(classify(&service_error(502, None, None)) == Class::Transient);
        check!(classify(&service_error(400, Some("RequestTimeout"), None)) == Class::Transient);
        check!(classify(&service_error(403, Some("AccessDenied"), None)) == Class::Fatal);
        check!(classify(&service_error(404, Some("NoSuchBucket"), None)) == Class::Fatal);
    }

    #[test]
    fn backoff_is_capped_and_jittered() {
        let cap = Duration::from_secs(20);
        for retry in 0..40 {
            let d = backoff(THROTTLING_BASE, retry, cap);
            check!(d <= cap);
            check!(d <= THROTTLING_BASE * 2u32.saturating_pow(retry));
        }
    }

    fn limit(r: &Retrier) -> usize {
        r.inner.state.lock().unwrap().limit
    }

    fn generation(r: &Retrier) -> u64 {
        r.inner.state.lock().unwrap().generation
    }

    #[tokio::test]
    async fn throttle_halves_in_flight_once_per_generation() {
        let r = Retrier::new(100, 3, Duration::from_secs(1));
        let mut slots = Vec::new();
        for _ in 0..8 {
            slots.push(r.acquire().await);
        }
        let gen0 = generation(&r);
        // Two of the eight in-flight requests come back throttled; each
        // releases its slot before reporting, as `send_holding` does.
        slots.pop();
        r.on_throttle(gen0, true, Some("SlowDown"), None);
        slots.pop();
        r.on_throttle(gen0, true, Some("SlowDown"), None);
        // Half of the eight in flight, not half of the nominal 100.
        check!(limit(&r) == 4);
        // Idle permits went immediately; the rest go as slots come back.
        check!(r.inner.permits.available_permits() == 0);
        drop(slots);
        check!(r.inner.permits.available_permits() == 4);
        // A request issued after the decrease lowers it again.
        let slot = r.acquire().await;
        let gen1 = generation(&r);
        drop(slot);
        r.on_throttle(gen1, true, Some("SlowDown"), None);
        check!(limit(&r) == 1);
        check!(r.inner.permits.available_permits() == 1);
    }

    #[tokio::test]
    async fn successes_grow_limit_back_to_ceiling() {
        let r = Retrier::new(4, 3, Duration::from_secs(1));
        r.on_throttle(generation(&r), true, Some("SlowDown"), None);
        check!(limit(&r) == 1);
        r.on_success();
        check!(limit(&r) == 2);
        for _ in 0..2 {
            r.on_success();
        }
        check!(limit(&r) == 3);
        for _ in 0..100 {
            r.on_success();
        }
        check!(limit(&r) == 4);
        check!(r.inner.permits.available_permits() == 4);
    }

    #[tokio::test]
    async fn held_slot_counts_until_dropped() {
        let r = Retrier::new(1, 0, Duration::from_millis(1));
        let (_, slot) = r
            .send_holding(|| async { Ok::<_, SdkError<ListObjectsV2Error, HttpResponse>>(()) })
            .await
            .unwrap();
        check!(r.inner.permits.available_permits() == 0);
        drop(slot);
        check!(r.inner.permits.available_permits() == 1);
    }

    #[tokio::test(start_paused = true)]
    async fn send_retries_throttles_and_honors_retry_after() {
        let r = Retrier::new(4, 3, Duration::from_millis(1));
        let mut attempts = 0;
        let start = tokio::time::Instant::now();
        let result = r
            .send(|| {
                attempts += 1;
                let attempt = attempts;
                async move {
                    if attempt == 1 {
                        Err(service_error(503, Some("SlowDown"), Some("5000")))
                    } else {
                        Ok(attempt)
                    }
                }
            })
            .await;
        check!(result.ok() == Some(2));
        // The server's 5s is honored even though --max-backoff is 1ms.
        check!(start.elapsed() >= Duration::from_secs(5));
    }

    #[tokio::test(start_paused = true)]
    async fn send_gives_up_after_max_retries() {
        let r = Retrier::new(4, 2, Duration::from_millis(10));
        let mut attempts = 0;
        let result: Result<(), _> = r
            .send(|| {
                attempts += 1;
                async { Err(service_error(503, Some("SlowDown"), None)) }
            })
            .await;
        check!(attempts == 3);
        let err = result.unwrap_err();
        check!(err.to_string().contains("still throttled after 2 retries"));
    }

    #[tokio::test]
    async fn final_throttle_still_lowers_limit_and_counts() {
        // With no retries at all, throttling must still slow the run down
        // and show up in the summary.
        let r = Retrier::new(4, 0, Duration::from_millis(10));
        let result: Result<(), _> = r
            .send(|| async { Err(service_error(503, Some("SlowDown"), None)) })
            .await;
        check!(
            result
                .unwrap_err()
                .to_string()
                .contains("still throttled after 0 retries")
        );
        check!(limit(&r) == 1);
        check!(r.summary().is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn server_delay_is_capped() {
        let r = Retrier::new(4, 1, Duration::from_millis(1));
        let mut attempts = 0;
        let start = tokio::time::Instant::now();
        let result = r
            .send(|| {
                attempts += 1;
                let attempt = attempts;
                async move {
                    if attempt == 1 {
                        Err(service_error(503, Some("SlowDown"), Some("86400000")))
                    } else {
                        Ok(())
                    }
                }
            })
            .await;
        check!(result.is_ok());
        check!(start.elapsed() >= MAX_SERVER_DELAY);
        check!(start.elapsed() < MAX_SERVER_DELAY + Duration::from_secs(1));
    }

    #[tokio::test]
    async fn summary_counts_requests_not_responses() {
        let r = Retrier::new(4, 3, Duration::from_millis(1));
        let mut attempts = 0;
        let result = r
            .send(|| {
                attempts += 1;
                let attempt = attempts;
                async move {
                    if attempt <= 3 {
                        Err(service_error(503, Some("SlowDown"), None))
                    } else {
                        Ok(())
                    }
                }
            })
            .await;
        check!(result.is_ok());
        let summary = r.summary().unwrap();
        check!(summary.contains("S3 throttled 1 request (3 throttled responses in all)"));
    }

    #[tokio::test]
    async fn send_does_not_retry_fatal_errors() {
        let r = Retrier::new(4, 5, Duration::from_millis(10));
        let mut attempts = 0;
        let result: Result<(), _> = r
            .send(|| {
                attempts += 1;
                async { Err(service_error(403, Some("AccessDenied"), None)) }
            })
            .await;
        check!(attempts == 1);
        check!(!result.unwrap_err().to_string().contains("after"));
    }
}
