use crate::glob_matcher::GLOB_CHARS;

use super::PathMode;
use super::S3Object;
use super::add_atomic;
use aws_sdk_s3::Client;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use tokio::io::AsyncWriteExt as _;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tracing::warn;

/// A collection of pools for downloading objects
///
/// The general idea is that we want to saturate pretty fast internet,
/// while still prioritizing completing downloads over starting a lot of
/// concurrent downloads.
///
/// So, we have different numbers of parallel downloads allowed simultaneously
/// (e.g. 500 parallel for objects that are less than 200kb, but 4 for objects
/// that are over 10mb).
///
/// These numbers are loosely based on my experience, I haven't done a ton of
/// benchmarking.
pub(crate) struct DlPools {
    pub(crate) two_hundred_kb: UnboundedSender<(Downloader, S3Object)>,
    pub(crate) one_mb: UnboundedSender<(Downloader, S3Object)>,
    pub(crate) ten_mb: UnboundedSender<(Downloader, S3Object)>,
    pub(crate) more: UnboundedSender<(Downloader, S3Object)>,
}

impl DlPools {
    /// Create a new set of downloader pools
    pub(crate) fn new(max_parallelism: usize) -> DlPools {
        let (two_hundred_kb, rx) = tokio::sync::mpsc::unbounded_channel();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(max_parallelism.min(500)));
        start_threadpool(semaphore, rx);
        let (one_mb, rx) = tokio::sync::mpsc::unbounded_channel();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(max_parallelism.min(50)));
        start_threadpool(semaphore, rx);

        let (ten_mb, rx) = tokio::sync::mpsc::unbounded_channel();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(max_parallelism.min(10)));
        start_threadpool(semaphore, rx);

        let (more, rx) = tokio::sync::mpsc::unbounded_channel();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(max_parallelism.min(5)));
        start_threadpool(semaphore, rx);

        Self {
            two_hundred_kb,
            one_mb,
            ten_mb,
            more,
        }
    }

    pub(crate) fn download_object(&self, dl: Downloader, object: S3Object) {
        let size = object.size;
        let tx = if size < 200_000 {
            &self.two_hundred_kb
        } else if size < 1_000_000 {
            &self.one_mb
        } else if size < 10_000_000 {
            &self.ten_mb
        } else {
            &self.more
        };
        tx.send((dl, object))
            .expect("send on channel should succeed");
    }
}

pub(crate) fn start_threadpool(
    semaphore: Arc<tokio::sync::Semaphore>,
    mut rx: UnboundedReceiver<(Downloader, S3Object)>,
) {
    tokio::spawn(async move {
        while let Some((dl, obj)) = rx.recv().await {
            let permit = semaphore.clone().acquire_owned().await;
            tokio::spawn(async move {
                dl.download_object(obj).await;
                drop(permit);
            });
        }
    });
}

#[derive(Debug)]
pub(crate) struct Downloader {
    pub(crate) client: Client,
    pub(crate) bucket: String,
    pub(crate) prefix_to_strip: String,
    pub(crate) flatten: bool,
    pub(crate) base_path: PathBuf,
    pub(crate) obj_counter: Arc<AtomicUsize>,
    pub(crate) obj_id: usize,
    pub(crate) notifier: UnboundedSender<Notification>,
}

#[derive(Debug)]
pub(crate) enum Notification {
    ObjectDownloaded(PathBuf),
    BytesDownloaded(usize),
}

impl Downloader {
    pub(crate) fn new(
        client: Client,
        bucket: String,
        prefix_to_strip: String,
        flatten: bool,
        base_path: PathBuf,
        notifier: UnboundedSender<Notification>,
    ) -> Self {
        Self {
            client,
            bucket,
            obj_counter: Arc::new(AtomicUsize::new(0)),
            obj_id: 0,
            notifier,
            base_path,
            flatten,
            prefix_to_strip,
        }
    }

    /// Create a downloader that can safely download another object
    pub(crate) fn fresh(&self) -> Self {
        let obj_id = add_atomic(&self.obj_counter, 1);
        Self {
            client: self.client.clone(),
            bucket: self.bucket.clone(),
            obj_counter: Arc::clone(&self.obj_counter),
            obj_id,
            notifier: self.notifier.clone(),
            prefix_to_strip: self.prefix_to_strip.clone(),
            flatten: self.flatten,
            base_path: self.base_path.clone(),
        }
    }

    pub(crate) async fn download_object(self, obj: S3Object) {
        let key = &obj.key;
        let mut key_suffix = key
            .strip_prefix(&self.prefix_to_strip)
            .expect("all found objects will include the prefix")
            .to_string();
        if self.flatten {
            key_suffix = key_suffix.replace(std::path::MAIN_SEPARATOR_STR, "-");
        }
        let path = self.base_path.join(key_suffix);
        let dir = path.parent().unwrap();
        if let Err(e) = std::fs::create_dir_all(dir) {
            warn!("Failed to create directory {}: {}", dir.display(), e);
            return;
        };
        let result = self
            .client
            .get_object()
            .bucket(self.bucket)
            .key(key)
            .send()
            .await;
        let Ok(mut obj) = result else {
            warn!("Failed to download object {}", key);
            return;
        };
        let temp_path = path.with_extension(format!(".s3glob-tmp-{}", self.obj_id));
        let mut file = match tokio::fs::File::create(&temp_path).await {
            Ok(file) => file,
            Err(e) => {
                warn!("Failed to create file {}: {}", temp_path.display(), e);
                return;
            }
        };
        let mut res = obj.body.try_next().await;
        loop {
            match res {
                Ok(Some(bytes)) => {
                    if let Err(e) = file.write_all(&bytes).await {
                        warn!("Failed to write to file {}: {}", path.display(), e);
                        return;
                    };
                    self.notifier
                        .send(Notification::BytesDownloaded(bytes.len()))
                        .expect("can send on channel");
                }
                Ok(None) => break,
                Err(e) => {
                    warn!("Failed to download object {}: {}", key, e);
                    return;
                }
            }
            res = obj.body.try_next().await;
        }
        if let Err(e) = file.flush().await {
            warn!("Failed to flush file {}: {}", temp_path.display(), e);
            drop(file);
            return;
        };
        drop(file);
        if let Err(e) = std::fs::rename(&temp_path, &path) {
            warn!(
                "Failed to rename file {} -> {}: {}",
                &temp_path.display(),
                path.display(),
                e
            );
            return;
        };
        self.notifier
            .send(Notification::ObjectDownloaded(path))
            .expect("send on our channel should succeed");
    }
}

pub(crate) fn extract_prefix_to_strip(
    raw_pattern: &str,
    path_mode: PathMode,
    keys: &[S3Object],
) -> String {
    match path_mode {
        PathMode::Abs | PathMode::Absolute => String::new(),
        PathMode::FromFirstGlob | PathMode::G => {
            let up_to_glob: String = raw_pattern
                .chars()
                .take_while(|c| !GLOB_CHARS.contains(c))
                .collect();
            // find the last slash in the prefix and only include that
            match up_to_glob.rfind('/') {
                Some(slash_idx) => up_to_glob[..slash_idx + 1].to_string(),
                None => up_to_glob,
            }
        }
        PathMode::S | PathMode::Shortest => {
            let Some(prefix) = keys.first() else {
                return String::new();
            };
            let mut prefix = prefix.key.to_string();
            for key_obj in &keys[1..] {
                prefix = prefix
                    .chars()
                    .zip(key_obj.key.chars())
                    .take_while(|(a, b)| a == b)
                    .map(|(a, _)| a)
                    .collect();
            }
            // get the prefix up to and including the last slash
            let suffix = prefix.chars().rev().take_while(|c| *c != '/').count();
            prefix.truncate(prefix.len() - suffix);
            prefix
        }
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_s3::primitives::DateTime;

    use super::*;

    macro_rules! assert_extract_prefix_to_strip {
        ($pattern:expr, $path_mode:expr, $expected:expr) => {
            let actual = extract_prefix_to_strip($pattern, $path_mode, &[]);
            assert2::check!(
                actual == $expected,
                "input: {} path_mode: {:?}",
                $pattern,
                $path_mode,
            );
        };
        ($pattern:expr, $path_mode:expr, $expected:expr, $keys:expr) => {
            let keys: &[S3Object] = $keys;
            let actual = extract_prefix_to_strip($pattern, $path_mode, keys);
            assert2::check!(
                actual == $expected,
                "input: {} path_mode: {:?} keys: {:?}",
                $pattern,
                $path_mode,
                keys,
            );
        };
    }

    #[test]
    fn test_extract_prefix_to_strip() {
        // Test absolute path mode
        assert_extract_prefix_to_strip!("prefix/path/to/*.txt", PathMode::Absolute, "");
        assert_extract_prefix_to_strip!("bucket/deep/path/*.txt", PathMode::Abs, "");

        // Test from-first-glob path mode
        assert_extract_prefix_to_strip!(
            "prefix/path/to/*.txt",
            PathMode::FromFirstGlob,
            "prefix/path/to/"
        );
        assert_extract_prefix_to_strip!(
            "prefix/path/*/more/*.txt",
            PathMode::FromFirstGlob,
            "prefix/path/"
        );
        assert_extract_prefix_to_strip!("prefix/*.txt", PathMode::FromFirstGlob, "prefix/");
        assert_extract_prefix_to_strip!("*.txt", PathMode::FromFirstGlob, "");
        assert_extract_prefix_to_strip!("prefix/a.txt", PathMode::FromFirstGlob, "prefix/");
        // Test with different glob characters
        assert_extract_prefix_to_strip!(
            "prefix/path/to/[abc]/*.txt",
            PathMode::FromFirstGlob,
            "prefix/path/to/"
        );
        assert_extract_prefix_to_strip!(
            "prefix/path/to/?/*.txt",
            PathMode::FromFirstGlob,
            "prefix/path/to/"
        );
        assert_extract_prefix_to_strip!(
            "prefix/path/{a,b}/*.txt",
            PathMode::FromFirstGlob,
            "prefix/path/"
        );
    }

    #[test]
    fn test_extract_prefix_to_strip_shortest() {
        // Helper function to create S3Objects for testing
        fn make_objects(keys: &[&str]) -> Vec<S3Object> {
            keys.iter()
                .map(|&key| S3Object {
                    key: key.to_string(),
                    size: 0,
                    last_modified: DateTime::from_millis(0),
                })
                .collect()
        }

        // Different prefixes entirely - no common prefix
        assert_extract_prefix_to_strip!(
            "different/*/file*.txt",
            PathMode::Shortest,
            "",
            &make_objects(&["different/path/file1.txt", "alternate/path/file2.txt",])
        );

        // Partial prefix overlap
        assert_extract_prefix_to_strip!(
            "shared-prefix/*/data/*.txt",
            PathMode::Shortest,
            "",
            &make_objects(&[
                "shared-prefix/abc/data/file1.txt",
                "shared-prefix-extra/xyz/data/file2.txt",
            ])
        );

        // One path is a prefix of another
        assert_extract_prefix_to_strip!(
            "deep/nested/*/file*.txt",
            PathMode::Shortest,
            "deep/nested/path/",
            &make_objects(&[
                "deep/nested/path/file1.txt",
                "deep/nested/path/more/file2.txt",
            ])
        );

        // Empty prefix case - files in root
        assert_extract_prefix_to_strip!(
            "*.txt",
            PathMode::Shortest,
            "",
            &make_objects(&["file1.txt", "file2.txt",])
        );

        // Original test cases
        assert_extract_prefix_to_strip!(
            "prefix/2024-*/file*.txt",
            PathMode::Shortest,
            "prefix/",
            &make_objects(&[
                "prefix/2024-01/file1.txt",
                "prefix/2024-01/file2.txt",
                "prefix/2024-02/file2.txt",
            ])
        );

        assert_extract_prefix_to_strip!(
            "prefix/nested/*/file*.txt",
            PathMode::Shortest,
            "prefix/nested/",
            &make_objects(&["prefix/nested/a/file1.txt", "prefix/nested/b/file2.txt",])
        );

        assert_extract_prefix_to_strip!(
            "prefix/*/nested/*.txt",
            PathMode::Shortest,
            "prefix/a/nested/",
            &make_objects(&["prefix/a/nested/file1.txt", "prefix/a/nested/file2.txt",])
        );

        // Edge case: empty keys list
        assert_extract_prefix_to_strip!("any/pattern/*.txt", PathMode::Shortest, "", &[]);

        // Edge case: single key
        assert_extract_prefix_to_strip!(
            "single/path/*.txt",
            PathMode::Shortest,
            "single/path/",
            &make_objects(&["single/path/file.txt"])
        );
    }
}
