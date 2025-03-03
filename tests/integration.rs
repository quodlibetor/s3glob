use assert_cmd::Command;
use assert_fs::TempDir;
use assert_fs::prelude::*;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use predicates::prelude::*;
use predicates::str::contains;
use rstest::rstest;
use testcontainers::core::logs::LogFrame;
use testcontainers::core::logs::consumer::LogConsumer;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};

mod minio_testcontainer;

use minio_testcontainer::MinIO;

#[rstest]
#[case("prefix/2024-*/file*.txt", &[
    "prefix/2024-01/file1.txt",
    "prefix/2024-02/file2.txt",
    "prefix/2024-03/file4.txt",
])]
#[case("prefix/2024-*/nested/file*.txt", &[
    "prefix/2024-03/nested/file3.txt",
])]
#[case("prefix/2024-*/*", &[
    "prefix/2024-01/file1.txt",
    "prefix/2024-02/file2.txt",
    "prefix/2024-03/nested/",
    "prefix/2024-03/file4.txt",
])]
#[case("prefix/2024-*/**", &[
    "prefix/2024-01/file1.txt",
    "prefix/2024-02/file2.txt",
    "prefix/2024-03/nested/file3.txt",
    "prefix/2024-03/file4.txt",
])]
#[case("prefix/2024-{01,03}/*", &[
    "prefix/2024-01/file1.txt",
    "prefix/2024-03/nested/",
    "prefix/2024-03/file4.txt",
])]
#[trace]
#[tokio::test]
async fn test_s3glob_pattern_matching(
    #[values("ls", "dl")] command: &str,
    #[case] glob: &str,
    #[case] expected: &[&str],
) -> anyhow::Result<()> {
    // Start MinIO container and configure S3 client
    let (_node, port, client) = minio_and_client().await;

    // Create test bucket and upload test objects
    let bucket = "test-bucket";
    client.create_bucket().bucket(bucket).send().await?;

    let test_objects = vec![
        "prefix/2024-01/file1.txt",
        "prefix/2024-02/file2.txt",
        "prefix/2024-03/nested/file3.txt",
        "prefix/2024-03/file4.txt",
        "other/2024-01/file5.txt",
    ];
    for key in &test_objects {
        create_object(&client, bucket, key).await?;
    }

    let uri = format!("s3://{}/{}", bucket, glob);

    if command == "ls" {
        let mut cmd = run_s3glob(port, &[command, uri.as_str()])?;
        let mut res = cmd.assert().success();
        for object in &test_objects {
            if expected.contains(object) {
                res = res.stdout(contains(*object));
            } else {
                res = res.stdout(contains(*object).not());
            }
        }
    } else {
        let tempdir = TempDir::new()?;
        let mut cmd = run_s3glob(
            port,
            &[
                command,
                "-pabs",
                uri.as_str(),
                tempdir.path().to_str().unwrap(),
            ],
        )?;
        let _ = cmd.assert().success();
        for object in &test_objects {
            if expected.contains(object) {
                tempdir.child(object).assert(predicate::path::exists());
            } else {
                tempdir.child(object).assert(predicate::path::missing());
            }
        }
    };

    Ok(())
}

#[rstest]
#[case("prefix/2024-01/file1.txt", &["file1.txt"])]
#[case("prefix/2024-01/file*.txt", &["file1.txt", "file2.txt"])]
#[case("prefix/2024-*/file1.txt", &["2024-02/file1.txt"])]
#[case("prefix/2024-*/nested/*3*", &["2024-02/nested/file3.txt"])]
#[case("prefix/2024-0{1,3}/*", &["2024-01/file1.txt", "2024-03/file5.txt"])]
#[tokio::test]
async fn test_download_prefix_from_first_glob(
    #[case] glob: &str,
    #[case] expected: &[&str],
) -> anyhow::Result<()> {
    let (_node, port, client) = minio_and_client().await;

    let bucket = "test-bucket";
    client.create_bucket().bucket(bucket).send().await?;

    let test_objects = vec![
        "prefix/2024-01/file1.txt",
        "prefix/2024-01/file2.txt",
        "prefix/2024-02/nested/file3.txt",
        "prefix/2024-02/nested/file4.txt",
        "prefix/2024-03/file5.txt",
    ];
    for key in &test_objects {
        create_object(&client, bucket, key).await?;
    }

    let tempdir = TempDir::new()?;

    let mut cmd = run_s3glob(
        port,
        &[
            "dl",
            "-p",
            "from-first-glob",
            format!("s3://{}/{}", bucket, glob).as_str(),
            tempdir.path().to_str().unwrap(),
        ],
    )?;

    let _ = cmd.assert().success();

    for object in test_objects {
        if expected.contains(&object) {
            tempdir.child(object).assert(predicate::path::exists());
        } else {
            tempdir.child(object).assert(predicate::path::missing());
        }
    }

    Ok(())
}

#[rstest]
#[case("{key}", "test/file.txt")]
#[case("{size_bytes}", "1234")]
#[case("{size_human}", "1.2kB")]
#[case("{key} ({size_human})", "test/file.txt (1.2kB)")]
#[case(
    "Size: {size_bytes} bytes, Name: {key}",
    "Size: 1234 bytes, Name: test/file.txt"
)]
#[case(
    "File: {key}\nSize: {size_human}\nModified: {last_modified}",
    "File: test/file.txt\nSize: 1.2kB\nModified: "
)]
#[tokio::test]
async fn test_format_patterns(
    #[case] format: &str,
    #[case] expected: &'static str,
) -> anyhow::Result<()> {
    let (_node, port, client) = minio_and_client().await;

    let bucket = "format-test";
    client.create_bucket().bucket(bucket).send().await?;

    let key = "test/file.txt";
    create_object_with_size(&client, bucket, key, 1234).await?;

    let objects = client.list_objects_v2().bucket(bucket).send().await?;
    for obj in objects.contents() {
        println!(
            "created obj: {:?} size: {:?}",
            obj.key().unwrap(),
            obj.size()
        );
    }

    let pattern = format!("s3://{}/*/file.txt", bucket);

    let mut cmd = run_s3glob(port, &["ls", "--format", format, pattern.as_str()])?;
    cmd.assert().success().stdout(contains(expected));
    Ok(())
}

#[rstest]
#[case("prefix/2024/file1.txt", &["prefix/2024/file1.txt"])]
#[case("prefix/2024/file*.txt", &[
    "prefix/2024/file1.txt",
    "prefix/2024/file2.txt",
])]
#[tokio::test]
async fn test_patterns_in_file_not_path_component(
    #[values("ls", "dl")] command: &str,
    #[case] glob: &str,
    #[case] expected: &[&str],
) -> anyhow::Result<()> {
    let (_node, port, client) = minio_and_client().await;

    let bucket = "test-bucket";
    client.create_bucket().bucket(bucket).send().await?;

    let test_objects = vec![
        "prefix/2024/file1.txt",
        "prefix/2024/file2.txt",
        "prefix/2024/other.txt",
        "other/path/file.txt",
    ];
    for key in &test_objects {
        create_object(&client, bucket, key).await?;
    }

    let needle = format!("s3://{}/{}", bucket, glob);

    if command == "ls" {
        let mut cmd = run_s3glob(port, &[command, needle.as_str()])?;
        let mut res = cmd.assert().success();
        for object in &test_objects {
            if expected.contains(object) {
                res = res.stdout(contains(*object));
            } else {
                res = res.stdout(contains(*object).not());
            }
        }
    } else {
        let tempdir = TempDir::new()?;
        let out_path = tempdir.path().to_str().unwrap();
        let mut cmd = run_s3glob(port, &[command, "-pabs", needle.as_str(), out_path])?;
        let _ = cmd.assert().success();
        for object in &test_objects {
            if expected.contains(object) {
                tempdir.child(object).assert(predicate::path::exists());
            } else {
                tempdir.child(object).assert(predicate::path::missing());
            }
        }
    }

    Ok(())
}

#[rstest]
#[case( // 1 Keep different subdirs
    &[
        "prefix/2024-01/file1.txt",
        "prefix/2024-01/file2.txt",
        "prefix/2024-02/file2.txt",
    ],
    &[
        "2024-01/file1.txt",
        "2024-01/file2.txt",
        "2024-02/file2.txt",
    ]
)]
#[case( // 2 Slightly deeper nesting
    &[
        "prefix/nested/a/file1.txt",
        "prefix/nested/b/file2.txt",
    ],
    &[
        "a/file1.txt",
        "b/file2.txt",
    ]
)]
#[case( // 3 Strip nested prefix
    &[
        "prefix/a/nested/file1.txt",
        "prefix/a/nested/file2.txt",
    ],
    &[
        "file1.txt",
        "file2.txt",
    ]
)]
#[case( // 4 Empty prefix case - when files are in root of bucket
    &[
        "file1.txt",
        "file2.txt",
    ],
    &[
        "file1.txt",
        "file2.txt",
    ]
)]
#[case( // 5 Different prefixes entirely - shortest should find no common prefix
    &[
        "different/path/file1.txt",
        "alternate/path/file2.txt",
    ],
    &[
        "different/path/file1.txt",
        "alternate/path/file2.txt",
    ]
)]
#[case( // 6 Partial prefix overlap - shortest should break on path boundaries
    &[
        "shared-prefix/abc/data/file1.txt",
        "shared-prefix-extra/xyz/data/file2.txt",
    ],
    &[
        "shared-prefix/abc/data/file1.txt",
        "shared-prefix-extra/xyz/data/file2.txt",
    ]
)]
#[case( // 7 One path is a prefix of another - shortest should preserve uniqueness

    &[
        "deep/nested/path/file1.txt",
        "deep/nested/path/more/file2.txt",
    ],
    &[
        "file1.txt",
        "more/file2.txt",
    ]
)]
#[tokio::test]
async fn test_download_prefix_shortest(
    #[case] source_files: &[&str],
    #[case] expected_paths: &[&str],
) -> anyhow::Result<()> {
    let glob = "**";
    let (_node, port, client) = minio_and_client().await;

    let bucket = "test-bucket";
    client.create_bucket().bucket(bucket).send().await?;

    for key in source_files {
        create_object(&client, bucket, key).await?;
    }

    let tempdir = TempDir::new()?;

    let mut cmd = run_s3glob(
        port,
        &[
            "dl",
            "-p",
            "shortest",
            format!("s3://{}/{}", bucket, glob).as_str(),
            tempdir.path().to_str().unwrap(),
        ],
    )?;

    let _ = cmd.assert().success();

    for path in expected_paths {
        tempdir.child(path).assert(predicate::path::exists());
    }

    Ok(())
}

#[tokio::test]
async fn test_download_flatten() -> anyhow::Result<()> {
    let (_node, port, client) = minio_and_client().await;

    let bucket = "test-bucket";
    client.create_bucket().bucket(bucket).send().await?;

    // Create some nested test files
    let source_files = [
        "prefix/nested/deep/file1.txt",
        "prefix/other/path/file2.txt",
        "prefix/file3.txt",
    ];
    for key in &source_files {
        create_object(&client, bucket, key).await?;
    }

    let tempdir = TempDir::new()?;

    // Run s3glob with flatten flag
    let mut cmd = run_s3glob(
        port,
        &[
            "dl",
            "--flatten",
            format!("s3://{}/prefix/**/*.txt", bucket).as_str(),
            tempdir.path().to_str().unwrap(),
        ],
    )?;

    let _ = cmd.assert().success();

    // Expected flattened filenames
    let expected_files = ["nested-deep-file1.txt", "other-path-file2.txt", "file3.txt"];

    // Verify that files exist with flattened names
    for expected in &expected_files {
        tempdir.child(expected).assert(predicate::path::exists());
    }

    // Verify original paths don't exist
    for source in &source_files {
        tempdir.child(source).assert(predicate::path::missing());
    }

    Ok(())
}

//
// Helpers
//

async fn minio_and_client() -> (ContainerAsync<MinIO>, u16, Client) {
    let minio = MinIO::default()
        .with_name("quay.io/minio/minio")
        .with_log_consumer(LogPrinter::new());
    let node = match minio.start().await {
        Ok(node) => node,
        Err(e) => {
            panic!("can't start minio: {}", e);
        }
    };
    let port = node.get_host_port_ipv4(9000).await.expect("can get port");

    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::v2024_03_28())
        .region(Region::new("us-east-1"))
        .endpoint_url(format!("http://127.0.0.1:{}", port))
        .credentials_provider(Credentials::new(
            "minioadmin",
            "minioadmin",
            None,
            None,
            "test",
        ))
        .build();

    let client = Client::from_conf(config);

    (node, port, client)
}

async fn create_object(client: &Client, bucket: &str, key: &str) -> anyhow::Result<()> {
    create_object_with_size(client, bucket, key, 1).await?;
    Ok(())
}

async fn create_object_with_size(
    client: &Client,
    bucket: &str,
    key: &str,
    size: usize,
) -> anyhow::Result<()> {
    let body = vec![b'a'; size];
    client
        .put_object()
        .bucket(bucket)
        .key(key.to_string())
        .body(ByteStream::from(body))
        .send()
        .await?;

    Ok(())
}

fn run_s3glob(port: u16, args: &[&str]) -> anyhow::Result<Command> {
    let mut command = Command::cargo_bin("s3glob")?;
    command
        .env("AWS_ENDPOINT_URL", format!("http://127.0.0.1:{}", port))
        .env("AWS_ACCESS_KEY_ID", "minioadmin")
        .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
        .env("S3GLOB_LOG", "s3glob=trace")
        .args(args);

    print_s3glob_output(&mut command);
    Ok(command)
}

fn print_s3glob_output(cmd: &mut Command) {
    let output = cmd.output().unwrap();
    println!(
        "==== s3glob stdout ====\n{}\n==== s3glob stderr ====\n{}\n==== end s3glob output ====\n",
        String::from_utf8(output.stdout).unwrap(),
        String::from_utf8(output.stderr).unwrap()
    );
}

use std::borrow::Cow;

use futures::{FutureExt, future::BoxFuture};

/// A consumer that logs the output of container with the [`log`] crate.
///
/// By default, both standard out and standard error will both be emitted at INFO level.
#[derive(Debug)]
pub struct LogPrinter {
    prefix: Option<String>,
}

impl LogPrinter {
    /// Creates a new instance of the logging consumer.
    pub fn new() -> Self {
        Self { prefix: None }
    }

    /// Sets a prefix to be added to each log message (space will be added between prefix and message).
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    fn format_message<'a>(&self, message: &'a str) -> Cow<'a, str> {
        let message = message.trim_end_matches(['\n', '\r']);

        if let Some(prefix) = &self.prefix {
            Cow::Owned(format!("{} {}", prefix, message))
        } else {
            Cow::Borrowed(message)
        }
    }
}

impl Default for LogPrinter {
    fn default() -> Self {
        Self::new()
    }
}

impl LogConsumer for LogPrinter {
    fn accept<'a>(&'a self, record: &'a LogFrame) -> BoxFuture<'a, ()> {
        async move {
            match record {
                LogFrame::StdOut(bytes) => {
                    println!(
                        "minio> {}",
                        self.format_message(&String::from_utf8_lossy(bytes))
                    );
                }
                LogFrame::StdErr(bytes) => {
                    eprintln!(
                        "minio> {}",
                        self.format_message(&String::from_utf8_lossy(bytes))
                    );
                }
            }
        }
        .boxed()
    }
}
