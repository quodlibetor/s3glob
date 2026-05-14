use std::borrow::Cow;
use std::env;
use std::io::{BufRead, BufReader};

use assert_cmd::Command;
use assert_fs::TempDir;
use assert_fs::prelude::*;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use futures::{FutureExt, future::BoxFuture};
use predicates::prelude::*;
use predicates::str::contains;
use regex::RegexBuilder;
use rstest::rstest;
use testcontainers::core::logs::LogFrame;
use testcontainers::core::logs::consumer::LogConsumer;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::minio::MinIO;

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
#[case("prefix/", &["prefix/2024-01-01/$", "prefix/2024-01-02/$", "prefix/2024-01-03/$"])]
#[case("prefix/*", &["prefix/2024-01-01/$", "prefix/2024-01-02/$", "prefix/2024-01-03/$"])]
#[case("prefix/*03", &["prefix/2024-01-03/$"])]
#[case("prefix/*03*", &["prefix/2024-01-03/$"])]
#[case("prefix/2024*03", &["prefix/2024-01-03/$"])]
#[case("prefix/2024*03*", &["prefix/2024-01-03/$"])]
#[case("prefix/2024*/", &["prefix/2024-01-01/1.txt$", "prefix/2024-01-02/2.txt$", "prefix/2024-01-03/3.txt$"])]
#[case("prefix/2024*/*", &["prefix/2024-01-01/1.txt$", "prefix/2024-01-02/2.txt$", "prefix/2024-01-03/3.txt$"])]
#[trace]
#[tokio::test]
async fn test_glob_prefixes(#[case] glob: &str, #[case] expected: &[&str]) -> anyhow::Result<()> {
    let (_node, port, client) = minio_and_client().await;

    let bucket = "test-bucket";
    client.create_bucket().bucket(bucket).send().await?;

    let test_objects = &[
        "prefix/2024-01-01/1.txt",
        "prefix/2024-01-02/2.txt",
        "prefix/2024-01-03/3.txt",
    ];
    for key in test_objects {
        create_object(&client, bucket, key).await?;
    }

    let needle = format!("s3://{}/{}", bucket, glob);

    let mut cmd = run_s3glob(port, &["ls", needle.as_str()])?;
    let res = cmd.assert().success();
    let output = std::str::from_utf8(&res.get_output().stdout).unwrap();
    // let is_expected = RegexSet::new(expected).unwrap();
    let expected_regexes = expected
        .iter()
        .map(|s| RegexBuilder::new(s).multi_line(true).build().unwrap())
        .collect::<Vec<_>>();
    for re in expected_regexes {
        assert!(
            re.is_match(output),
            "expected regex {:?} to match output {:?}",
            re,
            output
        );
    }

    Ok(())
}

#[rstest]
// TODO: negative case test
// #[case("SARS-CoV-1/*/*RUN0/*CLONE997", &["Error: Could not continue search in prefixes:$"])]
#[case("SARS-CoV-1/*/*/*RUN0/*CLONE997", &["SARS-CoV-1/spike/PROJ14583/RUN0/CLONE997/$"])]
#[trace]
#[tokio::test]
async fn test_glob_prefixes_specific(
    #[case] glob: &str,
    #[case] expected: &[&str],
) -> anyhow::Result<()> {
    let (_node, port, client) = minio_and_client().await;

    let bucket = "test-bucket";
    client.create_bucket().bucket(bucket).send().await?;

    let test_objects = &[
        "SARS-CoV-1/spike/PROJ14583/RUN0/CLONE997/results97/frame97.xtc",
        "SARS-CoV-1/spike/PROJ14216/RUN4/CLONE4/results7/frame7.xtc",
        "SARS-CoV-1/nsp9/PROJ13850/RUN0/CLONE133/results99/frame99.xtc",
    ];
    for key in test_objects {
        create_object(&client, bucket, key).await?;
    }

    let needle = format!("s3://{}/{}", bucket, glob);

    let mut cmd = run_s3glob(port, &["ls", needle.as_str()])?;
    let res = cmd.assert().success();
    let output = std::str::from_utf8(&res.get_output().stdout).unwrap();
    // let is_expected = RegexSet::new(expected).unwrap();
    let expected_regexes = expected
        .iter()
        .map(|s| RegexBuilder::new(s).multi_line(true).build().unwrap())
        .collect::<Vec<_>>();
    for re in expected_regexes {
        assert!(
            re.is_match(output),
            "expected regex {:?} to match output {:?}",
            re,
            output
        );
    }

    Ok(())
}

/// Invariants for canonical `PrefixResult` output.
#[rstest]
// A pattern whose regex matches the empty string (e.g. `*`) must not
// surface a blank `PRE` line for the empty initial prefix.
#[case::no_empty_pre_line(
    &["root.txt", "dir/file.txt"],
    "*",
    &[r"(?m)^PRE\s+dir/$", r"(?m) root\.txt$"],
    &[r"(?m)^PRE\s*$"],
)]
// Intermediate prefixes that can't satisfy the pattern's trailing
// `[delim]?$` end-anchor must be filtered out — `a/bbb/` cannot match
// `*/?` because `?` is a single character.
#[case::trailing_anchor_filters_intermediate_prefix(
    &["a/b/x.txt", "a/bbb/y.txt"],
    "*/?",
    &[r"(?m)^PRE\s+a/b/$"],
    &[r"a/bbb/"],
)]
// When a candidate prefix resolves to a directory rather than a key,
// `get_exact` must emit it with the trailing delimiter (`a/`, not `a`).
#[case::prefix_canonicalized_with_trailing_delim(
    &["a/x"],
    "{a}",
    &[r"(?m)^PRE\s+a/$"],
    &[r"(?m)^PRE\s+a$"],
)]
// A candidate validated only by `check_prefixes` (S3 list with
// `max_keys=1`) returns true for any string that is a prefix of some
// key. `ab` against bucket `[abc]` is such a "phantom" — neither a key
// nor a directory — and must be dropped, not emitted as `Prefix(ab)`.
#[case::phantom_prefix_dropped(
    &["abc"],
    "{ab}",
    &[],
    &[r"(?m)^PRE"],
)]
// In the Choice handler's filter+append branch, an empty alternative
// must not short-circuit the non-empty alternatives. Pattern `*{,/y}`
// against `[a/y]` must surface `Object("a/y")` via the `/y` alt.
#[case::choice_handler_recovers_content_alt(
    &["a/y"],
    "*{,/y}",
    &[r"(?m) a/y$"],
    &[],
)]
#[trace]
#[tokio::test]
async fn test_canonical_prefix_output(
    #[case] source_files: &[&str],
    #[case] glob: &str,
    #[case] must_match: &[&str],
    #[case] must_not_match: &[&str],
) -> anyhow::Result<()> {
    let (_node, port, client) = minio_and_client().await;

    let bucket = "test-bucket";
    client.create_bucket().bucket(bucket).send().await?;
    for key in source_files {
        create_object(&client, bucket, key).await?;
    }

    let needle = format!("s3://{}/{}", bucket, glob);
    let mut cmd = run_s3glob(port, &["ls", needle.as_str()])?;
    let res = cmd.assert().success();
    let output = std::str::from_utf8(&res.get_output().stdout).unwrap();

    for re in must_match {
        let re = RegexBuilder::new(re).multi_line(true).build().unwrap();
        assert!(
            re.is_match(output),
            "expected regex {:?} to match output {:?}",
            re,
            output
        );
    }
    for re in must_not_match {
        let re = RegexBuilder::new(re).multi_line(true).build().unwrap();
        assert!(
            !re.is_match(output),
            "expected regex {:?} NOT to match output {:?}",
            re,
            output
        );
    }

    Ok(())
}

/// The `ls` summary must count objects and logical prefixes
/// (directories) separately. A pattern ending in `/*` matches the
/// directories it descends into as well as their contents, so the
/// summary must not lump the directories in with "objects" — that was
/// the bug behind a run reporting thousands of "matched objects" when
/// only a handful of real objects existed.
#[tokio::test]
async fn test_summary_counts_objects_and_prefixes_separately() -> anyhow::Result<()> {
    let (_node, port, client) = minio_and_client().await;

    let bucket = "test-bucket";
    client.create_bucket().bucket(bucket).send().await?;

    // Pattern `p/*/RUN0/*CLONE997/*` against these keys matches six
    // entities: the three `*CLONE997/` directories themselves (`*`
    // matches the empty trailing segment), the two nested `results/`
    // directories, and the one object sitting directly in a CLONE997
    // dir. That's 1 object + 5 prefixes.
    let test_objects = &[
        "p/a/RUN0/xCLONE997/results/frame.xtc",
        "p/b/RUN0/yCLONE997/results/frame.xtc",
        "p/c/RUN0/zCLONE997/direct.txt",
    ];
    for key in test_objects {
        create_object(&client, bucket, key).await?;
    }

    let needle = format!("s3://{}/p/*/RUN0/*CLONE997/*", bucket);
    let mut cmd = run_s3glob(port, &["ls", needle.as_str()])?;
    let res = cmd.assert().success();
    let stderr = std::str::from_utf8(&res.get_output().stderr).unwrap();

    assert!(
        stderr.contains("Discovered matches:     6"),
        "expected 'Discovered matches: 6' in stderr, got:\n{stderr}",
    );
    assert!(
        stderr.contains("Matched 1 objects and 5 prefixes"),
        "expected 'Matched 1 objects and 5 prefixes' in stderr, got:\n{stderr}",
    );

    Ok(())
}

/// When the search fans out wider than the final match set, the
/// summary reports the peak as "out of N candidates" so the user can
/// see how much keyspace was traversed.
#[tokio::test]
async fn test_summary_reports_candidate_count() -> anyhow::Result<()> {
    let (_node, port, client) = minio_and_client().await;

    let bucket = "test-bucket";
    client.create_bucket().bucket(bucket).send().await?;

    // Listing `data/` for the `*` fans out to five sub-directories;
    // the trailing `/keep` literal then narrows that to the two that
    // actually contain a `keep` key. Peak candidates = 5, matched = 2.
    let test_objects = &[
        "data/a/keep",
        "data/b/keep",
        "data/c/nope",
        "data/d/nope",
        "data/e/nope",
    ];
    for key in test_objects {
        create_object(&client, bucket, key).await?;
    }

    let needle = format!("s3://{}/data/*/keep", bucket);
    let mut cmd = run_s3glob(port, &["ls", needle.as_str()])?;
    let res = cmd.assert().success();
    let stderr = std::str::from_utf8(&res.get_output().stderr).unwrap();

    assert!(
        stderr.contains("Matched 2 objects and 0 prefixes out of 5 candidates"),
        "expected candidate count in stderr summary, got:\n{stderr}",
    );

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
        .behavior_version(BehaviorVersion::latest())
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
    let mut command = assert_cmd::cargo::cargo_bin_cmd!("s3glob");
    let log_directive = env::var("S3GLOB_LOG").unwrap_or_else(|_| "s3glob=trace".to_string());
    command
        .env("AWS_ENDPOINT_URL", format!("http://127.0.0.1:{}", port))
        .env("AWS_ACCESS_KEY_ID", "minioadmin")
        .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
        .env("S3GLOB_LOG", log_directive)
        .args(args);

    print_s3glob_output(&mut command);
    Ok(command)
}

/// Verifies that --force-path-style allows s3glob to work with a hostname
/// endpoint, and that without it the command fails (because the SDK uses
/// virtual-hosted-style, prepending the bucket name to the hostname, e.g.
/// bucket.localhost, which does not resolve).
#[tokio::test]
async fn test_force_path_style() -> anyhow::Result<()> {
    let (_node, port, client) = minio_and_client().await;

    let bucket = "path-style-test";
    client.create_bucket().bucket(bucket).send().await?;
    create_object(&client, bucket, "test/object.txt").await?;

    let pattern = format!("s3://{}/test/*", bucket);
    let localhost_endpoint = format!("http://localhost:{}", port);

    // With --force-path-style the SDK uses http://localhost:{port}/bucket/key
    // and MinIO responds correctly.
    let mut cmd =
        s3glob_with_endpoint(&localhost_endpoint, &["ls", "--force-path-style", &pattern])?;
    cmd.assert().success().stdout(contains("test/object.txt"));

    // Without --force-path-style the SDK tries to reach
    // http://path-style-test.localhost:{port}/test/* which does not resolve.
    let mut cmd = s3glob_with_endpoint(&localhost_endpoint, &["ls", &pattern])?;
    cmd.assert().failure();

    Ok(())
}

fn s3glob_with_endpoint(endpoint: &str, args: &[&str]) -> anyhow::Result<Command> {
    let mut command = assert_cmd::cargo::cargo_bin_cmd!("s3glob");
    let log_directive = env::var("S3GLOB_LOG").unwrap_or_else(|_| "s3glob=trace".to_string());
    command
        .env("AWS_ENDPOINT_URL", endpoint)
        .env("AWS_ACCESS_KEY_ID", "minioadmin")
        .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
        .env("S3GLOB_LOG", log_directive)
        .args(args);
    print_s3glob_output(&mut command);
    Ok(command)
}

#[tokio::test]
async fn test_platform_tls_env_var() -> anyhow::Result<()> {
    let (_node, port, client) = minio_and_client().await;

    let bucket = "platform-tls-test";
    client.create_bucket().bucket(bucket).send().await?;
    create_object(&client, bucket, "hello/world.txt").await?;

    // Run with EXPERIMENTAL_PLATFORM_TLS=true — MinIO is HTTP so https_or_http() covers it.
    // The key thing being tested: the binary doesn't crash or error out when the env var is set,
    // and it still successfully lists objects.
    let mut cmd = run_s3glob(port, &["ls", &format!("s3://{}/hello/*", bucket)])?;
    cmd.env("EXPERIMENTAL_PLATFORM_TLS", "true");
    cmd.assert().success().stdout(contains("hello/world.txt"));

    Ok(())
}

fn print_s3glob_output(cmd: &mut Command) {
    let output = cmd.output().unwrap();
    println!(
        "==== s3glob stdout ====\n{}\n==== s3glob stderr ====\n{}\n==== end s3glob output ====\n",
        String::from_utf8(output.stdout).unwrap(),
        String::from_utf8(output.stderr).unwrap()
    );
}

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

/// Closing stdout early (e.g. piping to `head`) should not panic or
/// crash; s3glob must exit cleanly with status 0.
#[tokio::test]
async fn test_ls_pipe_closed_early_exits_cleanly() -> anyhow::Result<()> {
    use std::process::{Command as StdCommand, Stdio};

    let (_node, port, client) = minio_and_client().await;

    let bucket = "pipe-test";
    client.create_bucket().bucket(bucket).send().await?;
    for i in 0..200 {
        create_object(&client, bucket, &format!("prefix/{i:04}/file.txt")).await?;
    }

    // In `--stream` mode s3glob writes each result as it arrives, and the
    // reader below takes one line then drops the pipe so the next write
    // already hits EPIPE. That alone is enough to exercise the path. The
    // wide `--format` literal (~2 KiB/line, ~400 KiB total for 200 keys)
    // additionally makes it a hard guarantee: that much output cannot fit
    // the OS pipe buffer (~64 KiB), so the child is provably still blocked
    // in `write` when the reader drops, independent of streaming pace or
    // OS scheduling.
    let wide_format = format!("{}{{key}}", "x".repeat(2000));

    let cargo_bin = assert_cmd::cargo::cargo_bin("s3glob");
    let mut child = StdCommand::new(&cargo_bin)
        .env("AWS_ENDPOINT_URL", format!("http://127.0.0.1:{}", port))
        .env("AWS_ACCESS_KEY_ID", "minioadmin")
        .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
        .args([
            "ls",
            "--stream",
            "--format",
            &wide_format,
            &format!("s3://{}/prefix/**", bucket),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    // Read just one line then drop the pipe — the child should keep
    // trying to write more, hit EPIPE, and exit cleanly.
    let stdout = child.stdout.take().expect("piped stdout");
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    assert!(!line.is_empty(), "expected at least one output line");
    drop(reader);

    let output = child.wait_with_output()?;
    assert!(
        output.status.success(),
        "s3glob did not exit cleanly when stdout pipe closed: {:?}",
        output.status,
    );
    // The "Matched ..." summary is emitted to stderr after the stdout
    // loop exits, and must still appear when the pipe closed early.
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Matched "),
        "expected 'Matched' summary in stderr after pipe-close, got:\n{stderr}",
    );

    Ok(())
}
