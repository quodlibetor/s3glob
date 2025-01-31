use assert_cmd::Command;
use assert_fs::prelude::*;
use assert_fs::TempDir;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use predicates::prelude::*;
use predicates::str::contains;
use rstest::rstest;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_modules::minio::MinIO;

#[rstest]
#[case("prefix/2024-*/file*.txt", &[
    "prefix/2024-01/file1.txt",
    "prefix/2024-02/file2.txt",
    "prefix/2024-03/file4.txt",
    "prefix/2024-03/nested/file3.txt",
])]
#[case("prefix/2024-*/nested/file*.txt", &[
    "prefix/2024-03/nested/file3.txt",
])]
#[case("prefix/2024-*/*", &[
    "prefix/2024-01/file1.txt",
    "prefix/2024-02/file2.txt",
    "prefix/2024-03/nested/file3.txt",
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
    "prefix/2024-03/nested/file3.txt",
    "prefix/2024-03/file4.txt",
])]
#[trace]
#[tokio::test]
async fn test_s3glob_pattern_matching(
    #[values("ls", "dl")] command: &str,
    #[case] glob: &str,
    #[case] expected: &[&str],
) -> anyhow::Result<()> {
    println!("---- testing glob: {}", glob);
    println!("---- expected: {:?}", expected);

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
            &[command, uri.as_str(), tempdir.path().to_str().unwrap()],
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
#[case("prefix/2024/file.txt", &["prefix/2024/file.txt"])]
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
        let mut cmd = run_s3glob(port, &[command, needle.as_str(), out_path])?;
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

//
// Helpers
//

async fn minio_and_client() -> (ContainerAsync<MinIO>, u16, Client) {
    let minio = testcontainers_modules::minio::MinIO::default();
    let node = minio.start().await.expect("can start minio");
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
        .args(args);

    print_s3glob_output(&mut command);
    Ok(command)
}

fn print_s3glob_output(cmd: &mut Command) {
    let output = cmd.output().unwrap();
    println!(
        "s3glob output:\n{}\n{}",
        String::from_utf8(output.stdout).unwrap(),
        String::from_utf8(output.stderr).unwrap()
    );
}
