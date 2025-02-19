use std::io::{IsTerminal as _, Write as _};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context as _, Result};
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::types::Object;
use aws_sdk_s3::{config::BehaviorVersion, config::Region, Client};
use clap::{ArgAction, Parser, Subcommand};
use glob_matcher::{S3Engine, S3GlobMatcher};
use humansize::{FormatSizeOptions, SizeFormatter, DECIMAL};
use num_format::{Locale, ToFormattedString};
use regex::Regex;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, trace};

mod glob_matcher;

#[derive(Debug, Subcommand)]
enum Command {
    /// List objects matching the pattern
    #[clap(name = "ls")]
    List {
        /// Glob pattern to match objects against
        ///
        /// The pattern can either be an s3 uri or a <bucket>/<glob> without the
        /// s3://
        ///
        /// Example:
        ///     s3://my-bucket/my_prefix/2024-12-*/something_else/*
        ///     my-bucket/my_prefix/2024-12-*/something_else/*
        #[clap(verbatim_doc_comment)]
        pattern: String,

        /// Format string for output
        ///
        /// This is a string that will be formatted for each object.
        ///
        /// The format string can use the following variables:
        ///
        /// - `{key}`: the key of the object
        /// - `{uri}`: the s3 uri of the object, e.g. s3://my-bucket/my-object.txt
        /// - `{size_bytes}`: the size of the object in bytes, with no suffix
        /// - `{size_human}`: the size of the object in a decimal format (e.g. 1.23MB)
        /// - `{last_modified}`: the last modified date of the object, RFC3339 format
        ///
        /// For example, the default format looks as though you ran s3glob like this:
        ///
        ///     s3glob ls -f "{last_modified} {size_human} {key}" "my-bucket/*"
        #[clap(short, long, verbatim_doc_comment)]
        format: Option<String>,

        /// Stream keys as they are found, rather than sorting and printing at the end
        #[clap(long)]
        stream: bool,
    },

    /// Download objects matching the pattern
    #[clap(name = "dl")]
    Download {
        /// Glob pattern to match objects against
        ///
        /// The pattern can either be an s3 uri or a <bucket>/<glob> without the
        /// s3://
        ///
        /// Example:
        ///     s3://my-bucket/my_prefix/2024-12-*/something_else/*
        ///     my-bucket/my_prefix/2024-12-*/something_else/*
        #[clap(verbatim_doc_comment)]
        pattern: String,

        /// The destination directory to download the objects to
        ///
        /// The full key name will be reproduced in the directory, so multiple
        /// folders may be created.
        dest: String,
    },

    /// Learn how to tune s3glob's parallelism for better performance
    ///
    /// You only need to read this doc if you feel like s3glob is running
    /// slower than you hope.
    ///
    /// Because of the APIs provided by AWS, s3glob can only meaningfully issue
    /// parallel requests for prefixes. Additionally, prefixes can only be
    /// generated before a delimiter.
    ///
    /// So if you have a keyspace (using {..-..} to represent a range) that
    /// looks like:
    ///
    ///    s3://bucket/{a-z}/{0-999}/OBJECT_ID.txt
    ///
    /// and you want to find all the text files where OBJECT_ID is 5, you have
    /// several options for patterns:
    ///
    ///    1: s3glob ls bucket/**/5.txt    -- parallelism 1
    ///    2: s3glob ls bucket/*/**/5.txt  -- parallelism 26
    ///    3: s3glob ls bucket/*/*/5.txt   -- parallelism 26,000
    ///
    /// Which one is best depends on exactly what you're searching for.
    ///
    /// If you have suggestions for improving s3glob's parallelism,
    /// please feel free to open an issue at https://github.com/quodlibetor/s3glob/issues
    #[clap(verbatim_doc_comment)]
    Parallelism {
        #[clap(short, hide = true)]
        region: bool,

        #[clap(short, hide = true)]
        delimiter: bool,

        #[clap(short, hide = true)]
        verbose: bool,

        #[clap(short, hide = true)]
        no_sign_requests: bool,
    },
}

#[derive(Debug, Parser)]
#[command(version, author, about, max_term_width = 80)]
/// A fast aws s3 ls and downloader that supports glob patterns
///
/// Object discovery is done based on a unixy glob pattern,
/// See the README for more details:
/// https://github.com/quodlibetor/s3glob/blob/main/README.md
struct Opts {
    #[clap(subcommand)]
    command: Command,

    /// A region to begin bucket region auto-discovery in
    ///
    /// You should be able to ignore this option if you are using AWS S3.
    #[clap(short, long, default_value = "us-east-1", global = true)]
    region: String,

    /// S3 delimiter to use when listing objects
    ///
    /// This will be used to create a filtered list of prefixes at the first "directory"
    /// that includes a glob character.
    ///
    /// Example:
    ///     my_prefix/2024-12-*/something_else/*
    ///
    /// will first find all the prefixes that match this pattern, with no
    /// slashes between the dash and the slash:
    ///
    ///     my_prefix/2024-12-*/
    ///
    /// and then will list all the objects in these prefixes, filtering them
    /// with the remainder of the pattern.
    #[clap(short, long, default_value = "/", global = true)]
    delimiter: char,

    /// How verbose to be, specify multiple times to increase verbosity
    ///
    /// - `-v` will show debug logs from s3glob
    /// - `-vv` will show trace logs from s3glob
    /// - `-vvv` will show trace logs from s3glob and debug logs from all
    ///   dependencies
    ///
    /// If you want more control you can set the S3GLOB_LOG env var
    /// using rust-tracing's EnvFilter syntax.
    #[clap(short, long, global = true, action = ArgAction::Count, verbatim_doc_comment)]
    verbose: u8,

    /// Do not provide your credentials when issuing requests
    ///
    /// This is useful for downloading objects from a bucket that is not
    /// associated with your AWS account, such as a public bucket.
    #[clap(long, global = true, alias = "no-sign-requests")]
    no_sign_request: bool,
}

fn main() {
    let opts = Opts::parse();
    setup_logging(log_directive(opts.verbose));
    debug!(?opts, "parsed options");

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        if let Err(err) = run(opts).await {
            eprintln!("Failed to run: {}", err);
            let mut err = err.source();
            let mut count = 0;
            let mut prev_msg = String::new();
            while let Some(e) = err {
                if e.to_string() != prev_msg {
                    eprintln!("  : {}", e);
                    prev_msg = e.to_string();
                }
                err = e.source();
                count += 1;
                if count > 10 {
                    break;
                }
            }
            std::process::exit(1);
        }
    });
    // without this, tokio takes a long time to exit
    rt.shutdown_timeout(Duration::from_millis(1));
}

async fn run(opts: Opts) -> Result<()> {
    let start = Instant::now();
    let pat = match &opts.command {
        Command::List { pattern, .. } | Command::Download { pattern, .. } => pattern,
        Command::Parallelism { .. } => {
            eprintln!("This is just for documentation, run instead: s3glob help parallelism");
            return Ok(());
        }
    };
    let s3re = Regex::new(r"^(?:s3://)?([^/]+)/(.*)").unwrap();
    let matches = s3re.captures(pat);
    let (bucket, raw_pattern) = if let Some(m) = matches {
        (
            m.get(1).unwrap().as_str().to_owned(),
            m.get(2).unwrap().as_str().to_owned(),
        )
    } else {
        bail!("pattern must have a <bucket>/<pattern> format, with an optional s3:// prefix");
    };

    let client = create_s3_client(&opts, &bucket).await?;

    let prefix = raw_pattern
        .find(['*', '?', '[', '{'])
        .map_or(raw_pattern.clone(), |i| raw_pattern[..i].to_owned());

    let engine = S3Engine::new(client.clone(), bucket.clone(), opts.delimiter.to_string());
    let matcher = S3GlobMatcher::parse(raw_pattern, &opts.delimiter.to_string())?;
    let mut prefixes = match matcher.find_prefixes(engine).await {
        Ok(prefixes) => prefixes,
        Err(err) => {
            // the matcher prints some progress info to stderr, if there's an
            // error we should make sure to add a newline
            eprintln!();
            return Err(err);
        }
    };
    trace!(?prefixes, "matcher generated prefixes");
    debug!(prefix_count = prefixes.len(), "matcher generated prefixes");

    // If there are no common prefixes, then the prefix itself is the only
    // matching prefix.
    if prefixes.is_empty() {
        debug!(?prefix, "no glob_prefixes found, using simple prefix");
        prefixes.push(prefix);
    }

    let mut tasks = Vec::new();

    let total_objects = Arc::new(AtomicUsize::new(0));
    let seen_prefixes = Arc::new(AtomicUsize::new(0));
    let total_prefixes = prefixes.len();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PrefixResult>();
    for prefix in prefixes {
        let client = client.clone();
        let total_objects = Arc::clone(&total_objects);
        let seen_prefixes = Arc::clone(&seen_prefixes);
        let matcher = matcher.clone();
        let bucket = bucket.clone();
        let tx = tx.clone();

        tasks.push(tokio::spawn(async move {
            list_matching_objects(&client, &bucket, &prefix, &matcher, total_objects, tx).await?;

            add_atomic(&seen_prefixes, 1);
            Ok::<_, anyhow::Error>(())
        }));
    }
    drop(tx);

    match opts.command {
        Command::List { format, stream, .. } => {
            let user_format = if let Some(user_fmt) = format {
                Some(compile_format(&user_fmt)?)
            } else {
                None
            };
            let mut matching_objects = Vec::new();
            let mut match_count = 0;
            let decimal = decimal_format();
            while let Some(PrefixResult {
                matching_objects: mo,
                ..
            }) = rx.recv().await
            {
                if stream {
                    match_count += mo.len();
                    for obj in mo.iter() {
                        if let Some(user_fmt) = &user_format {
                            print_user(&bucket, obj, user_fmt);
                        } else {
                            print_default(obj, decimal);
                        }
                    }
                } else {
                    match_count += mo.len();
                    matching_objects.extend(mo);
                    eprint!(
                        "\rmatches/total {:>4}/{:<10} prefixes completed/total {:>4}/{:<4}",
                        match_count.to_formatted_string(&Locale::en),
                        total_objects
                            .load(Ordering::Relaxed)
                            .to_formatted_string(&Locale::en),
                        seen_prefixes.load(Ordering::Relaxed),
                        total_prefixes
                    );
                }
            }
            eprintln!();
            let mut objects = matching_objects;
            objects.sort_by(|a, b| a.key.cmp(&b.key));
            for obj in objects.iter() {
                if let Some(user_fmt) = &user_format {
                    print_user(&bucket, obj, user_fmt);
                } else {
                    print_default(obj, decimal);
                }
            }
            eprintln!(
                "Matched {}/{} objects across {} prefixes in {:?}",
                match_count,
                total_objects.load(Ordering::Relaxed),
                total_prefixes,
                Duration::from_millis(start.elapsed().as_millis() as u64)
            );
        }
        Command::Download { dest, .. } => {
            let mut matching_objects = Vec::new();
            let mut match_count = 0;
            while let Some(PrefixResult {
                matching_objects: mo,
                ..
            }) = rx.recv().await
            {
                match_count += mo.len();
                matching_objects.extend(mo);
                eprint!(
                    "\rmatches/total {:>4}/{:<10} prefixes completed/total {:>4}/{:<4}",
                    match_count.to_formatted_string(&Locale::en),
                    total_objects
                        .load(Ordering::Relaxed)
                        .to_formatted_string(&Locale::en),
                    seen_prefixes.load(Ordering::Relaxed),
                    total_prefixes
                );
            }
            eprintln!();
            let objects = matching_objects;
            let obj_count = objects.len();
            let base_path = Path::new(&dest);
            let mut total_bytes = 0_usize;
            for (i, obj) in objects.iter().enumerate() {
                let key = obj.key.as_ref().unwrap();
                let path = base_path.join(key);
                let dir = path.parent().unwrap();
                std::fs::create_dir_all(dir)
                    .with_context(|| format!("Creating directory: {}", dir.display()))?;
                let mut obj = client.get_object().bucket(&bucket).key(key).send().await?;
                let temp_path = path.with_extension(format!(".s3glob-tmp-{i}"));
                let mut file = std::fs::File::create(&temp_path)?;
                while let Some(bytes) = obj
                    .body
                    .try_next()
                    .await
                    .context("failed to read from S3 download stream")?
                {
                    file.write_all(&bytes).with_context(|| {
                        format!("failed to write to file: {}", &temp_path.display())
                    })?;
                    total_bytes += bytes.len();
                    eprint!(
                        "\rdownloaded {}/{} objects, {}",
                        i,
                        obj_count,
                        SizeFormatter::new(total_bytes as u64, decimal_format())
                    );
                }
                std::fs::rename(&temp_path, &path).with_context(|| {
                    format!(
                        "failed to rename file: {} -> {}",
                        &temp_path.display(),
                        &path.display()
                    )
                })?;

                eprint!(
                    "\rdownloaded {}/{} objects, {}",
                    i + 1,
                    obj_count,
                    SizeFormatter::new(total_bytes as u64, decimal_format())
                );
            }
            eprintln!();
        }
        Command::Parallelism { .. } => {
            eprintln!("This is just for documentation, run instead: s3glob help parallelism");
        }
    }

    Ok(())
}

#[derive(Debug)]
struct PrefixResult {
    #[allow(dead_code)]
    prefix: String,
    matching_objects: Vec<Object>,
}

/// Create a new S3 client with region auto-detection
async fn create_s3_client(opts: &Opts, bucket: &String) -> Result<Client> {
    let region = RegionProviderChain::first_try(Region::new(opts.region.clone()));
    let mut config = aws_config::defaults(BehaviorVersion::v2024_03_28()).region(region);
    if opts.no_sign_request {
        config = config.no_credentials();
    }
    let config = config.load().await;
    let client = Client::new(&config);

    let res = client.head_bucket().bucket(bucket).send().await;

    let bucket_region = match res {
        Ok(_) => return Ok(client),
        Err(err) => err
            .raw_response()
            .and_then(|res| res.headers().get("x-amz-bucket-region"))
            .map(str::to_owned)
            .ok_or_else(|| anyhow!(err).context("failed to extract bucket region"))?,
    };

    let region = Region::new(bucket_region);

    let mut config = aws_config::defaults(BehaviorVersion::v2024_03_28()).region(region);
    if opts.no_sign_request {
        config = config.no_credentials();
    }
    let config = config.load().await;
    let client = Client::new(&config);
    Ok(client)
}

fn decimal_format() -> FormatSizeOptions {
    FormatSizeOptions::from(DECIMAL)
        .decimal_places(1)
        .space_after_value(false)
}

#[derive(Debug)]
enum FormatToken {
    Literal(String),
    Variable(fn(&str, &Object) -> String),
}

fn compile_format(format: &str) -> Result<Vec<FormatToken>> {
    let mut char_iter = format.chars();
    let mut tokens = Vec::new();
    let mut current_literal = String::new();
    while let Some(char) = char_iter.next() {
        if char == '{' {
            if !current_literal.is_empty() {
                tokens.push(FormatToken::Literal(current_literal.clone()));
                current_literal.clear();
            }
            let mut var = String::new();
            for c in char_iter.by_ref() {
                if c == '}' {
                    break;
                }
                var.push(c);
            }
            match var.as_str() {
                "key" => tokens.push(FormatToken::Variable(|_, obj| {
                    obj.key.as_ref().unwrap().to_string()
                })),
                "uri" => tokens.push(FormatToken::Variable(|bucket, obj| {
                    format!("s3://{}/{}", bucket, obj.key.as_ref().unwrap())
                })),
                "size_bytes" => tokens.push(FormatToken::Variable(|_, obj| {
                    obj.size.unwrap_or(0).to_string()
                })),
                "size_human" => tokens.push(FormatToken::Variable(|_, obj| {
                    SizeFormatter::new(obj.size.unwrap_or(0) as u64, decimal_format()).to_string()
                })),
                "last_modified" => tokens.push(FormatToken::Variable(|_, obj| {
                    obj.last_modified.as_ref().unwrap().to_string()
                })),
                _ => return Err(anyhow::anyhow!("unknown variable: {}", var)),
            }
        } else {
            current_literal.push(char);
        }
    }
    if !current_literal.is_empty() {
        tokens.push(FormatToken::Literal(current_literal.clone()));
    }
    Ok(tokens)
}

fn print_default(obj: &Object, format: FormatSizeOptions) {
    println!(
        "{:>10}   {:>7}   {}",
        obj.last_modified
            .as_ref()
            .map(|dt| dt.to_string())
            .unwrap_or_default(),
        SizeFormatter::new(obj.size.unwrap_or(0) as u64, format).to_string(),
        obj.key.as_ref().unwrap_or(&String::new()),
    );
}

fn print_user(bucket: &str, obj: &Object, tokens: &[FormatToken]) {
    println!("{}", format_user(bucket, obj, tokens));
}

fn format_user(bucket: &str, obj: &Object, tokens: &[FormatToken]) -> String {
    let mut result = String::new();
    for token in tokens {
        match token {
            FormatToken::Literal(lit) => result.push_str(lit),
            FormatToken::Variable(var) => result.push_str(&var(bucket, obj)),
        }
    }
    result
}

fn add_atomic(atomic: &AtomicUsize, value: usize) -> usize {
    atomic.fetch_add(value, Ordering::Relaxed);
    atomic.load(Ordering::Relaxed)
}

async fn list_matching_objects(
    client: &Client,
    bucket: &str,
    prefix: &str,
    matcher: &S3GlobMatcher,
    total_objects: Arc<AtomicUsize>,
    tx: UnboundedSender<PrefixResult>,
) -> Result<()> {
    let mut paginator = client
        .list_objects_v2()
        .bucket(bucket)
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
            tx.send(PrefixResult {
                prefix: prefix.to_string(),
                matching_objects,
            })?;
        }
    }
    Ok(())
}

fn log_directive(loglevel: u8) -> Option<&'static str> {
    match loglevel {
        0 => None,
        1 => Some("s3glob=debug"),
        2 => Some("s3glob=trace"),
        _ => Some("trace"),
    }
}

pub(crate) fn setup_logging(directive: Option<&str>) {
    let mut env_filter = tracing_subscriber::EnvFilter::new("s3glob=warn");
    if let Ok(env) = std::env::var("S3GLOB_LOG") {
        env_filter = env_filter.add_directive(env.parse().unwrap());
    } else if let Ok(env) = std::env::var("RUST_LOG") {
        env_filter = env_filter.add_directive(env.parse().unwrap());
    }
    if let Some(directive) = directive {
        env_filter = env_filter.add_directive(directive.parse().unwrap());
    }

    let use_ansi = std::io::stderr().is_terminal()
        || std::env::var("CLICOLOR").is_ok_and(|v| ["1", "true"].contains(&v.as_str()))
        || std::env::var("CLICOLOR_FORCE").is_ok_and(|v| ["1", "true"].contains(&v.as_str()));

    tracing_subscriber::fmt()
        .with_target(false)
        .with_file(true)
        .with_line_number(true)
        .with_ansi(use_ansi)
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .init();
}

#[cfg(test)]
mod tests {
    use aws_sdk_s3::types::Object;
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("Size: {size_bytes}, Name: {key}", "Size: 1234, Name: test/file.txt")]
    #[case("s: {size_human}\t{key}", "s: 1.2kB\ttest/file.txt")]
    #[case("uri: {uri}", "uri: s3://bkt/test/file.txt")]
    #[trace]
    fn test_compile_format(#[case] format: &str, #[case] expected: &str) {
        let fmt = compile_format(format).unwrap();

        let object = Object::builder().key("test/file.txt").size(1234).build();

        let result = format_user("bkt", &object, &fmt);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_format_invalid_variable() {
        assert!(compile_format("{invalid_var}").is_err());
    }
}
