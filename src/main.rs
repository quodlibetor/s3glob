use std::io::IsTerminal as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow, bail};
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::primitives::DateTime;
use aws_sdk_s3::types::Object;
use aws_sdk_s3::{Client, config::BehaviorVersion, config::Region};
use clap::{ArgAction, Parser, Subcommand, ValueEnum};
use glob_matcher::{ListResult, PrefixResult, S3Engine, S3GlobMatcher};
use humansize::{DECIMAL, FormatSizeOptions, SizeFormatter};
use messaging::{MESSAGE_LEVEL, MessageLevel};
use num_format::{Locale, ToFormattedString};
use regex::Regex;
use tokio::runtime::Runtime;
use tracing::debug;

mod download;
mod glob_matcher;
mod messaging;

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

        /// Control how S3 object keys are mapped to local file paths
        ///
        /// - absolute | abs: the full key path will be reproduced in the
        ///   destination
        /// - from-first-glob | g: the key path relative to the first path part
        ///   containing a glob in the pattern will be reproduced in the
        ///   destination
        /// - shortest | s: the shortest path that can be made without conflicts.
        ///   This strips the longest common directory prefix from the key path.
        #[clap(short, long, verbatim_doc_comment, default_value = "from-first-glob")]
        path_mode: PathMode,

        /// Flatten the downloaded files into a single directory
        ///
        /// This will replace all slashes in the key path with dashes in the
        /// downloaded file.
        #[clap(long)]
        flatten: bool,
    },

    /// Learn how to tune s3glob's parallelism for better performance
    ///
    /// You only need to read this doc if you feel like s3glob is running
    /// slower than you hope, or if you're getting a slowdown error.
    ///
    /// If you want to limit parallel API calls, you can use the
    /// --max-parallelism flag.
    ///
    /// You probably want the maximum parallelism possible. Because of the
    /// APIs provided by AWS, s3glob can only meaningfully issue parallel
    /// requests for prefixes. Additionally, prefixes can only be generated
    /// before a delimiter.
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PathMode {
    Abs,
    Absolute,
    G,
    FromFirstGlob,
    S,
    Shortest,
}

impl ValueEnum for PathMode {
    fn value_variants<'a>() -> &'a [Self] {
        &[
            PathMode::Absolute,
            PathMode::Abs,
            PathMode::FromFirstGlob,
            PathMode::G,
            PathMode::S,
            PathMode::Shortest,
        ]
    }

    fn from_str(s: &str, _ignore_case: bool) -> Result<Self, String> {
        match s {
            "absolute" | "abs" => Ok(PathMode::Absolute),
            "from-first-glob" | "g" => Ok(PathMode::FromFirstGlob),
            "shortest" | "s" => Ok(PathMode::Shortest),
            _ => Err(format!("invalid path type: {}", s)),
        }
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        match self {
            PathMode::Abs => Some(clap::builder::PossibleValue::new("abs")),
            PathMode::Absolute => Some(clap::builder::PossibleValue::new("absolute")),
            PathMode::FromFirstGlob => Some(clap::builder::PossibleValue::new("from-first-glob")),
            PathMode::G => Some(clap::builder::PossibleValue::new("g")),
            PathMode::Shortest => Some(clap::builder::PossibleValue::new("shortest")),
            PathMode::S => Some(clap::builder::PossibleValue::new("s")),
        }
    }
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

    /// Be more quiet, specify multiple times to increase quietness
    ///
    /// - `-q` will not show progress messages, only errors
    /// - `-qq` will not even show error messages
    ///
    /// This overrides the --verbose flag if both are set.
    #[clap(short, long, global = true, action = ArgAction::Count, verbatim_doc_comment)]
    quiet: u8,

    /// Do not provide your credentials when issuing requests
    ///
    /// This is useful for downloading objects from a bucket that is not
    /// associated with your AWS account, such as a public bucket.
    #[clap(long, global = true, alias = "no-sign-requests")]
    no_sign_request: bool,

    /// Maximum number of parallel requests to make
    ///
    /// If you get a slowdown error you can use this to limit the number of
    /// concurrent requests.
    #[clap(short = 'M', long, global = true, default_value = "10000")]
    max_parallelism: usize,
}

fn main() {
    let opts = Opts::parse();
    setup_logging(log_directive(opts.verbose, opts.quiet));
    if opts.quiet == 1 {
        MESSAGE_LEVEL.get_or_init(|| MessageLevel::Quiet);
    } else if opts.quiet >= 2 {
        MESSAGE_LEVEL.get_or_init(|| MessageLevel::VeryQuiet);
    }
    debug!(?opts, "parsed options");

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        if let Err(err) = run(opts).await {
            // TODO: Separate user error from internal error?
            message_err!("Error: {}", err);
            let mut err = err.source();
            let mut count = 0;
            let mut prev_msg = String::new();
            while let Some(e) = err {
                if e.to_string() != prev_msg {
                    message_err!("  : {}", e);
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
            progressln!("This is just for documentation, run instead: s3glob help parallelism");
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

    let engine = S3Engine::new(client.clone(), bucket.clone());
    let mut matcher = S3GlobMatcher::parse(raw_pattern.clone(), &opts.delimiter.to_string())?;
    matcher.set_max_parallelism(opts.max_parallelism);
    let ListResult {
        status,
        totals,
        mut rx,
    } = matcher.get_objects(engine).await?;

    match opts.command {
        Command::List { format, stream, .. } => {
            let user_format = if let Some(user_fmt) = format {
                Some(compile_format(&user_fmt)?)
            } else {
                None
            };
            let mut matching_objects: Vec<PrefixResult> = Vec::new();
            let mut match_count = 0;
            let decimal = decimal_format();
            while let Some(results) = rx.recv().await {
                if stream {
                    match_count += results.len();
                    for result in results {
                        print_prefix_result(&bucket, &user_format, decimal, result);
                    }
                } else {
                    match_count += results.len();
                    matching_objects.extend(results);
                    if !matcher.is_complete() {
                        progress!(
                            "\rmatches/total {:>4}/{:<10} prefixes completed/total {:>4}/{:<4}",
                            match_count.to_formatted_string(&Locale::en),
                            status
                                .total_objects
                                .load(Ordering::Relaxed)
                                .to_formatted_string(&Locale::en),
                            status.seen_prefixes.load(Ordering::Relaxed),
                            totals.total_prefixes
                        );
                    }
                }
            }
            progressln!();
            let mut objects = matching_objects;
            objects.sort_by_key(|r| r.key().to_owned());
            for obj in objects {
                print_prefix_result(&bucket, &user_format, decimal, obj);
            }
            progressln!(
                "Matched {}/{} objects across {} prefixes in {:?}",
                match_count,
                status
                    .total_objects
                    .load(Ordering::Relaxed)
                    .max(totals.max_objects_observed),
                totals.max_prefixes_observed,
                Duration::from_millis(start.elapsed().as_millis() as u64)
            );
        }
        Command::Download {
            dest,
            path_mode,
            flatten,
            ..
        } => {
            let mut total_matches = 0;
            let pools = download::DlPools::new(opts.max_parallelism);
            let prefix_to_strip = download::extract_prefix_to_strip(&raw_pattern, path_mode, &[]);
            let (ntfctn_tx, mut ntfctn_rx) =
                tokio::sync::mpsc::unbounded_channel::<download::Notification>();
            let base_path = PathBuf::from(dest);
            let dl = download::Downloader::new(
                client.clone(),
                bucket.clone(),
                prefix_to_strip,
                flatten,
                base_path.clone(),
                ntfctn_tx.clone(),
            );
            // if the path_mode is shortes then we need to know all the paths to be able to extract the shortest
            let mut objects_to_download = Vec::new();
            while let Some(result) = rx.recv().await {
                total_matches += result
                    .iter()
                    .filter(|r| matches!(r, PrefixResult::Object(_)))
                    .count();
                for obj in result {
                    match obj {
                        PrefixResult::Object(obj) => {
                            if matches!(path_mode, PathMode::Shortest | PathMode::S) {
                                objects_to_download.push(obj);
                            } else {
                                pools.download_object(dl.fresh(), obj);
                            }
                        }
                        PrefixResult::Prefix(prefix) => {
                            debug!("Skipping prefix: {}", prefix);
                        }
                    }
                }
                if !matcher.is_complete() {
                    progress!(
                        "\rmatches/total {:>4}/{:<10} prefixes completed/total {:>4}/{:<4}",
                        total_matches.to_formatted_string(&Locale::en),
                        status
                            .total_objects
                            .load(Ordering::Relaxed)
                            .to_formatted_string(&Locale::en),
                        status.seen_prefixes.load(Ordering::Relaxed),
                        totals.total_prefixes
                    );
                }
            }
            if !matcher.is_complete() {
                progressln!();
            }
            // close the tx so the downloaders know to finish
            drop(dl);
            drop(pools);
            if matches!(path_mode, PathMode::Shortest | PathMode::S) {
                let prefix_to_strip = download::extract_prefix_to_strip(
                    &raw_pattern,
                    path_mode,
                    &objects_to_download,
                );
                progressln!(
                    "Stripping longest common prefix from keys: {}",
                    prefix_to_strip
                );
                let dl = download::Downloader::new(
                    client,
                    bucket,
                    prefix_to_strip,
                    flatten,
                    base_path,
                    ntfctn_tx,
                );
                let pools = download::DlPools::new(opts.max_parallelism);
                for obj in objects_to_download {
                    pools.download_object(dl.fresh(), obj);
                }
            } else {
                progressln!();
                drop(ntfctn_tx);
            }
            let start_time = Instant::now();
            let mut downloaded_matches = 0;
            let mut total_bytes = 0_usize;
            let mut speed = 0.0;
            let mut files = Vec::with_capacity(total_matches);
            while let Some(n) = ntfctn_rx.recv().await {
                match n {
                    download::Notification::ObjectDownloaded(path) => {
                        downloaded_matches += 1;
                        files.push(path.display().to_string());
                    }
                    download::Notification::BytesDownloaded(bytes) => {
                        total_bytes += bytes;
                    }
                }
                let elapsed = start_time.elapsed().as_secs_f64();
                speed = total_bytes as f64 / elapsed;
                progress!(
                    "\rdownloaded {}/{} objects, {:>7}   {:>10}/s",
                    downloaded_matches,
                    total_matches,
                    SizeFormatter::new(total_bytes as u64, decimal_format()).to_string(),
                    SizeFormatter::new(speed.round() as u64, decimal_format()).to_string(),
                );
            }
            if files.is_empty() {
                progressln!();
                bail!("No objects found matching the pattern.");
            }
            progressln!("\n");

            files.sort_unstable();
            for path in files {
                println!("{}", path);
            }
            let dl_ms = start_time.elapsed().as_millis() as u64;
            progressln!(
                "\ndiscovered {} objects in {:?} | downloaded {} in {:?} ({}/s)",
                downloaded_matches,
                Duration::from_millis(start.elapsed().as_millis() as u64 - dl_ms),
                SizeFormatter::new(total_bytes as u64, decimal_format()),
                Duration::from_millis(dl_ms),
                SizeFormatter::new(speed.round() as u64, decimal_format()),
            );
        }
        Command::Parallelism { .. } => {
            progressln!("This is just for documentation, run instead: s3glob help parallelism");
        }
    }

    Ok(())
}

fn print_prefix_result(
    bucket: &str,
    user_format: &Option<Vec<FormatToken>>,
    decimal: FormatSizeOptions,
    result: PrefixResult,
) {
    match result {
        PrefixResult::Object(obj) => {
            if let Some(user_fmt) = user_format {
                print_user(bucket, &obj, user_fmt);
            } else {
                print_default(&obj, decimal);
            }
        }
        PrefixResult::Prefix(prefix) => {
            // TODO: support user-customizable prefix formatting too?
            println!("PRE     {prefix}");
        }
    }
}

#[derive(Debug)]
struct S3Object {
    key: String,
    size: i64,
    last_modified: DateTime,
}

impl From<Object> for S3Object {
    fn from(obj: Object) -> Self {
        Self {
            key: obj.key.unwrap(),
            size: obj.size.unwrap_or(0),
            last_modified: obj
                .last_modified
                .unwrap_or_else(|| DateTime::from_millis(0)),
        }
    }
}

impl S3Object {
    fn from_head_object(key: String, obj: HeadObjectOutput) -> Self {
        Self {
            key,
            size: obj.content_length().unwrap(),
            last_modified: obj.last_modified.unwrap(),
        }
    }
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
    Variable(fn(&str, &S3Object) -> String),
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
                "key" => tokens.push(FormatToken::Variable(|_, obj| obj.key.clone())),
                "uri" => tokens.push(FormatToken::Variable(|bucket, obj| {
                    format!("s3://{}/{}", bucket, obj.key)
                })),
                "size_bytes" => tokens.push(FormatToken::Variable(|_, obj| obj.size.to_string())),
                "size_human" => tokens.push(FormatToken::Variable(|_, obj| {
                    SizeFormatter::new(obj.size as u64, decimal_format()).to_string()
                })),
                "last_modified" => tokens.push(FormatToken::Variable(|_, obj| {
                    obj.last_modified.to_string()
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

fn print_default(obj: &S3Object, format: FormatSizeOptions) {
    println!(
        "{:>10}   {:>7}   {}",
        obj.last_modified,
        SizeFormatter::new(obj.size as u64, format).to_string(),
        obj.key,
    );
}

fn print_user(bucket: &str, obj: &S3Object, tokens: &[FormatToken]) {
    println!("{}", format_user(bucket, obj, tokens));
}

fn format_user(bucket: &str, obj: &S3Object, tokens: &[FormatToken]) -> String {
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

fn log_directive(loglevel: u8, quiet: u8) -> Option<&'static str> {
    if quiet >= 2 {
        return Some("s3glob=error");
    }
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
    #![allow(clippy::comparison_to_empty)]
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

        let result = format_user("bkt", &S3Object::from(object), &fmt);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_format_invalid_variable() {
        assert!(compile_format("{invalid_var}").is_err());
    }
}
