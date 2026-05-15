use std::io::{self, IsTerminal as _, Write as _};
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
use serde::Serialize;
use tokio::runtime::Runtime;
use tracing::debug;

mod download;
mod glob_matcher;
mod messaging;
mod platform_tls;
mod progress;

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
        /// - `{kind}`: the kind of the result.
        ///   Either "OBJ" (if it is an object) or "PRE" (if it is a prefix)
        /// - `{key}`: the key of the object
        /// - `{bucket}`: the bucket name
        /// - `{uri}`: the s3 uri of the object, e.g. s3://my-bucket/my-object.txt
        /// - `{size_bytes}`: the size of the object in bytes, with no suffix
        /// - `{size_human}`: the size of the object in a decimal format (e.g. 1.23MB)
        /// - `{last_modified}`: the last modified date of the object, RFC3339 format
        /// - `{etag}`: the object ETag
        /// - `{storage_class}`: STANDARD, GLACIER, etc.
        /// - `{restore_in_progress}`: "true"/"false" if the object is a Glacier
        ///   restore in progress, empty otherwise
        /// - `{restore_expiry}`: RFC3339 expiry of an active restore, empty otherwise
        /// - `{checksums}`: comma-separated list of additional-checksum algorithms
        ///
        /// For example, the default format looks as though you ran s3glob like this:
        ///
        ///     s3glob ls -f "{last_modified} {size_human} {key}" "my-bucket/*"
        ///
        /// Mutually exclusive with `--output json` and `--output ndjson`.
        #[clap(short, long, verbatim_doc_comment)]
        format: Option<String>,

        /// Stream keys as they are found, rather than sorting and printing at the end
        #[clap(long)]
        stream: bool,

        /// Output format: text|json|ndjson
        ///
        /// - `text` (default): one match per line, optionally formatted by --format
        /// - `json`: a single sorted JSON array of records (buffered until end)
        /// - `ndjson`: one JSON record per line, streamed in arrival order
        ///
        /// JSON records carry the full object metadata: type ("object" or "prefix"),
        /// bucket, key, uri, size, last_modified, etag, storage_class,
        /// checksum_algorithms, and restore_status.
        #[clap(short, long, verbatim_doc_comment, default_value = "text")]
        output: OutputFormat,
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

        /// Output format: text|json|ndjson
        ///
        /// - `text` (default): one local file path per line on stdout, summary on stderr
        /// - `json`: single buffered `{ "downloads": [...], "summary": {...} }` object
        /// - `ndjson`: streams `{ "event": "downloaded", ... }` per file then a final
        ///   `{ "event": "summary", ... }` record (summary moves to stdout)
        #[clap(short, long, verbatim_doc_comment, default_value = "text")]
        output: OutputFormat,
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
    /// requests for distinct prefixes. Prefixes come from two places:
    /// explicit glob components (`*`, `{a,b,c}`, `[abc]`) before a
    /// delimiter, and automatic expansion at `**`.
    ///
    /// At a `**` component s3glob runs a bounded breadth-first
    /// expansion: it walks one directory level at a time with `LIST`
    /// calls until it has enough sub-prefixes to scan in parallel.
    /// This typically helps for buckets with broad subtrees under
    /// `**`. If your bucket shape makes the expansion
    /// counter-productive (e.g. each level has only one sub-directory
    /// so the expansion just costs extra LISTs) pass
    /// `--no-recursive-auto-parallel` to skip the expansion and list
    /// the `**` parent directly.
    ///
    /// So if you have a keyspace (using {..-..} to represent a range) that
    /// looks like:
    ///
    ///    s3://bucket/{a-z}/{0-999}/OBJECT_ID.txt
    ///
    /// and you want to find all the text files where OBJECT_ID is 5, you have
    /// several options for patterns:
    ///
    ///    1: s3glob ls bucket/**/5.txt    -- relies on `**` auto-expansion
    ///    2: s3glob ls bucket/*/**/5.txt  -- parallelism >= 26
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[clap(rename_all = "lowercase")]
enum OutputFormat {
    Text,
    Json,
    Ndjson,
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

    /// Disable automatic parallelization of `**` (recursive) listings
    ///
    /// At a `**` glob component s3glob expands the frontier one
    /// directory level at a time to discover sub-prefixes it can list
    /// in parallel. This flag skips that expansion and lists the `**`
    /// parent serially.
    ///
    /// Use this if the expansion is firing too many `LIST` calls for
    /// your bucket's shape, or if you want predictable serial listing.
    #[clap(long, global = true, action = ArgAction::SetTrue)]
    no_recursive_auto_parallel: bool,

    /// Target prefix count for `**` BFS expansion (escape hatch)
    ///
    /// The expansion loop at `**` runs while the discovered prefix
    /// set is smaller than this. Lower to reduce expansion API
    /// calls, raise to fan out more aggressively, `0` to skip the
    /// loop entirely (`--no-recursive-auto-parallel` is the
    /// supported way to do that).
    #[clap(long, global = true, default_value = "25", hide = true)]
    min_prefixes: usize,

    /// Use path-style S3 addressing
    ///
    /// By default s3glob uses the standard virtualhost-style addressing,
    /// where the bucket name is prepended to the endpoint hostname
    /// (e.g. http://bucket.host/key).
    ///
    /// Use this flag when connecting to S3-compatible servers accessed by
    /// hostname (e.g. MinIO at http://my.local.server:9000) that do not
    /// support virtualhost-style addressing.
    #[clap(long, global = true)]
    force_path_style: bool,

    /// Allow `?` and `[!...]` to match the delimiter (default: true)
    ///
    /// When true (the default), `?` matches any single character including
    /// the delimiter, and a negated character class like `[!abc]` matches
    /// any single character not in the set, including the delimiter. This
    /// preserves the historical behavior of s3glob.
    ///
    /// Pass `--no-cross-delim` to make these patterns single-segment:
    /// `?` becomes "any single non-delimiter character" and `[!abc]`
    /// becomes "any single character not in the set and not the
    /// delimiter". `*` is always single-segment regardless of this flag.
    ///
    /// A future major release will flip the default to `false`.
    #[clap(
        long,
        global = true,
        default_value_t = true,
        env = "S3GLOB_CROSS_DELIM",
        action = ArgAction::Set,
        num_args = 0..=1,
        default_missing_value = "true",
        overrides_with = "no_cross_delim",
        verbatim_doc_comment,
    )]
    cross_delim: bool,

    /// Negated form of `--cross-delim`; setting this is equivalent to
    /// `--cross-delim=false`. Hidden because `--cross-delim` already
    /// documents both directions.
    #[clap(
        long = "no-cross-delim",
        global = true,
        action = ArgAction::SetTrue,
        hide = true,
        overrides_with = "cross_delim",
    )]
    no_cross_delim: bool,
}

impl Opts {
    /// Resolve the effective `cross_delim` value.
    ///
    /// `--no-cross-delim` and `--cross-delim` use clap's `overrides_with`
    /// so the last one wins on the command line; this combiner produces
    /// the final boolean by treating `--no-cross-delim` as the canonical
    /// "off" signal.
    fn cross_delim(&self) -> bool {
        if self.no_cross_delim {
            false
        } else {
            self.cross_delim
        }
    }
}

fn main() {
    let opts = Opts::parse();
    setup_logging(log_directive(opts.verbose, opts.quiet));
    let level = if opts.quiet >= 2 {
        MessageLevel::VeryQuiet
    } else if opts.quiet == 1 {
        MessageLevel::Quiet
    } else {
        MessageLevel::Normal
    };
    let _ = MESSAGE_LEVEL.set(level);
    progress::init(level);
    debug!(?opts, "parsed options");

    let rt = Runtime::new().expect("tokio runtime should create successfully");
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
    if let Command::List {
        format: Some(_),
        output,
        ..
    } = &opts.command
        && !matches!(output, OutputFormat::Text)
    {
        bail!("--format cannot be combined with --output json or --output ndjson");
    }
    let pat = match &opts.command {
        Command::List { pattern, .. } | Command::Download { pattern, .. } => pattern,
        Command::Parallelism { .. } => {
            progressln!("This is just for documentation, run instead: s3glob help parallelism");
            return Ok(());
        }
    };
    let s3re = Regex::new(r"^(?:s3://)?([^/]+)/(.*)").expect("Static regex is valid");
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
    let mut matcher = S3GlobMatcher::parse(
        raw_pattern.clone(),
        &opts.delimiter.to_string(),
        opts.cross_delim(),
    )?;
    matcher.set_max_parallelism(opts.max_parallelism);
    let effective_min_prefixes = if opts.no_recursive_auto_parallel {
        0
    } else {
        opts.min_prefixes
    };
    matcher.set_min_prefixes(effective_min_prefixes);
    let ListResult {
        status,
        totals,
        mut rx,
    } = matcher.get_objects(engine).await?;

    match opts.command {
        Command::List {
            format,
            stream,
            output,
            ..
        } => {
            let user_format = if let Some(user_fmt) = format {
                Some(compile_format(&user_fmt)?)
            } else {
                None
            };
            let stream_mode = match output {
                OutputFormat::Text => stream,
                OutputFormat::Ndjson => true,
                OutputFormat::Json => {
                    if stream {
                        progressln!(
                            "note: --stream ignored with --output json (records are buffered)"
                        );
                    }
                    false
                }
            };
            let mut matching_objects: Vec<PrefixResult> = Vec::new();
            // The matcher surfaces both real objects and logical prefixes
            // (directories) as matches; count them separately so the
            // summary doesn't report directories as "objects".
            let mut object_count = 0;
            let mut prefix_count = 0;
            let decimal = decimal_format();
            let matches_progress = if !matcher.is_complete() {
                Some(progress::get().spinner(progress::matches_spinner_style()))
            } else {
                None
            };
            let mut stdout = io::stdout().lock();
            'recv: while let Some(results) = rx.recv().await {
                for result in &results {
                    match result {
                        PrefixResult::Object(_) => object_count += 1,
                        PrefixResult::Prefix(_) => prefix_count += 1,
                    }
                }
                if stream_mode {
                    for result in &results {
                        let written = match output {
                            OutputFormat::Text => write_prefix_result(
                                &mut stdout,
                                &bucket,
                                &user_format,
                                decimal,
                                result,
                            ),
                            OutputFormat::Ndjson => write_json_line(
                                &mut stdout,
                                &JsonLsRecord::from_result(&bucket, result),
                            ),
                            OutputFormat::Json => unreachable!(),
                        };
                        if !keep_writing(written)? {
                            break 'recv;
                        }
                    }
                } else {
                    matching_objects.extend(results);
                }
                if let Some(matches_progress) = &matches_progress {
                    let total_objects = status.total_objects.load(Ordering::Relaxed);
                    matches_progress.set_message(format!(
                        "{:>4}/{:<10}",
                        (object_count + prefix_count).to_formatted_string(&Locale::en),
                        total_objects.to_formatted_string(&Locale::en),
                    ));
                    matches_progress.set_prefix(format!(
                        "{:>4}/{:<4}",
                        status.seen_prefixes.load(Ordering::Relaxed),
                        totals.total_prefixes,
                    ));
                }
            }
            // Done receiving. Dropping the receiver lets the matcher's senders
            // fail fast if we broke out early on a closed pipe. In-flight S3
            // list calls still finish their current page, but nothing new is
            // queued.
            drop(rx);
            if let Some(matches_progress) = &matches_progress {
                matches_progress.finish_and_clear();
            }
            if !stream_mode {
                let mut objects = matching_objects;
                objects.sort_by_key(|r| r.key().to_owned());
                match output {
                    OutputFormat::Text => {
                        for obj in &objects {
                            if !keep_writing(write_prefix_result(
                                &mut stdout,
                                &bucket,
                                &user_format,
                                decimal,
                                obj,
                            ))? {
                                break;
                            }
                        }
                    }
                    OutputFormat::Json => {
                        let records: Vec<JsonLsRecord> = objects
                            .iter()
                            .map(|r| JsonLsRecord::from_result(&bucket, r))
                            .collect();
                        keep_writing(write_json_line(&mut stdout, &records))?;
                    }
                    OutputFormat::Ndjson => unreachable!(),
                }
            }
            let elapsed = Duration::from_millis(start.elapsed().as_millis() as u64);
            let matched = object_count + prefix_count;
            let candidates = totals
                .max_candidate_prefixes
                .max(status.total_objects.load(Ordering::Relaxed))
                .max(matched);
            if candidates > matched {
                progressln!(
                    "Matched {} objects and {} prefixes out of {} candidates in {:?}",
                    object_count,
                    prefix_count,
                    candidates,
                    elapsed,
                );
            } else {
                progressln!(
                    "Matched {} objects and {} prefixes in {:?}",
                    object_count,
                    prefix_count,
                    elapsed,
                );
            }
        }
        Command::Download {
            dest,
            path_mode,
            flatten,
            output,
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
            let matches_progress = if !matcher.is_complete() {
                Some(progress::get().spinner(progress::matches_spinner_style()))
            } else {
                None
            };
            // if the path_mode is shortest then we need to know all the paths to be able to extract the shortest
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
                if let Some(matches_progress) = &matches_progress {
                    let total_objects = status.total_objects.load(Ordering::Relaxed);
                    matches_progress.set_message(format!(
                        "{:>4}/{:<10}",
                        total_matches.to_formatted_string(&Locale::en),
                        total_objects.to_formatted_string(&Locale::en),
                    ));
                    matches_progress.set_prefix(format!(
                        "{:>4}/{:<4}",
                        status.seen_prefixes.load(Ordering::Relaxed),
                        totals.total_prefixes,
                    ));
                }
            }
            if let Some(matches_progress) = matches_progress {
                matches_progress.finish_and_clear();
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
                    bucket.clone(),
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
                drop(ntfctn_tx);
            }
            let start_time = Instant::now();
            let mut downloaded_matches = 0;
            let mut total_bytes = 0_usize;
            let mut speed = 0.0;
            let mut records: Vec<DownloadedRecord> = Vec::with_capacity(total_matches);
            let downloads_progress = progress::get().spinner(progress::downloads_count_style());
            downloads_progress.set_length(total_matches as u64);
            let bytes_progress = progress::get().bar(progress::downloads_bytes_style());
            let mut ndjson_stdout =
                matches!(output, OutputFormat::Ndjson).then(|| io::stdout().lock());
            while let Some(n) = ntfctn_rx.recv().await {
                match n {
                    download::Notification::ObjectDownloaded { object, local_path } => {
                        downloaded_matches += 1;
                        downloads_progress.set_position(downloaded_matches as u64);
                        let record = DownloadedRecord { object, local_path };
                        if let Some(out) = &mut ndjson_stdout {
                            let event = JsonDlEvent::Downloaded {
                                record: JsonDlObject::new(&bucket, &record),
                            };
                            if !keep_writing(write_json_line(out, &event))? {
                                ndjson_stdout = None;
                            }
                        }
                        records.push(record);
                    }
                    download::Notification::BytesDownloaded(bytes) => {
                        total_bytes += bytes;
                        bytes_progress.set_position(total_bytes as u64);
                    }
                }
                let elapsed = start_time.elapsed().as_secs_f64();
                speed = total_bytes as f64 / elapsed;
            }
            downloads_progress.finish_and_clear();
            bytes_progress.finish_and_clear();
            if records.is_empty() {
                bail!("No objects found matching the pattern.");
            }
            let dl_ms = start_time.elapsed().as_millis() as u64;
            let summary = JsonDlSummary {
                bytes: total_bytes,
                discovery_ms: start_time.duration_since(start).as_millis() as u64,
                download_ms: dl_ms,
                bytes_per_sec: speed.round() as u64,
            };
            match output {
                OutputFormat::Text => {
                    let mut files: Vec<String> = records
                        .iter()
                        .map(|r| r.local_path.display().to_string())
                        .collect();
                    files.sort_unstable();
                    let mut stdout = io::stdout().lock();
                    for path in &files {
                        if !keep_writing(writeln!(stdout, "{}", path))? {
                            break;
                        }
                    }
                    progressln!(
                        "discovered {} objects in {:?} | downloaded {} in {:?} ({}/s)",
                        downloaded_matches,
                        Duration::from_millis(summary.discovery_ms),
                        SizeFormatter::new(total_bytes as u64, decimal_format()),
                        Duration::from_millis(dl_ms),
                        SizeFormatter::new(speed.round() as u64, decimal_format()),
                    );
                }
                OutputFormat::Ndjson => {
                    if let Some(mut out) = ndjson_stdout {
                        let event = JsonDlEvent::Summary { record: &summary };
                        keep_writing(write_json_line(&mut out, &event))?;
                    }
                }
                OutputFormat::Json => {
                    records.sort_by(|a, b| a.object.key.cmp(&b.object.key));
                    let downloads: Vec<JsonDlObject<'_>> = records
                        .iter()
                        .map(|r| JsonDlObject::new(&bucket, r))
                        .collect();
                    let wrapper = JsonDlWrapper {
                        downloads,
                        summary: &summary,
                    };
                    let mut stdout = io::stdout().lock();
                    keep_writing(write_json_line(&mut stdout, &wrapper))?;
                }
            }
        }
        Command::Parallelism { .. } => {
            progressln!("This is just for documentation, run instead: s3glob help parallelism");
        }
    }

    Ok(())
}

fn write_prefix_result(
    stdout: &mut io::StdoutLock<'_>,
    bucket: &str,
    user_format: &Option<Vec<FormatToken>>,
    decimal: FormatSizeOptions,
    result: &PrefixResult,
) -> io::Result<()> {
    if let Some(user_fmt) = user_format {
        writeln!(stdout, "{}", format_user(bucket, result, user_fmt))
    } else {
        match result {
            PrefixResult::Object(obj) => writeln!(
                stdout,
                "{:>10}   {:>7}   {}",
                obj.last_modified,
                SizeFormatter::new(obj.size as u64, decimal).to_string(),
                obj.key,
            ),
            PrefixResult::Prefix(prefix) => writeln!(stdout, "PRE     {prefix}"),
        }
    }
}

fn s3_uri(bucket: &str, key: &str) -> String {
    format!("s3://{bucket}/{key}")
}

fn fmt_rfc3339(d: &DateTime) -> String {
    d.fmt(aws_sdk_s3::primitives::DateTimeFormat::DateTime)
        .unwrap_or_default()
}

fn write_json_line<W: io::Write, T: Serialize>(w: &mut W, value: &T) -> io::Result<()> {
    serde_json::to_writer(&mut *w, value).map_err(io::Error::other)?;
    w.write_all(b"\n")
}

/// Per-object fields shared by ls and dl JSON outputs.
#[derive(Serialize)]
struct ObjectMetadata<'a> {
    key: &'a str,
    uri: String,
    size: i64,
    last_modified: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    etag: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    storage_class: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    checksum_algorithms: Option<&'a [String]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    restore_status: Option<&'a RestoreStatus>,
}

impl<'a> ObjectMetadata<'a> {
    fn new(bucket: &str, obj: &'a S3Object) -> Self {
        Self {
            key: &obj.key,
            uri: s3_uri(bucket, &obj.key),
            size: obj.size,
            last_modified: fmt_rfc3339(&obj.last_modified),
            etag: obj.etag.as_deref(),
            storage_class: obj.storage_class.as_deref(),
            checksum_algorithms: obj.checksum_algorithms.as_deref(),
            restore_status: obj.restore_status.as_ref(),
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum JsonLsRecord<'a> {
    Object {
        bucket: &'a str,
        #[serde(flatten)]
        meta: ObjectMetadata<'a>,
    },
    Prefix {
        bucket: &'a str,
        key: &'a str,
        uri: String,
    },
}

impl<'a> JsonLsRecord<'a> {
    fn from_result(bucket: &'a str, result: &'a PrefixResult) -> Self {
        match result {
            PrefixResult::Object(obj) => JsonLsRecord::Object {
                bucket,
                meta: ObjectMetadata::new(bucket, obj),
            },
            PrefixResult::Prefix(prefix) => JsonLsRecord::Prefix {
                bucket,
                key: prefix,
                uri: s3_uri(bucket, prefix),
            },
        }
    }
}

#[derive(Debug)]
struct DownloadedRecord {
    object: S3Object,
    local_path: PathBuf,
}

#[derive(Serialize)]
struct JsonDlObject<'a> {
    bucket: &'a str,
    #[serde(flatten)]
    meta: ObjectMetadata<'a>,
    local_path: String,
}

impl<'a> JsonDlObject<'a> {
    fn new(bucket: &'a str, rec: &'a DownloadedRecord) -> Self {
        Self {
            bucket,
            meta: ObjectMetadata::new(bucket, &rec.object),
            local_path: rec.local_path.display().to_string(),
        }
    }
}

#[derive(Serialize)]
struct JsonDlSummary {
    bytes: usize,
    discovery_ms: u64,
    download_ms: u64,
    bytes_per_sec: u64,
}

#[derive(Serialize)]
struct JsonDlWrapper<'a> {
    downloads: Vec<JsonDlObject<'a>>,
    summary: &'a JsonDlSummary,
}

#[derive(Serialize)]
#[serde(tag = "event", rename_all = "lowercase")]
enum JsonDlEvent<'a> {
    Downloaded {
        #[serde(flatten)]
        record: JsonDlObject<'a>,
    },
    Summary {
        #[serde(flatten)]
        record: &'a JsonDlSummary,
    },
}

/// Classify the result of a write to stdout.
///
/// Returns `Ok(true)` to keep writing, `Ok(false)` when the reader has gone
/// away, and `Err` for an unknown write error.
fn keep_writing(result: io::Result<()>) -> Result<bool> {
    match result {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::BrokenPipe => Ok(false),
        Err(e) => Err(e.into()),
    }
}

#[derive(Debug)]
struct S3Object {
    key: String,
    size: i64,
    last_modified: DateTime,
    etag: Option<String>,
    storage_class: Option<String>,
    checksum_algorithms: Option<Vec<String>>,
    restore_status: Option<RestoreStatus>,
}

#[derive(Debug, Clone, Serialize)]
struct RestoreStatus {
    in_progress: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    expiry: Option<String>,
}

/// S3 returns ETag as a quoted string per the HTTP entity-tag grammar
/// (e.g. `"d41d8cd98f00b204e9800998ecf8427e"`). Strip the syntactic quotes
/// so consumers get the bare hash to compare against.
fn unquote_etag(s: String) -> String {
    s.trim_matches('"').to_owned()
}

impl From<Object> for S3Object {
    fn from(obj: Object) -> Self {
        Self {
            key: obj.key.expect("Object key is always present"),
            size: obj.size.unwrap_or(0),
            last_modified: obj
                .last_modified
                .unwrap_or_else(|| DateTime::from_millis(0)),
            etag: obj.e_tag.map(unquote_etag),
            storage_class: obj.storage_class.map(|s| s.as_str().to_owned()),
            checksum_algorithms: obj
                .checksum_algorithm
                .filter(|v| !v.is_empty())
                .map(|v| v.into_iter().map(|a| a.as_str().to_owned()).collect()),
            restore_status: obj.restore_status.map(|rs| RestoreStatus {
                in_progress: rs.is_restore_in_progress.unwrap_or(false),
                expiry: rs.restore_expiry_date.map(|d| {
                    d.fmt(aws_sdk_s3::primitives::DateTimeFormat::DateTime)
                        .unwrap_or_default()
                }),
            }),
        }
    }
}

impl S3Object {
    fn from_head_object(key: String, obj: HeadObjectOutput) -> Self {
        Self {
            key,
            size: obj.content_length().expect("Content length is present"),
            last_modified: obj.last_modified.unwrap(),
            etag: obj.e_tag.map(unquote_etag),
            storage_class: obj.storage_class.map(|s| s.as_str().to_owned()),
            checksum_algorithms: None,
            restore_status: None,
        }
    }
}

/// Create a new S3 client with region auto-detection
async fn create_s3_client(opts: &Opts, bucket: &String) -> Result<Client> {
    let region = RegionProviderChain::first_try(Region::new(opts.region.clone()));
    let mut config = aws_config::defaults(BehaviorVersion::latest()).region(region);
    if opts.no_sign_request {
        config = config.no_credentials();
    }
    if std::env::var("EXPERIMENTAL_PLATFORM_TLS")
        .is_ok_and(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
    {
        config = config.http_client(platform_tls::build());
    }
    let config = config.load().await;
    let client = build_s3_client(&config, opts.force_path_style);

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

    let mut config = aws_config::defaults(BehaviorVersion::latest()).region(region);
    if opts.no_sign_request {
        config = config.no_credentials();
    }
    if std::env::var("EXPERIMENTAL_PLATFORM_TLS")
        .is_ok_and(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
    {
        config = config.http_client(platform_tls::build());
    }
    let config = config.load().await;
    let client = build_s3_client(&config, opts.force_path_style);
    Ok(client)
}

fn build_s3_client(config: &aws_config::SdkConfig, force_path_style: bool) -> Client {
    Client::from_conf(
        aws_sdk_s3::config::Builder::from(config)
            .force_path_style(force_path_style)
            .build(),
    )
}

fn decimal_format() -> FormatSizeOptions {
    FormatSizeOptions::from(DECIMAL)
        .decimal_places(1)
        .space_after_value(false)
}

#[derive(Debug)]
enum FormatToken {
    Literal(String),
    Variable(fn(&str, &PrefixResult) -> String),
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
                "kind" => tokens.push(FormatToken::Variable(|_, obj| obj.kind())),
                "bucket" => tokens.push(FormatToken::Variable(|bucket, _| bucket.to_owned())),
                "key" => tokens.push(FormatToken::Variable(|_, obj| obj.key())),
                "uri" => tokens.push(FormatToken::Variable(|bucket, obj| {
                    format!("s3://{}/{}", bucket, obj.key())
                })),
                "size_bytes" => tokens.push(FormatToken::Variable(|_, obj| match obj {
                    PrefixResult::Object(obj) => obj.size.to_string(),
                    PrefixResult::Prefix(_) => "-1".to_owned(),
                })),
                "size_human" => tokens.push(FormatToken::Variable(|_, obj| match obj {
                    PrefixResult::Object(obj) => {
                        SizeFormatter::new(obj.size as u64, decimal_format()).to_string()
                    }
                    PrefixResult::Prefix(_) => "-1".to_owned(),
                })),
                "last_modified" => tokens.push(FormatToken::Variable(|_, obj| match obj {
                    PrefixResult::Object(obj) => obj.last_modified.to_string(),
                    PrefixResult::Prefix(_) => "-1".to_owned(),
                })),
                "etag" => tokens.push(FormatToken::Variable(|_, obj| match obj {
                    PrefixResult::Object(obj) => obj.etag.clone().unwrap_or_default(),
                    PrefixResult::Prefix(_) => String::new(),
                })),
                "storage_class" => tokens.push(FormatToken::Variable(|_, obj| match obj {
                    PrefixResult::Object(obj) => obj.storage_class.clone().unwrap_or_default(),
                    PrefixResult::Prefix(_) => String::new(),
                })),
                "restore_in_progress" => tokens.push(FormatToken::Variable(|_, obj| match obj {
                    PrefixResult::Object(obj) => match &obj.restore_status {
                        Some(rs) => rs.in_progress.to_string(),
                        None => String::new(),
                    },
                    PrefixResult::Prefix(_) => String::new(),
                })),
                "restore_expiry" => tokens.push(FormatToken::Variable(|_, obj| match obj {
                    PrefixResult::Object(obj) => obj
                        .restore_status
                        .as_ref()
                        .and_then(|rs| rs.expiry.clone())
                        .unwrap_or_default(),
                    PrefixResult::Prefix(_) => String::new(),
                })),
                "checksums" => tokens.push(FormatToken::Variable(|_, obj| match obj {
                    PrefixResult::Object(obj) => obj
                        .checksum_algorithms
                        .as_ref()
                        .map(|v| v.join(","))
                        .unwrap_or_default(),
                    PrefixResult::Prefix(_) => String::new(),
                })),
                _ => {
                    return Err(anyhow::anyhow!(
                        "unknown variable (see --help for options): {}",
                        var
                    ));
                }
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

fn format_user(bucket: &str, obj: &PrefixResult, tokens: &[FormatToken]) -> String {
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
    let env_var = std::env::var("S3GLOB_LOG")
        .or_else(|_| std::env::var("RUST_LOG"))
        .ok();
    if let Some(directive) = directive.or(env_var.as_deref()) {
        for d in directive.split(',') {
            match d.parse() {
                Ok(d) => env_filter = env_filter.add_directive(d),
                Err(e) => eprintln!("ERROR: failed to parse logging directive '{d}': {e:#}"),
            }
        }
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
    #[case("{kind} {key}", "OBJ test/file.txt")]
    #[trace]
    fn test_compile_format(#[case] format: &str, #[case] expected: &str) {
        let fmt = compile_format(format).unwrap();

        let object = Object::builder().key("test/file.txt").size(1234).build();

        let result = format_user("bkt", &PrefixResult::Object(S3Object::from(object)), &fmt);
        assert_eq!(result, expected);
    }

    #[rstest]
    #[case("{kind} {bucket}/{key}", "PRE bkt/test/")]
    #[case("{kind} {uri}", "PRE s3://bkt/test/")]
    #[trace]
    fn test_compile_prefix_format(#[case] format: &str, #[case] expected: &str) {
        let fmt = compile_format(format).unwrap();
        let prefix = "test/";
        let result = format_user("bkt", &PrefixResult::Prefix(prefix.to_owned()), &fmt);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_format_invalid_variable() {
        assert!(compile_format("{invalid_var}").is_err());
    }

    #[rstest]
    #[case("\"abc123\"", "abc123")]
    #[case("abc123", "abc123")]
    #[case("", "")]
    fn test_unquote_etag(#[case] input: &str, #[case] expected: &str) {
        assert_eq!(unquote_etag(input.to_owned()), expected);
    }

    #[rstest]
    #[case("{etag}", "abc123")]
    #[case("{storage_class}", "STANDARD")]
    #[case("{checksums}", "SHA256")]
    #[case("{etag} {storage_class}", "abc123 STANDARD")]
    #[trace]
    fn test_compile_format_new_vars(#[case] format: &str, #[case] expected: &str) {
        use aws_sdk_s3::types::{ChecksumAlgorithm, ObjectStorageClass};
        let fmt = compile_format(format).unwrap();
        let object = Object::builder()
            .key("test/file.txt")
            .size(1234)
            .e_tag("\"abc123\"")
            .storage_class(ObjectStorageClass::Standard)
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .build();
        let result = format_user("bkt", &PrefixResult::Object(S3Object::from(object)), &fmt);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_compile_format_new_vars_missing_render_empty() {
        let fmt = compile_format(
            "[{etag}|{storage_class}|{restore_in_progress}|{restore_expiry}|{checksums}]",
        )
        .unwrap();
        let object = Object::builder().key("test/file.txt").size(1234).build();
        let result = format_user("bkt", &PrefixResult::Object(S3Object::from(object)), &fmt);
        assert_eq!(result, "[||||]");
    }

    #[test]
    fn test_json_ls_record_object_omits_missing_fields() {
        let object = Object::builder().key("a/b.txt").size(42).build();
        let result = PrefixResult::Object(S3Object::from(object));
        let record = JsonLsRecord::from_result("bkt", &result);
        let v = serde_json::to_value(&record).unwrap();
        assert_eq!(v["type"], "object");
        assert_eq!(v["bucket"], "bkt");
        assert_eq!(v["key"], "a/b.txt");
        assert_eq!(v["uri"], "s3://bkt/a/b.txt");
        assert_eq!(v["size"], 42);
        // Optional fields not populated by Object::builder() must be omitted.
        assert!(v.get("etag").is_none());
        assert!(v.get("storage_class").is_none());
        assert!(v.get("checksum_algorithms").is_none());
        assert!(v.get("restore_status").is_none());
    }

    #[test]
    fn test_json_ls_record_object_includes_optional_fields() {
        use aws_sdk_s3::types::{ChecksumAlgorithm, ObjectStorageClass};
        let object = Object::builder()
            .key("a/b.txt")
            .size(42)
            .e_tag("\"deadbeef\"")
            .storage_class(ObjectStorageClass::IntelligentTiering)
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .build();
        let result = PrefixResult::Object(S3Object::from(object));
        let record = JsonLsRecord::from_result("bkt", &result);
        let v = serde_json::to_value(&record).unwrap();
        assert_eq!(v["etag"], "deadbeef");
        assert_eq!(v["storage_class"], "INTELLIGENT_TIERING");
        let algos = v["checksum_algorithms"].as_array().unwrap();
        assert_eq!(algos.len(), 2);
    }

    #[test]
    fn test_json_ls_record_prefix_shape() {
        let result = PrefixResult::Prefix("dir/".to_owned());
        let record = JsonLsRecord::from_result("bkt", &result);
        let v = serde_json::to_value(&record).unwrap();
        assert_eq!(v["type"], "prefix");
        assert_eq!(v["bucket"], "bkt");
        assert_eq!(v["key"], "dir/");
        assert_eq!(v["uri"], "s3://bkt/dir/");
        assert!(v.get("size").is_none());
        assert!(v.get("last_modified").is_none());
    }
}
