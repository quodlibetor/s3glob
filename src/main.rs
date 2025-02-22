use std::io::IsTerminal as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::types::Object;
use aws_sdk_s3::{config::BehaviorVersion, config::Region, Client};
use clap::{ArgAction, Parser, Subcommand, ValueEnum};
use glob_matcher::{S3Engine, S3GlobMatcher, GLOB_CHARS};
use humansize::{FormatSizeOptions, SizeFormatter, DECIMAL};
use num_format::{Locale, ToFormattedString};
use regex::Regex;
use tokio::io::AsyncWriteExt as _;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::{debug, trace, warn};

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

        /// Control how S3 object keys are mapped to local file paths
        ///
        /// - absolute | abs: the full key path will be reproduced in the
        ///   destination
        /// - from-first-glob | g: the key path relative to the first path part
        ///   containing a glob in the pattern will be reproduced in the
        ///   destination
        #[clap(short, long, verbatim_doc_comment, default_value = "from-first-glob")]
        path_mode: PathType,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PathType {
    Abs,
    Absolute,
    G,
    FromFirstGlob,
}

impl ValueEnum for PathType {
    fn value_variants<'a>() -> &'a [Self] {
        &[
            PathType::Absolute,
            PathType::Abs,
            PathType::FromFirstGlob,
            PathType::G,
        ]
    }

    fn from_str(s: &str, _ignore_case: bool) -> Result<Self, String> {
        match s {
            "absolute" | "abs" => Ok(PathType::Absolute),
            "from-first-glob" | "g" => Ok(PathType::FromFirstGlob),
            _ => Err(format!("invalid path type: {}", s)),
        }
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        match self {
            PathType::Abs => Some(clap::builder::PossibleValue::new("abs")),
            PathType::Absolute => Some(clap::builder::PossibleValue::new("absolute")),
            PathType::FromFirstGlob => Some(clap::builder::PossibleValue::new("from-first-glob")),
            PathType::G => Some(clap::builder::PossibleValue::new("g")),
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
        .find(GLOB_CHARS)
        .map_or(raw_pattern.clone(), |i| raw_pattern[..i].to_owned());

    let engine = S3Engine::new(client.clone(), bucket.clone(), opts.delimiter.to_string());
    let matcher = S3GlobMatcher::parse(raw_pattern.clone(), &opts.delimiter.to_string())?;
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
        Command::Download {
            dest, path_mode, ..
        } => {
            let mut total_matches = 0;
            let pools = DlPools::new();
            let prefix_to_strip = extract_prefix_to_strip(&raw_pattern, path_mode);
            let (ntfctn_tx, mut ntfctn_rx) = tokio::sync::mpsc::unbounded_channel::<Notification>();
            let base_path = PathBuf::from(dest);
            let dl = Downloader::new(client, bucket, prefix_to_strip, base_path, ntfctn_tx);
            while let Some(PrefixResult {
                matching_objects: mo,
                ..
            }) = rx.recv().await
            {
                total_matches += mo.len();
                for obj in mo {
                    pools.download_object(dl.fresh(), obj);
                }

                eprint!(
                    "\rmatches/total {:>4}/{:<10} prefixes completed/total {:>4}/{:<4}",
                    total_matches.to_formatted_string(&Locale::en),
                    total_objects
                        .load(Ordering::Relaxed)
                        .to_formatted_string(&Locale::en),
                    seen_prefixes.load(Ordering::Relaxed),
                    total_prefixes
                );
            }
            // close the tx so the downloaders know to finish
            drop(pools);
            eprintln!();
            let start_time = Instant::now();
            let mut downloaded_matches = 0;
            let mut total_bytes = 0_usize;
            let mut speed = 0.0;
            let mut files = Vec::with_capacity(total_matches);
            while let Some(n) = ntfctn_rx.recv().await {
                match n {
                    Notification::ObjectDownloaded(path) => {
                        downloaded_matches += 1;
                        files.push(path.display().to_string());
                    }
                    Notification::BytesDownloaded(bytes) => {
                        total_bytes += bytes;
                    }
                }
                let elapsed = start_time.elapsed().as_secs_f64();
                speed = total_bytes as f64 / elapsed;
                eprint!(
                    "\rdownloaded {}/{} objects, {:>7}   {:>10}/s",
                    downloaded_matches,
                    total_matches,
                    SizeFormatter::new(total_bytes as u64, decimal_format()).to_string(),
                    SizeFormatter::new(speed.round() as u64, decimal_format()).to_string(),
                );
                // TODO: the ntfc receiver should shut down, shouldn't it?
                if downloaded_matches >= total_matches {
                    break;
                }
            }
            eprintln!("\n");

            files.sort_unstable();
            for path in files {
                println!("{}", path);
            }
            let dl_ms = start_time.elapsed().as_millis() as u64;
            eprintln!(
                "\ndiscovered {} objects in {:?} | downloaded {} in {:?} ({}/s)",
                downloaded_matches,
                Duration::from_millis(start.elapsed().as_millis() as u64 - dl_ms),
                SizeFormatter::new(total_bytes as u64, decimal_format()),
                Duration::from_millis(dl_ms),
                SizeFormatter::new(speed.round() as u64, decimal_format()),
            );
        }
        Command::Parallelism { .. } => {
            eprintln!("This is just for documentation, run instead: s3glob help parallelism");
        }
    }

    Ok(())
}

struct DlPools {
    two_hundred_kb: UnboundedSender<(Downloader, Object)>,
    one_mb: UnboundedSender<(Downloader, Object)>,
    ten_mb: UnboundedSender<(Downloader, Object)>,
    more: UnboundedSender<(Downloader, Object)>,
}

impl DlPools {
    /// Loose heuristics based on pretty fast internet, I haven done a ton of benchmarking
    fn new() -> DlPools {
        let (two_hundred_kb, rx) = tokio::sync::mpsc::unbounded_channel();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(500));
        start_threadpool(semaphore, rx);
        let (one_mb, rx) = tokio::sync::mpsc::unbounded_channel();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(50));
        start_threadpool(semaphore, rx);

        let (ten_mb, rx) = tokio::sync::mpsc::unbounded_channel();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(10));
        start_threadpool(semaphore, rx);

        let (more, rx) = tokio::sync::mpsc::unbounded_channel();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(5));
        start_threadpool(semaphore, rx);

        Self {
            two_hundred_kb,
            one_mb,
            ten_mb,
            more,
        }
    }

    fn download_object(&self, dl: Downloader, object: Object) {
        let tx = if let Some(size) = object.size {
            if size < 200_000 {
                &self.two_hundred_kb
            } else if size < 1_000_000 {
                &self.one_mb
            } else if size < 10_000_000 {
                &self.ten_mb
            } else {
                &self.more
            }
        } else {
            debug!(?object, "object size not known, using more pool");
            &self.more
        };
        tx.send((dl, object))
            .expect("send on channel should succeed");
    }
}

fn start_threadpool(
    semaphore: Arc<tokio::sync::Semaphore>,
    mut rx: UnboundedReceiver<(Downloader, Object)>,
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
struct Downloader {
    client: Client,
    bucket: String,
    prefix_to_strip: String,
    base_path: PathBuf,
    obj_counter: Arc<AtomicUsize>,
    obj_id: usize,
    notifier: UnboundedSender<Notification>,
}

#[derive(Debug)]
enum Notification {
    ObjectDownloaded(PathBuf),
    BytesDownloaded(usize),
}

impl Downloader {
    fn new(
        client: Client,
        bucket: String,
        prefix_to_strip: String,
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
            prefix_to_strip,
        }
    }

    /// Create a downloader that can safely download another object
    fn fresh(&self) -> Self {
        let obj_id = add_atomic(&self.obj_counter, 1);
        Self {
            client: self.client.clone(),
            bucket: self.bucket.clone(),
            obj_counter: Arc::clone(&self.obj_counter),
            obj_id,
            notifier: self.notifier.clone(),
            prefix_to_strip: self.prefix_to_strip.clone(),
            base_path: self.base_path.clone(),
        }
    }

    async fn download_object(self, obj: Object) {
        let key = obj.key.as_ref().unwrap();
        let key_suffix = key
            .strip_prefix(&self.prefix_to_strip)
            .expect("all found objects will include the prefix");
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

fn extract_prefix_to_strip(raw_pattern: &str, path_mode: PathType) -> String {
    match path_mode {
        PathType::Abs | PathType::Absolute => String::new(),
        PathType::FromFirstGlob | PathType::G => {
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
    }
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

        let result = format_user("bkt", &object, &fmt);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_format_invalid_variable() {
        assert!(compile_format("{invalid_var}").is_err());
    }

    macro_rules! assert_extract_prefix_to_strip {
        ($pattern:expr, $path_mode:expr, $expected:expr) => {
            let actual = extract_prefix_to_strip($pattern, $path_mode);
            assert2::check!(
                actual == $expected,
                "input: {} path_mode: {:?}",
                $pattern,
                $path_mode,
            );
        };
    }

    #[test]
    fn test_extract_prefix_to_strip() {
        // Test absolute path mode
        assert_extract_prefix_to_strip!("prefix/path/to/*.txt", PathType::Absolute, "");
        assert_extract_prefix_to_strip!("bucket/deep/path/*.txt", PathType::Abs, "");

        // Test from-first-glob path mode
        assert_extract_prefix_to_strip!(
            "prefix/path/to/*.txt",
            PathType::FromFirstGlob,
            "prefix/path/to/"
        );
        assert_extract_prefix_to_strip!(
            "prefix/path/*/more/*.txt",
            PathType::FromFirstGlob,
            "prefix/path/"
        );
        assert_extract_prefix_to_strip!("prefix/*.txt", PathType::FromFirstGlob, "prefix/");
        assert_extract_prefix_to_strip!("*.txt", PathType::FromFirstGlob, "");
        assert_extract_prefix_to_strip!("prefix/a.txt", PathType::FromFirstGlob, "prefix/");
        // Test with different glob characters
        assert_extract_prefix_to_strip!(
            "prefix/path/to/[abc]/*.txt",
            PathType::FromFirstGlob,
            "prefix/path/to/"
        );
        assert_extract_prefix_to_strip!(
            "prefix/path/to/?/*.txt",
            PathType::FromFirstGlob,
            "prefix/path/to/"
        );
        assert_extract_prefix_to_strip!(
            "prefix/path/{a,b}/*.txt",
            PathType::FromFirstGlob,
            "prefix/path/"
        );
    }
}
