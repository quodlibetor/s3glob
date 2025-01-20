use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::types::Object;
use aws_sdk_s3::{config::BehaviorVersion, config::Region, Client};
use clap::Parser;
use globset::{Glob, GlobMatcher};
use humansize::{FormatSizeOptions, SizeFormatter, DECIMAL};
use num_format::{Locale, ToFormattedString};
use regex::Regex;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

#[derive(Debug, Parser)]
struct Opts {
    #[clap(short, long, default_value = "us-west-2")]
    region: String,

    /// Format string for output
    ///
    /// This is a string that will be formatted for each object.
    ///
    /// The format string can use the following variables:
    /// - `{key}`: the key of the object
    /// - `{size_bytes}`: the size of the object in bytes, with no suffix
    /// - `{size_human}`: the size of the object in a decimal format (e.g. 1.23MB)
    /// - `{last_modified}`: the last modified date of the object, RFC3339 format
    ///
    /// For example, the default format looks like this, but with some padding:
    ///     {last_modified} {size_human} {key}
    #[clap(short, long)]
    format: Option<String>,

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
    #[clap(short, long, default_value = "/")]
    delimiter: String,

    /// Glob pattern to match objects against
    ///
    /// The pattern can either be an s3 uri or a <bucket>/<glob> without the
    /// s3://
    ///
    /// Example:
    ///     s3://my-bucket/my_prefix/2024-12-*/something_else/*
    ///     my-bucket/my_prefix/2024-12-*/something_else/*
    pattern: String,
}

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        if let Err(err) = run().await {
            eprintln!("ERROR: {}", err);
            let mut err = err.source();
            let mut count = 0;
            while let Some(e) = err {
                eprintln!("  : {}", e);
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

async fn run() -> Result<()> {
    let opts = Opts::parse();

    // parse possible s3 uri using s3 crate api
    let region = Region::new(opts.region);
    let config = aws_config::defaults(BehaviorVersion::v2024_03_28())
        .region(RegionProviderChain::first_try(region))
        .load()
        .await;
    let client = Client::new(&config);

    let s3re = Regex::new(r"^(?:s3://)?([^/]+)/(.*)").unwrap();
    let pat = opts.pattern.clone();
    let matches = s3re.captures(&pat);
    let (bucket, raw_pattern) = if let Some(m) = matches {
        (
            m.get(1).unwrap().as_str().to_owned(),
            m.get(2).unwrap().as_str().to_owned(),
        )
    } else {
        bail!("pattern must have a <bucket>/<pattern> format, with an optional s3:// prefix");
    };

    let pattern = Glob::new(&raw_pattern)?;
    // Find prefix before first glob character
    let prefix = raw_pattern
        .find(['*', '?', '[', '{'])
        .map_or(raw_pattern.clone(), |i| raw_pattern[..i].to_owned());

    let user_format = if let Some(user_fmt) = opts.format {
        Some(compile_format(&user_fmt)?)
    } else {
        None
    };

    // List directories for the prefix at the first glob character
    //
    // TODO: apply sections of the glob as prefixes until we get to the last one
    // So a*/something/1*/other/*  would find ab ac and then use ab/something/1
    // and ac/something/1 to find prefixes before other, then just the full
    // expansion all the prefixes in a{bc}/something/1{23}/other/*
    //
    // probably only do that full expansion if the glob is immediately followed
    // by a delimiter char?
    //
    // Not doing it right now because s3glob is already finishing in a couple
    // seconds for tens of millions of objects.

    let mut prefixes = Vec::new();
    let mut paginator = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix(&prefix)
        .delimiter(opts.delimiter)
        .into_paginator()
        .send();

    while let Some(page) = paginator.next().await {
        let page = page?;
        if let Some(common_prefixes) = page.common_prefixes {
            prefixes.extend(common_prefixes.into_iter().filter_map(|p| p.prefix));
        }
    }

    // If there are no common prefixes, then the prefix itself is the only
    // matching prefix.
    if prefixes.is_empty() {
        prefixes.push(prefix);
    }

    // Process directories concurrently
    let matching_objects = Arc::new(Mutex::new(Vec::new()));
    let mut tasks = Vec::new();

    let total_objects = Arc::new(AtomicUsize::new(0));
    let seen_prefixes = Arc::new(AtomicUsize::new(0));
    let total_prefixes = prefixes.len();
    let matcher = pattern.compile_matcher();
    for prefix in prefixes {
        let client = client.clone();
        let matching_objects = Arc::clone(&matching_objects);
        let total_objects = Arc::clone(&total_objects);
        let seen_prefixes = Arc::clone(&seen_prefixes);
        let matcher = matcher.clone();
        let bucket = bucket.clone();

        tasks.push(tokio::spawn(async move {
            let (objects, seen) =
                list_matching_objects(&client, &bucket, &prefix, &matcher).await?;
            let match_count = {
                let mut m = matching_objects.lock().await;
                m.extend(objects);
                m.len()
            };

            let total_objects = add_atomic(&total_objects, seen);
            let seen_prefixes = add_atomic(&seen_prefixes, 1);
            eprint!(
                "\rmatches/total {:>4}/{:<10} prefixes/total {:>4}/{:<4}",
                match_count,
                total_objects.to_formatted_string(&Locale::en),
                seen_prefixes,
                total_prefixes
            );
            Ok::<_, anyhow::Error>(())
        }));
    }

    // Wait for all tasks to complete
    for task in tasks {
        task.await??;
    }

    eprintln!();
    let mut objects = matching_objects.lock().await;
    objects.sort_by(|a, b| a.key.cmp(&b.key));
    let decimal = decimal_format();
    for obj in objects.iter() {
        if let Some(user_fmt) = &user_format {
            print_user(obj, user_fmt);
        } else {
            print_default(obj, decimal);
        }
    }

    Ok(())
}

fn decimal_format() -> FormatSizeOptions {
    FormatSizeOptions::from(DECIMAL)
        .decimal_places(1)
        .space_after_value(false)
}

#[derive(Debug)]
enum FormatToken {
    Literal(String),
    Variable(fn(&Object) -> String),
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
                "key" => tokens.push(FormatToken::Variable(|obj| {
                    obj.key.as_ref().unwrap().to_string()
                })),
                "size_bytes" => tokens.push(FormatToken::Variable(|obj| {
                    obj.size.unwrap_or(0).to_string()
                })),
                "size_human" => tokens.push(FormatToken::Variable(|obj| {
                    SizeFormatter::new(obj.size.unwrap_or(0) as u64, decimal_format()).to_string()
                })),
                "last_modified" => tokens.push(FormatToken::Variable(|obj| {
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
        "{:>10} {:>6}    {}",
        obj.last_modified
            .as_ref()
            .map(|dt| dt.to_string())
            .unwrap_or_default(),
        SizeFormatter::new(obj.size.unwrap_or(0) as u64, format),
        obj.key.as_ref().unwrap_or(&String::new()),
    );
}

fn print_user(obj: &Object, tokens: &[FormatToken]) {
    println!("{}", format_user(obj, tokens));
}

fn format_user(obj: &Object, tokens: &[FormatToken]) -> String {
    let mut result = String::new();
    for token in tokens {
        match token {
            FormatToken::Literal(lit) => result.push_str(lit),
            FormatToken::Variable(var) => result.push_str(&var(obj)),
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
    matcher: &GlobMatcher,
) -> Result<(Vec<aws_sdk_s3::types::Object>, usize)> {
    let mut matching_objects = Vec::new();
    let mut paginator = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .into_paginator()
        .send();

    let mut seen_objects = 0;
    while let Some(page) = paginator.next().await {
        let page = page?;
        if let Some(contents) = page.contents {
            seen_objects += contents.len();
            for obj in contents {
                if let Some(key) = &obj.key {
                    if matcher.is_match(key) {
                        matching_objects.push(obj);
                    }
                }
            }
        }
    }

    Ok((matching_objects, seen_objects))
}

#[cfg(test)]
mod tests {
    use aws_sdk_s3::types::Object;
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("Size: {size_bytes}, Name: {key}", "Size: 1234, Name: test/file.txt")]
    #[case("s: {size_human}\t{key}", "s: 1.2kB\ttest/file.txt")]
    fn test_compile_format(#[case] format: &str, #[case] expected: &str) {
        let fmt = compile_format(format).unwrap();
        assert_eq!(fmt.len(), 4);

        let object = Object::builder().key("test/file.txt").size(1234).build();

        let result = format_user(&object, &fmt);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_format_invalid_variable() {
        assert!(compile_format("{invalid_var}").is_err());
    }
}
