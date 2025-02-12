# s3glob

s3glob is a fast aws s3 list implementation that basically obeys standard unix
glob patterns.

In my experience (on an ec2 instance) s3glob can list 10s of millions of files
in about 5 seconds, where I gave up on `aws s3 ls` after 5 minutes.

## Usage

These two commands are equivalent:

```bash
s3glob ls "s3://my-bucket/a*/something/1*/other/*"
s3glob ls    "my-bucket/a*/something/1*/other/*"
```

Output is in the same format as `aws s3 ls`, but you can change it with the `--format` flag.
For example, this will output just the `s3://<bucket>/<key>` for each object:

```bash
s3glob ls -f "{uri}" "s3://my-bucket/a*/something/1*/other/*"
```

You can also download objects:

```bash
s3glob dl "s3://my-bucket/a*/something/1*/other/*" my-local-dir
```

This will create a directory structure that mirrors the s3 object hierarchy.

### Installation

#### Install prebuilt binaries via shell script

```bash
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/quodlibetor/s3glob/releases/latest/download/s3glob-installer.sh | sh
```

#### Install prebuilt binaries via powershell script

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://github.com/quodlibetor/s3glob/releases/latest/download/s3glob-installer.ps1 | iex"
```

#### Install prebuilt binaries via Homebrew

```bash
brew install quodlibetor/tap/s3glob
```

### Syntax

Glob syntax supported:

- `*` matches any number of characters. Note that unlike extglob, `*` is
  not limited to a single "directory" level (it matches `/` characters), so it
  is strictly more general than `**`.
- `?` matches any single character.
- `[abc]`/`[!abc]` matches any single character in/not in the set.
- `[a-z]`/`[!a-z]` matches any single character in/not in the range.
- `{a,b,c}` matches any of the comma-separated options (but nested globs are not
  supported).

### Algorithm and performance implications

AWS S3 allows us to enumerate objects within a prefix, but it does not natively
allow any filtering. `s3glob` works around this by enumerating prefixes and
matching them recursively against the provided glob pattern.

I have observed s3glob to be able to list hundreds of thousands of objects in a
couple of seconds from within an ec2 instance.

It has a few tricks that it uses to minimize the number of objects that need to
listed, but all of those tricks end at the first recursive glob: `**`.

What this means in general is that, if you have a keyspace that looks like:

```
2000_01_01-2024_12_31/a-z/0-999/OBJECT_ID.txt
```

where each `-` represents the values in between, then you can roughly determine
how many objects S3Glob will need to list by multiplying the number of
values in each range. Adding a filter can reduce that number.

Some example approximate numbers:

| Pattern | Approximate number of objects | Reason |
|---------|--------------------------------|--------|
| `s3glob ls 2000_01_01/a/*/OBJECT_ID.txt` | 1,000 | 0-999 = 1000 |
| `s3glob ls 2000_01_01/[abc]/*/OBJECT_ID.txt` | 3,000 | (a + b + c) * 0-999 = 3 * 1000 |
| `s3glob ls 2000_01_01/*/*/OBJECT_ID.txt` | 26,000 | a-z * 0-999 = 26 * 1000 |
| `s3glob ls 2000_01_01/[!xyz]/*/OBJECT_ID.txt` | 23,026 | (list all of a-z) = 26 => (filter out x,y,z) => 23 * 1,000 = 23,000 |
| `s3glob ls 2000_01_*/*/*/OBJECT_ID.txt` | 806,000 | 01-31 * a-z * 0-999 = 31 * 26 * 1000 |

## Copying

All code is available under the MIT or Apache 2.0 license, at your option.

## Development

### Performing a release

Ensure git-cliff and cargo-release are both installed (run `mise install` to get them)
and run `cargo release [patch|minor]`.

If things look good, run again with `--execute`.
