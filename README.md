# s3glob

s3glob is a fast aws s3 list implementation that basically obeys standard unix
glob patterns.

In my experience (on an ec2 instance) s3glob can list 10s of millions of files
in about 5 seconds, where I gave up on `aws s3 ls` after 5 minutes.

## Usage

These two commands are equivalent:

```bash
s3glob "s3://my-bucket/a*/something/1*/other/*"
s3glob      "my-bucket/a*/something/1*/other/*"
```

Output is in the same format as `aws s3 ls`.

### Syntax

Glob syntax supported:

- `*` matches any number of characters. Note that unlike extglob, `*` is
  not limited to a single "directory" level (it matches `/` characters), so it
  is strictly more general than `**`.
- `?` matches any single character.
- `[abc]`/`[!abc]` matches any single character in/not in the set.
- `[a-z]`/`[!a-z]` matches any single character in/not in the range.
- `{a,b,c}` matches any of the comma-separated options, including nested globs
  (but cannot contain `{..}` patterns).

### Algorithm and performance implications

`s3glob` will search for the first glob character and use the string up to that
point as the prefix to search.

Parallelism is at the level of the number of objects discovered in that prefix,
within the given delimiter (specified by the `-d` option, defaulting to `/`).

As an example, let's walk through the pattern `s3://mycompany-importantdata/project/2024-*/anything/*/data*.csv`

1. `s3glob` will search for the first glob character, which is `*`, and discover the prefix `project/2024-`
2. An API request is made to list all the prefixes in `project/2024-` up to the delimeter `/`
   If there are 366 path elements like `2024-01-01` through `2024-12-31` then
   there will be 366 parallel tasks spawned.
3. Each of those tasks will do a full bucket scan for _its_ prefix, e.g. there
   will be one task for `project/2024-01-01` and one for `project/2024-01-02` and
   so on.

I haven't noticed performance degradation up to 1000s of parallel tasks, so I
recommend actively trying to choose a prefix that will generate lots of tasks.

## Copying

All code is available under the MIT or Apache 2.0 license, at your option.
