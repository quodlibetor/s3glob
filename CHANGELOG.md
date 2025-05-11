# Changelog

## [0.4.6](https://github.com/quodlibetor/s3glob/compare/v0.4.5...v0.4.6) - 2025-05-11

### 🐛 Bug Fixes

- Ensure that no non-matching prefixes are emitted by [@quodlibetor](https://github.com/quodlibetor) in [#40](https://github.com/quodlibetor/s3glob/pull/40)

## [0.4.5-prerelease.2](https://github.com/quodlibetor/s3glob/compare/v0.4.5-prerelease1...v0.4.5-prerelease.2) - 2025-05-04

### 📚 Documentation

- Mention the status of the project in the README by [@quodlibetor](https://github.com/quodlibetor) in [659d7c3](https://github.com/quodlibetor/s3glob/commit/659d7c3714e1180f48c7b751c2c48a45cd9911db)

## [0.4.5-prerelease1](https://github.com/quodlibetor/s3glob/compare/v0.4.4...v0.4.5-prerelease1) - 2025-05-04

### 🐛 Bug Fixes

- **(release)** Attempt to fix the brew publish command by [@quodlibetor](https://github.com/quodlibetor) in [#39](https://github.com/quodlibetor/s3glob/pull/39)

## [0.4.4](https://github.com/quodlibetor/s3glob/compare/v0.4.3...v0.4.4) - 2025-05-03

### 📚 Documentation

- Fix dl description in README by [@quodlibetor](https://github.com/quodlibetor) in [8cc2052](https://github.com/quodlibetor/s3glob/commit/8cc2052870dd9e602490de3d225b87e298ec01ac)
- Fix README description of glob rules by [@quodlibetor](https://github.com/quodlibetor) in [7b76d0c](https://github.com/quodlibetor/s3glob/commit/7b76d0cb69afcb203c0181187c65ddbd077b47d1)

### ⚡ Performance

- Don't double check or scan literal objects by [@quodlibetor](https://github.com/quodlibetor) in [#38](https://github.com/quodlibetor/s3glob/pull/38)

### ⚙️ Miscellaneous Tasks

- Remove unnecessary struct pattern in match by [@quodlibetor](https://github.com/quodlibetor) in [ffb3fed](https://github.com/quodlibetor/s3glob/commit/ffb3fed9c5821343ee911f80238b553ecc1416d4)
- Update release github runners to 22.04 by [@quodlibetor](https://github.com/quodlibetor) in [#35](https://github.com/quodlibetor/s3glob/pull/35)

## [0.4.3](https://github.com/quodlibetor/s3glob/compare/v0.4.2...v0.4.3) - 2025-03-01

### 🐛 Bug Fixes

- LCP message needs a newline prepended by [@quodlibetor](https://github.com/quodlibetor) in [79bbe08](https://github.com/quodlibetor/s3glob/commit/79bbe08382f812d786dbee85c2307a76f9e72149)
- Remove workaround for minio in tests by [@quodlibetor](https://github.com/quodlibetor) in [6e7777f](https://github.com/quodlibetor/s3glob/commit/6e7777f9749c43bfb8a60a88a32d8e7116f42cf2)
- Get rid of Docker login in CI to get dependabot working by [@quodlibetor](https://github.com/quodlibetor) in [#23](https://github.com/quodlibetor/s3glob/pull/23)

### 📚 Documentation

- Improve error message for no matches found by [@quodlibetor](https://github.com/quodlibetor) in [36a4ef8](https://github.com/quodlibetor/s3glob/commit/36a4ef8dcc0e7fe0a4f3ef5638fe68bf842de916)

### ⚙️ Miscellaneous Tasks

- Remove dead code from early experimentation by [@quodlibetor](https://github.com/quodlibetor) in [e21bb7d](https://github.com/quodlibetor/s3glob/commit/e21bb7d413fc14aaf7e02ea69de47ca29770d0e1)
- Update to edition 2024 by [@quodlibetor](https://github.com/quodlibetor) in [465f9f5](https://github.com/quodlibetor/s3glob/commit/465f9f53b696070ac7ba6d56902e19ff8dbba697)

## [0.4.2](https://github.com/quodlibetor/s3glob/compare/v0.4.1...v0.4.2) - 2025-02-28

### 🚀 Features

- Add a --quiet/-q flag to suppress progress messages by [@quodlibetor](https://github.com/quodlibetor) in [1730ef8](https://github.com/quodlibetor/s3glob/commit/1730ef8603695e10e50297982bb129243dc1c495)
- Add a --max-parallelism flag, defaulting to 10,000 by [@quodlibetor](https://github.com/quodlibetor) in [eb86ae4](https://github.com/quodlibetor/s3glob/commit/eb86ae49d71a293a3e9ddff2a92b5633ffc13d4a)

### 📚 Documentation

- Add a gif of s3glob in action to the README by [@quodlibetor](https://github.com/quodlibetor) in [27f8dbe](https://github.com/quodlibetor/s3glob/commit/27f8dbe0d203ec6185ae0dec73d31a17efbbc226)

### ⚙️ Miscellaneous Tasks

- Keep track of the max prefixes seen for better messaging by [@quodlibetor](https://github.com/quodlibetor) in [a7ebb41](https://github.com/quodlibetor/s3glob/commit/a7ebb4158fc3d9f34500dd534cbe7990df6df4b5)

## [0.4.1](https://github.com/quodlibetor/s3glob/compare/v0.4.0...v0.4.1) - 2025-02-26

### 🚀 Features

- **breaking** Prefix prefix display with PRE prefix by [@quodlibetor](https://github.com/quodlibetor) in [f99adf1](https://github.com/quodlibetor/s3glob/commit/f99adf1e2b37fa2a44e33b48de2b21dff668818e)
- Add a "shortest" algorithm for --path-mode by [@quodlibetor](https://github.com/quodlibetor) in [14c1376](https://github.com/quodlibetor/s3glob/commit/14c13762679acba0600be39b74ece515a620aaa0)
- Add a --flatten download arg that replaces / with - by [@quodlibetor](https://github.com/quodlibetor) in [bc9ae52](https://github.com/quodlibetor/s3glob/commit/bc9ae52451f8196df5832ec20958ce2f031bedf6)

### ⚙️ Miscellaneous Tasks

- Fix link in CHANGELOG.md by [@quodlibetor](https://github.com/quodlibetor) in [6813927](https://github.com/quodlibetor/s3glob/commit/6813927f5cdee1a42b44481a060c682fbbacd9e4)
- Split unit and integration test jobs by [@quodlibetor](https://github.com/quodlibetor) in [de6e56f](https://github.com/quodlibetor/s3glob/commit/de6e56feddad0be98d35c40ae7585feab2ffbae5)
- Fix changelog template by [@quodlibetor](https://github.com/quodlibetor) in [09e3e80](https://github.com/quodlibetor/s3glob/commit/09e3e8069b22d4f8ac4b76d139a7109943220bd3)

## [0.4.0](https://github.com/quodlibetor/s3glob/compare/v0.3.1...v0.4.0) - 2025-02-26

### 🚀 Features

- **breaking** Support listing intermediate prefixes, not just full objects by [@quodlibetor](https://github.com/quodlibetor) in [8ba6dc6](https://github.com/quodlibetor/s3glob/commit/8ba6dc621f5859e65aba3f4e2eae2d6f241c7b87)

### 🐛 Bug Fixes

- Return a useful error if a glob filters out all prefixes by [@quodlibetor](https://github.com/quodlibetor) in [5708291](https://github.com/quodlibetor/s3glob/commit/570829165ebf3992881e15b757e037d7e532305d)
- Correctly end when there are no more files to download by [@quodlibetor](https://github.com/quodlibetor) in [a25564b](https://github.com/quodlibetor/s3glob/commit/a25564bc7acbccddad56c0904359e237f30ac317)

### 📚 Documentation

- Add comment explaining downloader pools by [@quodlibetor](https://github.com/quodlibetor) in [25b1411](https://github.com/quodlibetor/s3glob/commit/25b14110ae2ebfe1fa077476527b2014adce3018)

### ⚙️ Miscellaneous Tasks

- Improve changelog generation with github links by [@quodlibetor](https://github.com/quodlibetor) in [9f7f93b](https://github.com/quodlibetor/s3glob/commit/9f7f93bca87d3d74a0f127f1602a6ab4151e688e)

## [0.3.1](https://github.com/quodlibetor/s3glob/compare/v0.3.0...v0.3.1) - 2025-02-22

### ⚡ Performance

- Download objects in parallel for (potentially) huge speedups by [@quodlibetor](https://github.com/quodlibetor) in [#13](https://github.com/quodlibetor/s3glob/pull/13)

## [0.3.0](https://github.com/quodlibetor/s3glob/compare/v0.2.7...v0.3.0) - 2025-02-20

### 🚀 Features

- Add --path-mode and change default download behavior to only keep paths after glob patterns by [@quodlibetor](https://github.com/quodlibetor) in [958fc6e](https://github.com/quodlibetor/s3glob/commit/958fc6e26e44755b42d83b07316525250561e1c5)

## [0.2.7](https://github.com/quodlibetor/s3glob/compare/v0.2.6...v0.2.7) - 2025-02-19

### 🚀 Features

- Better error when all generated prefixes become invalid by [@quodlibetor](https://github.com/quodlibetor) in [8c54af1](https://github.com/quodlibetor/s3glob/commit/8c54af1c945ecf47c332a1d74d5f2eddfe554349)

### 🐛 Bug Fixes

- Small improvements to progress output by [@quodlibetor](https://github.com/quodlibetor) in [d071674](https://github.com/quodlibetor/s3glob/commit/d07167461c3452aa737594b06bc11f3e130cf7fe)
- -vvv should generate a directive that parses by [@quodlibetor](https://github.com/quodlibetor) in [f6876c6](https://github.com/quodlibetor/s3glob/commit/f6876c6f2635c5d14a1df79e339612dd9ebe54f1)

### ⚡ Performance

- Use a set for some "prefix contains" queries by [@quodlibetor](https://github.com/quodlibetor) in [bf50c5c](https://github.com/quodlibetor/s3glob/commit/bf50c5c27356f41b3cc399f176876d221a89c9a9)

### ⚙️ Miscellaneous Tasks

- Reduce toomanyrequests Docker Hub errors in CI by [@quodlibetor](https://github.com/quodlibetor) in [#9](https://github.com/quodlibetor/s3glob/pull/9)
- Add github attestations to dist release config by [@quodlibetor](https://github.com/quodlibetor) in [#11](https://github.com/quodlibetor/s3glob/pull/11)

## [0.2.6](https://github.com/quodlibetor/s3glob/compare/v0.2.5...v0.2.6) - 2025-02-17

### 🐛 Bug Fixes

- Support character ranges [a-c] in prefix generation by [@quodlibetor](https://github.com/quodlibetor) in [f3b1393](https://github.com/quodlibetor/s3glob/commit/f3b139351ce22c3065b4ba804a881147bf7a0bc6)

## [0.2.5](https://github.com/quodlibetor/s3glob/compare/v0.2.4...v0.2.5) - 2025-02-15

### 🚀 Features

- Add the ability to stream output by [@quodlibetor](https://github.com/quodlibetor) in [bcfc81a](https://github.com/quodlibetor/s3glob/commit/bcfc81a3ea2e29c528a23cb2dde3f25170b19612)
- Warn when a large number of prefixes are being discovered by [@quodlibetor](https://github.com/quodlibetor) in [a5c5d1c](https://github.com/quodlibetor/s3glob/commit/a5c5d1c86fcab44eb9754e81f0ae1ea8c93459f8)

### 🐛 Bug Fixes

- Support actual aws cli option --no-sign-request by [@quodlibetor](https://github.com/quodlibetor) in [04b718a](https://github.com/quodlibetor/s3glob/commit/04b718a54f0170ad7f660d5d70f80e066e10460d)

## [0.2.4](https://github.com/quodlibetor/s3glob/compare/v0.2.3...v0.2.4) - 2025-02-14

### 🐛 Bug Fixes

- Don't try to find prefixes in file parts by [@quodlibetor](https://github.com/quodlibetor) in [359331b](https://github.com/quodlibetor/s3glob/commit/359331bf3878eac953d9578e36052f8e0e1e3898)

### 📚 Documentation

- Point out the parallelism help when few prefixes are found by [@quodlibetor](https://github.com/quodlibetor) in [f07d3b1](https://github.com/quodlibetor/s3glob/commit/f07d3b129df0b8c20fab9428163b399830ea8ff1)

## [0.2.3](https://github.com/quodlibetor/s3glob/compare/v0.2.2...v0.2.3) - 2025-02-13

### 🐛 Bug Fixes

- Skip prefix discovery in the file part of keys by [@quodlibetor](https://github.com/quodlibetor) in [9d7726a](https://github.com/quodlibetor/s3glob/commit/9d7726a3446f922fee072918d28528ce05249f8b)
- Show full errors for bucket discovery by [@quodlibetor](https://github.com/quodlibetor) in [038451d](https://github.com/quodlibetor/s3glob/commit/038451d71613530f0da63b8aa78c7f92f9540fdb)

### 📚 Documentation

- Add an s3glob help parallelism command by [@quodlibetor](https://github.com/quodlibetor) in [d0c5a65](https://github.com/quodlibetor/s3glob/commit/d0c5a6590eb419a3961e6fa1502c3c59ebb8e0ac)

## [0.2.2](https://github.com/quodlibetor/s3glob/compare/v0.2.1...v0.2.2) - 2025-02-13

### 🐛 Bug Fixes

- Improve error messages by [@quodlibetor](https://github.com/quodlibetor) in [ec42518](https://github.com/quodlibetor/s3glob/commit/ec42518ce077eac246739c00f3c4c653ac753236)
- Handle globs at the end of keys better by [@quodlibetor](https://github.com/quodlibetor) in [50fcd20](https://github.com/quodlibetor/s3glob/commit/50fcd209b23af3e800e3d6b14d46c4a57f020f6b)

### ⚡ Performance

- Ensure reasonable concurrency as base for scan by [@quodlibetor](https://github.com/quodlibetor) in [3fce5b2](https://github.com/quodlibetor/s3glob/commit/3fce5b2be0d944b3196c1cf8fc169aeb611efea7)
- Parallelize "directory" scanning for prefixes by [@quodlibetor](https://github.com/quodlibetor) in [ab4aba0](https://github.com/quodlibetor/s3glob/commit/ab4aba0931f13b18ce2477761dc2f967232e1ae1)

### ⚙️ Miscellaneous Tasks

- Add cargo nextest test timeouts by [@quodlibetor](https://github.com/quodlibetor) in [626c6c3](https://github.com/quodlibetor/s3glob/commit/626c6c339eefbc2e28a8cf9f04954639e56034ee)

## [0.2.1](https://github.com/quodlibetor/s3glob/compare/v0.2.0...v0.2.1) - 2025-02-12

### 🚀 Features

- Do region auto-discovery for buckets by [@quodlibetor](https://github.com/quodlibetor) in [ed7cc9d](https://github.com/quodlibetor/s3glob/commit/ed7cc9d2a3bd41cf0264a964f3bb6ff027346394)
- Improve documentation in the README and cli by [@quodlibetor](https://github.com/quodlibetor) in [49469a9](https://github.com/quodlibetor/s3glob/commit/49469a96cc57bca706706453c2a4bf8720805eae)
- Provide a --no-sign-requests flag for public buckets by [@quodlibetor](https://github.com/quodlibetor) in [9c69788](https://github.com/quodlibetor/s3glob/commit/9c697886baa784ef107c59a2f3e4285ce058ee03)

### ⚙️ Miscellaneous Tasks

- Add mise config for dev tools by [@quodlibetor](https://github.com/quodlibetor) in [09115f0](https://github.com/quodlibetor/s3glob/commit/09115f08c0dad7699c5eef5e131218e7b3fb9b92)

## [0.2.0](https://github.com/quodlibetor/s3glob/compare/v0.1.1...v0.2.0) - 2025-02-11

### 🚀 Features

- Add support for downloading matches by [@quodlibetor](https://github.com/quodlibetor) in [712df04](https://github.com/quodlibetor/s3glob/commit/712df04fc2523e2c86f6ed9dfa63ff2e5d0ee4ee)
- Add {uri} as a list format token by [@quodlibetor](https://github.com/quodlibetor) in [ba1b599](https://github.com/quodlibetor/s3glob/commit/ba1b599573c5b02874d96f825e0398c2d0a49f39)
- Dramatically improve performance for early patterns by [@quodlibetor](https://github.com/quodlibetor) in [01e17a1](https://github.com/quodlibetor/s3glob/commit/01e17a11822f803774b1eaa59dbc4ca22d66256b)

### ⚙️ Miscellaneous Tasks

- Recognize more release comments in cliff by [@quodlibetor](https://github.com/quodlibetor) in [179e750](https://github.com/quodlibetor/s3glob/commit/179e7501c38c113b3baf7d024440cda3069c5fd9)
- Use cargo-nextest in CI by [@quodlibetor](https://github.com/quodlibetor) in [13c56b9](https://github.com/quodlibetor/s3glob/commit/13c56b9b89d3ab2677bc9ff13e57e716471c2780)

## [0.1.1](https://github.com/quodlibetor/s3glob/compare/v0.1.0...v0.1.1) - 2025-01-20

### 🚀 Features

- Add the ability for users to specify the output format by [@quodlibetor](https://github.com/quodlibetor) in [7efdcef](https://github.com/quodlibetor/s3glob/commit/7efdcef774c5f61dcd27605190ec46869ae180ce)

### 🐛 Bug Fixes

- Ensure that globs in the file part still get searched by [@quodlibetor](https://github.com/quodlibetor) in [25179a4](https://github.com/quodlibetor/s3glob/commit/25179a47e84772bf272187b75c4a6a00ca1e42d7)

<!-- generated by git-cliff -->
