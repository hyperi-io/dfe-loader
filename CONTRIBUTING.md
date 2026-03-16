# Contributing

We welcome contributions to this project. By contributing, you agree to the
terms outlined below.

## Commit Message Format

HyperI projects use [Conventional Commits](https://www.conventionalcommits.org/)
and [semantic-release](https://semantic-release.gitbook.io/) for automated
versioning and changelog generation. All commits must follow this format:

```
<type>(<scope>): <subject>

[optional body]

[optional footer(s)]
```

### Types

| Type | Description | Version Bump |
|------|-------------|--------------|
| `feat` | A new feature | Minor (0.X.0) |
| `fix` | A bug fix | Patch (0.0.X) |
| `docs` | Documentation only | None |
| `style` | Code style (formatting, semicolons, etc.) | None |
| `refactor` | Code change that neither fixes a bug nor adds a feature | None |
| `perf` | Performance improvement | Patch (0.0.X) |
| `test` | Adding or correcting tests | None |
| `build` | Changes to build system or dependencies | None |
| `ci` | Changes to CI configuration | None |
| `chore` | Other changes that don't modify src or test files | None |
| `revert` | Reverts a previous commit | Varies |

### Breaking Changes

For breaking changes that require a major version bump, add `!` after the type
or include `BREAKING CHANGE:` in the footer:

```
feat!: remove deprecated API endpoints

BREAKING CHANGE: The /v1/users endpoint has been removed. Use /v2/users instead.
```

### Examples

```
feat(auth): add OAuth2 support for Google login

fix(api): handle null response from upstream service

docs: update installation instructions for Windows

refactor(core)!: restructure module exports

BREAKING CHANGE: Named exports are now used instead of default exports.
```

### Scope

Scope is optional but recommended. Use it to indicate the area of the codebase
affected (e.g., `api`, `auth`, `core`, `cli`, `docs`).

## Semantic Versioning

This project follows [Semantic Versioning 2.0.0](https://semver.org/):

- **MAJOR** (X.0.0): Breaking changes that require users to modify their code
- **MINOR** (0.X.0): New features that are backwards-compatible
- **PATCH** (0.0.X): Bug fixes and minor improvements

Versions are automatically determined by semantic-release based on commit
messages. Do not manually update version numbers.

## Developer Certificate of Origin

This project uses the Developer Certificate of Origin (DCO) to ensure that
contributors have the right to submit their contributions.

By making a contribution to this project, you certify that:

1. The contribution was created in whole or in part by you and you have the
   right to submit it under the license indicated in the file; or

2. The contribution is based upon previous work that, to the best of your
   knowledge, is covered under an appropriate open source license and you
   have the right under that license to submit that work with modifications;
   or

3. The contribution was provided directly to me by some other person who
   certified (1), (2) or (3) and you have not modified it.

4. You understand and agree that this project and the contribution are public
   and that a record of the contribution (including all personal information
   you submit with it, including your sign-off) is maintained indefinitely
   and may be redistributed consistent with this project or the license(s)
   involved.

## How to Sign Off Your Commits

You must sign off each commit to indicate your acceptance of the DCO. Combine
the signoff with your conventional commit message:

```
git commit --signoff -m "feat(auth): add two-factor authentication"
```

This produces:

```
feat(auth): add two-factor authentication

Signed-off-by: Your Name <your.email@example.com>
```

Make sure your Git configuration has your correct name and email:

```
git config --global user.name "Your Name"
git config --global user.email "your.email@example.com"
```

## License for Contributions

All contributions to this project are licensed under the Functional Source
License, Version 1.1, ALv2 Future License (FSL-1.1-ALv2), the same license
that covers the project.

Each version of the software (including your contributions) will automatically
become available under the Apache License, Version 2.0 on the second
anniversary of its release.

## How to Contribute

1. **Fork the repository** and create your branch from `main`
2. **Make your changes** following the commit message format above
3. **Sign off your commits** with the DCO
4. **Test your changes** to ensure they work as expected
5. **Submit a pull request** with a clear description of what you've done

### Pull Request Checklist

- [ ] Commits follow the conventional commit format
- [ ] All commits are signed off (DCO)
- [ ] Tests pass (if applicable)
- [ ] Documentation is updated (if applicable)

## Building

### Local Build

Run `hyperi-ci check` before pushing — it runs quality checks and tests in
the same sequence as CI:

```bash
hyperi-ci check               # Quality + tests (default)
hyperi-ci check --quick       # Quality only
hyperi-ci check --full        # Quality + tests + build
make check                    # Same as hyperi-ci check
```

Or build binaries directly:

```bash
cargo build --release --features jemalloc
```

### Local Build Artifacts

| Artifact | Location | Notes |
|----------|----------|-------|
| Native binary | `$CARGO_TARGET_DIR/release/dfe-loader` | Default `target/` or `~/.cargo-target/` |
| Cross-compiled binaries | `dist/dfe-loader-{version}-linux-amd64` | Stripped, release-optimised |
| | `dist/dfe-loader-{version}-linux-arm64` | Cross-compiled via `aarch64-linux-gnu-gcc` |
| Checksums | `dist/checksums.sha256` | SHA-256 for all binaries |

### Cross-Compilation Requirements

The aarch64 cross-build requires these system packages (the CI build script
installs them automatically via `sudo`):

- `gcc-aarch64-linux-gnu` — C cross-compiler
- `g++-aarch64-linux-gnu` — C++ cross-compiler (for rdkafka cmake build)
- `libssl-dev:arm64` — OpenSSL headers/libs for arm64
- `libsasl2-dev:arm64` — Cyrus SASL headers/libs for arm64

On Ubuntu, arm64 packages require multiarch with `ports.ubuntu.com` sources.
The build script configures this automatically. Note that `libsasl2-dev` is
not `Multi-Arch: same`, so arm64 and amd64 variants cannot coexist — the
build script handles this by building the native target first.

## CI/CD Pipeline

### Triggers

| Workflow | Trigger | What It Does |
|----------|---------|--------------|
| `ci.yml` | Push to any branch, PRs | Quality checks + tests |
| `publish.yml` | GitHub Release published, manual dispatch | Build + publish crate and binaries |

### CI Publish Artifacts

When a GitHub Release is published, CI produces and deploys:

| Artifact | Destination | Path/URL |
|----------|-------------|----------|
| **Rust crate** | JFrog Artifactory (`hyperi` registry) | `hyperi-cargo-virtual/dfe-loader/{version}` |
| **Linux amd64 binary** | JFrog Artifactory (generic repo) | `hyperi-binaries/dfe-loader/{version}/dfe-loader-{version}-linux-amd64` |
| **Linux arm64 binary** | JFrog Artifactory (generic repo) | `hyperi-binaries/dfe-loader/{version}/dfe-loader-{version}-linux-arm64` |
| **Linux amd64 binary** | GitHub Release assets | Attached to the release |
| **Linux arm64 binary** | GitHub Release assets | Attached to the release |
| **Checksums** | Both Artifactory + GitHub Release | `checksums.sha256` |
| **LATEST_VERSION.txt** | JFrog Artifactory | `hyperi-binaries/dfe-loader/latest/LATEST_VERSION.txt` |

### Artifactory Details

| Setting | Value |
|---------|-------|
| JFrog domain | `hypersec.jfrog.io` |
| Cargo registry name | `hyperi` |
| Cargo index URL | `sparse+https://hypersec.jfrog.io/artifactory/api/cargo/hyperi-cargo-virtual/index/` |
| Binary repo | `hyperi-binaries` |
| Binary folder structure | `{project}/{version}/{binary-name}-{version}-{os}-{arch}` |

### Version Flow

1. Commit merged to `main` with `fix:` or `feat:` type
2. **semantic-release** determines version bump from commit messages
3. Updates `CHANGELOG.md`, creates git tag, publishes GitHub Release
4. `publish.yml` triggers on the release event
5. Builds crate + cross-compiled binaries
6. Publishes crate to Artifactory Cargo registry
7. Publishes binaries to Artifactory generic repo + GitHub Release assets

## Questions

If you have questions about contributing, please open an issue or contact us.
