# Contributing

Thanks for working on filesql.

This repository is a Go library first. The bar is simple:

- keep behavior clear
- keep docs accurate
- keep tests readable
- keep `main` releasable

## Before You Start

- Open an issue first for large API changes or behavior changes that could break users.
- Small fixes, docs improvements, and test cleanups can go straight to a PR.
- If you change public behavior, update `README.md` and the relevant package docs or examples in the same branch.

## Local Setup

```bash
git clone https://github.com/nao1215/filesql.git
cd filesql
make tools
```

filesql requires Go 1.25 or later.

## Development Rules

- Follow normal Go style. [`Effective Go`](https://go.dev/doc/effective_go) is the baseline.
- Keep public APIs documented.
- Add or update tests with the code change.
- Prefer direct, explicit tests over clever test abstractions.
- If you change a README code sample, keep the sample covered by tests.
- Do not leave stale docs, stale examples, or dead code behind.

## Validation

Run these before you send a PR:

```bash
go test ./...
make lint
```

If you touch the public API, also run the examples. They are separate modules, so `go test ./...` never compiles them:

```bash
make examples
```

That builds each example against your checkout and diffs what it prints against the `Expected Output` block of its README.

## Pull Requests

A good PR for this repository usually does four things:

1. Explains the behavior change in a few sentences.
2. Includes tests or explains why tests were not needed.
3. Updates docs when the public surface changed.
4. Updates `CHANGELOG.md` when the change matters to users.

Use the PR template. Keep the diff focused. If a change is broad, split it into reviewable commits even when it ships in one PR.

## Documentation

`README.md` is the source of truth for the top-level product description.

When docs change:

- update English docs first
- keep code samples runnable
- prefer examples that match real usage over exhaustive API tours

## Release Notes

User-facing changes should be added to `CHANGELOG.md`. Write the entry so someone scanning the release can tell:

- what changed
- who should care
- any limitation or migration note

## Bug Reports

Good bug reports include:

- OS and Go version
- filesql version or commit
- the smallest input that reproduces the issue
- the expected result and the actual result

Use the issue template when possible.

## Security

Please do not open a public issue for a suspected vulnerability. See [SECURITY.md](./SECURITY.md).

## Code of Conduct

Please read [CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md). If you cannot be respectful in issues, PRs, and reviews, do not participate.

## Support

If filesql helps your work, a GitHub star or sponsor page visit helps the project:
https://github.com/sponsors/nao1215
