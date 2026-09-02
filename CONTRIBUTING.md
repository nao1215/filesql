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

filesql requires Go 1.25.13 or later. On the 1.26 line the minimum is 1.26.6,
not 1.26.0: those two patch releases are where the standard library fixes for
[GO-2026-6088](https://pkg.go.dev/vuln/GO-2026-6088) (`encoding/xml`) and
[GO-2026-5972](https://pkg.go.dev/vuln/GO-2026-5972) (`encoding/asn1`) landed,
and the CI matrix pins exactly those two.

`govulncheck` runs on every pull request and on `main`. To run it yourself:

```bash
go install golang.org/x/vuln/cmd/govulncheck@v1.7.0
govulncheck ./...
```

It reports on the standard library of whichever Go builds the module, so a
toolchain older than the minimum above will report those two advisories against
your local Go rather than against filesql.

## Development Rules

- Follow normal Go style. [`Effective Go`](https://go.dev/doc/effective_go) is the baseline.
- Keep public APIs documented.
- Add or update tests with the code change.
- Put the test in `<source>_test.go`, next to the source that implements the behavior. A new test file belongs with a new source file, not with a new case: `foo_edge_test.go` beside `foo_test.go` splits one subject across two files, and the next person adds a third. Group cases with `t.Run` instead.
- Prefer direct, explicit tests over clever test abstractions.
- If you change a README code sample, keep the sample covered by tests.
- Do not leave stale docs, stale examples, or dead code behind.

## Compatibility

What a release promises to keep is the exported surface of the three published packages -- `filesql`, `filesql/dialect` and `filesql/prep` -- meaning every exported name, its signature, and the behavior its godoc states. A change to any of those is a breaking change and goes under `### Breaking Changes` in the changelog with a migration note, whatever the version number does.

What a release does not promise to keep is an answer that was wrong. A dialect translation is a promise about the engine the dialect names: a translated query answers what MySQL, PostgreSQL or BigQuery answers, and a change that moves an answer toward that engine's is a fix, even when the old answer was one a caller had pinned in a test. The same holds for a load that stored a value differently from the file, a save that wrote a file differently from the one it read, and an error that named the wrong sentinel. Those go under `### Fixed`, and the entry says what the old answer was so a caller who depended on it can find it.

The line between the two is whether the godoc said so. Behavior the godoc states is the contract; behavior a caller observed and the godoc did not state is not, and the fix for an unstated behavior that mattered is to state it, in the godoc, in the same change.

Do not add to the exported surface to fix a bug. If the fix needs a new name, the pull request says why no existing one could carry it.

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
