# AGENTS.md

Guidance for AI agents and contributors working in this repository.
This is the single source of truth; `CLAUDE.md` and
`.github/copilot-instructions.md` only point here.

## What is filesql

A Go library that loads files (CSV, TSV, LTSV, Parquet, Excel, JSON/JSONL, and
their `.gz` / `.bz2` / `.xz` / `.zst` variants) into SQLite3 and exposes them
through the standard `database/sql` interface. It backs
[sqly](https://github.com/nao1215/sqly) and
[sqluv](https://github.com/nao1215/sqluv).

## Commands

- `make test` — run tests with coverage (`cover.out`, `cover.html`)
- `make lint` — run golangci-lint
- `make tools` — install dev tools (golangci-lint, octocov)

`make test` and `make lint` must pass before work is considered done.

## Conventions

- **Test-Driven Development** (t-wada style). Table-driven tests with `t.Run`,
  use `t.Parallel()` where possible, keep fixtures in `testdata/`, and keep
  coverage at 80%+ (checked by octocov).
- **No CGO** — uses pure-Go `modernc.org/sqlite`; do not add CGO dependencies.
- **Cross-platform** (Linux/macOS/Windows): use `filepath.Join`, never hardcode
  path separators or line endings.
- **No global variables** — pass state through arguments and return values.
- **Errors**: wrap with `fmt.Errorf("...: %w", err)`, compare with `errors.Is` /
  `errors.As`, and never ignore them.
- **Comments in English.** Write godoc comments for every exported symbol and a
  package overview in each package's `doc.go`.

## When you change things

- **New file format**: update `file.go` (FileType + detection),
  `file_processor.go` (parsing), `parser_bridge.go` (map to fileparser), and add
  `testdata/` fixtures.
- **Feature that touches the README**: update all 7 languages — `README.md` and
  `doc/{es,fr,ja,ko,ru,zh-cn}/README.md`. `doc_sync_test.go` guards against
  drift (translations existing, sharing stable markers, matching the English
  top-level section count, and linking every language); run `make test` after
  README changes and update those checks when you add a top-level section.
- **CHANGELOG.md**: add an entry referencing the PR number and commit hash as
  clickable links, e.g.
  `- Description (PR #123, [abc1234](https://github.com/nao1215/filesql/commit/abc1234))`.
- Keep sponsor links (https://github.com/sponsors/nao1215) in user-facing docs.
