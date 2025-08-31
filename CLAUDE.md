# filesql

filesql is a library that implements the common functionality of the following commands: "Load files such as CSV into SQLite3 and execute SQL queries against CSV, etc."
- [sqly](https://github.com/nao1215/sqly): easily execute SQL against CSV/TSV/LTSV/JSON and Microsoft Excel™ with shell.
- [sqluv](https://github.com/nao1215/sqluv): simple terminal UI for RDBMS & CSV/TSV/LTSV at local/https/s3

filesql reads the contents of specified input files and returns [sql.DB](https://pkg.go.dev/database/sql#DB). Developers can execute SQL queries in the same way as the standard sql.DB they normally use, making it easy to learn.

## Codebase Information
### Development Commands
- `make test`: Run tests and measure coverage (generates cover.out file, viewable in browser with cover.html)
- `make lint`: Code inspection with golangci-lint (.golangci.yml configuration)
- `make clean`: Delete generated files
- `make tools`: Install dependency tools (golangci-lint, octocov)

### Key Features
- Input file formats: CSV, TSV, LTSV, Parquet, Excel + compressed versions (.gz, .bz2, .xz, .zst)
- Input methods: File path, directory, io.Reader, embed.FS
- Streaming: Chunk-based processing for large files
- Type inference: Automatic column data type detection (INTEGER/REAL/TEXT)
- Auto-save: Automatic saving at Commit or db.Close timing

## Development Rules
- Test-Driven Development: We adopt the test-driven development promoted by t-wada (Takuto Wada). Always write test code and be mindful of the test pyramid.
- Working code: Ensure that `make test` and `make lint` succeed after completing work.
- Sponsor acquisition: Since development incurs financial costs, we seek sponsors via `https://github.com/sponsors/nao1215`. Include sponsor links in README and documentation.
- Contributor acquisition: Create developer documentation so anyone can participate in development and recruit contributors.
- Comments in English: Write code comments in English to accept international contributors.
- User-friendly documentation comments: Write detailed explanations and example code for public functions so users can understand usage at a glance.

## Coding Guidelines
- No global variables: Do not use global variables. Manage state through function arguments and return values.
- Coding rules: Follow Golang coding rules. [Effective Go](https://go.dev/doc/effective_go) is the basic rule.
- Package comments are mandatory: Describe the package overview in `doc.go` for each package. Clarify the purpose and usage of the package.
- Comments for public functions, variables, and struct fields are mandatory: When visibility is public, always write comments following go doc rules.
- Remove duplicate code: After completing your work, check if you have created duplicate code and remove unnecessary code.
- Update README: When adding new features, update the README at the following paths:
  - README.md
  - doc/es/README.md
  - doc/fr/README.md
  - doc/ja/README.md
  - doc/ko/README.md
  - doc/ru/README.md
  - doc/zh-cn/README.md
- CHANGELOG.md maintenance: When updating CHANGELOG.md, always include references to the relevant PR numbers and commit hashes with clickable GitHub links. This helps developers trace which specific changes were made in which PR/commit and allows them to browse the actual code changes. Format examples:
  - **Feature description ([abc1234](https://github.com/nao1215/filesql/commit/abc1234))**: Detailed explanation of the change
  - **Feature description (PR #123, [abc1234](https://github.com/nao1215/filesql/commit/abc1234))**: When both PR and commit are relevant
  - Use `git log --oneline` and GitHub PR numbers to identify the specific changes
  - Always format commit hashes as clickable links: `[hash](https://github.com/nao1215/filesql/commit/hash)`
  - This improves traceability and allows developers to browse code changes directly in their browser
  - Users want to see the actual implementation, so always provide GitHub links for commits

## Testing
- [Readable Test Code](https://logmi.jp/main/technology/327449): Avoid excessive optimization (DRY) and aim for a state where it's easy to understand what tests exist.
- Clear input/output: Create tests with `t.Run()` and clarify test case input/output. Test cases clarify test intent by explicitly showing input and expected output.
- Test granularity: Aim for 80% or higher coverage with unit tests.
- Parallel test execution: Use `t.Parallel()` to run tests in parallel whenever possible.
- Using `octocov`: Run `octocov` after `make test` to confirm test coverage exceeds 80%.
- Cross-platform support: Tests run on Linux, macOS, and Windows through GitHub Actions. Examples of non-cross-platform code include "concatenating paths without using `filepath.Join`" and "using "\n" for line breaks".
- Test data storage: Store sample files in various formats in the testdata directory
