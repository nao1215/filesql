# Security Policy

## Reporting

If you believe you found a vulnerability, email:
[`n.chika156@gmail.com`](mailto:n.chika156@gmail.com)

Please do not open a public GitHub issue first.

Include:

- affected version or commit
- reproduction steps
- impact
- any workaround you already found

## What To Expect

- Acknowledgement after the report is received
- Follow-up questions if reproduction details are missing
- A fix or mitigation plan when the report is valid

Response time depends on severity and reproduction quality, but valid reports are handled as priority work.

## Supported Versions

Security fixes are provided for the latest published release series only.

| Version | Supported | Notes |
|---------|-----------|-------|
| `0.56.x` | Yes | Current published release series as of September 2, 2026 |
| `0.55.x` and earlier | No | Upgrade to the latest `0.56.x` release |

Security fixes are not backported to unsupported release series. When the next
series is published, support moves to it.

`v1.0.0` has not been published. Work towards it is in `main` and in the
Unreleased section of [CHANGELOG.md](./CHANGELOG.md); nothing in this table
covers it until there is a tag. Once `v1.0.0` is out, support will move to
`1.0.x` and this table will say so, and from that point the compatibility
promise in the version number is what says whether an upgrade is safe to take.

## Toolchain Baseline

filesql requires Go 1.25.13 or later, and 1.26.6 or later on the 1.26 line.
Those are the patch releases carrying the standard library fixes for
[GO-2026-6088](https://pkg.go.dev/vuln/GO-2026-6088) (`encoding/xml`) and
[GO-2026-5972](https://pkg.go.dev/vuln/GO-2026-5972) (`encoding/asn1`). filesql
reaches `encoding/xml` on every XLSX it reads, so the first of those is on a
path any caller loading a workbook takes. `govulncheck` runs on every pull
request and on `main`.

## Known Limitations With Untrusted Input

filesql parses file formats. A file from somewhere you do not control is input
to a decoder, and the guarantees below are what this project can honestly claim
today.

### Parquet

A Parquet page header states how large the page's statistics are. filesql reads
Parquet through `parquet-go`, and that decoder allocates the declared amount
before reading the bytes, so it never finds out that they are not there. A
damaged 473-byte file therefore allocates about 98 MiB and is then refused. The
refusal is fast — under a millisecond — so this is exhaustion of memory, not of
time, and the cost is paid again on every call: a long-running process opening
the same file twenty times pays it twenty times.

The number sits inside a column chunk rather than in the footer, so filesql
never sees it and has nothing to check in front. The footer length, which is the
other number a Parquet file declares, *is* read and bounded before the bytes are
handed over.

This was investigated as [#526](https://github.com/nao1215/filesql/issues/526).
It is not fixed: the allocation is inside the decoder this project depends on,
and forking it or writing an unvetted one of our own would trade a bounded,
known cost for an unbounded, unknown one.

**Do not point a memory-constrained process at Parquet files you did not
write.** Where you must, bound the process rather than the file: a memory limit
on the container or cgroup, or `GOMEMLIMIT`, turns this into a refused load
rather than a machine under pressure. Checking the file's size first does not
help, since the whole point is that 473 bytes can ask for 98 MiB.

### xz and zstd

Both formats declare in their header how much working memory the decoder must
hold, and a decoder allocates that before reading any data. filesql caps the xz
dictionary at 256 MiB and the zstd window at 128 MiB, so a damaged file costs a
fixed ceiling rather than whatever its header names. The zstd cap holds for
every frame; the xz cap is read from the first block of the first stream, so a
later block or a concatenated second stream is not covered by it.

### ACH and Fedwire

Both formats are parsed by the moov-io libraries, which validate as they read, so
a malformed file is refused rather than partly loaded. Neither format is
compressed and neither declares a size for filesql to allocate against, so the
memory a file costs is bounded by the file.

A Fedwire write-back is verified before it reaches your file: the message is
written to a buffer, read back, and compared field by field with the table it was
written from. A field that did not survive refuses the whole write, and nothing
is written, so a refused export leaves the original where it was. An ACH
write-back is not verified the same way, because ACH control records are
recalculated by the write and so cannot be compared against what was read.

## SQL and the Query Surface

filesql executes SQL against an in-memory SQLite database. A query string built
by concatenating untrusted text is an injection in filesql exactly as it is
anywhere else; use `database/sql` placeholders.

`OpenReadOnly` refuses writes, and it is a guard against writing by accident
rather than a sandbox: it does not confine a caller who sets out to write, and
it says nothing about what a query may read or how long it may run. It is not a
boundary to put untrusted SQL behind.
