// Package prep provides preprocessing and validation for file formats
// supported by filesql (CSV, TSV, LTSV, JSON, JSONL, Parquet, Excel with gzip, bzip2, xz, zstd support).
//
// prep complements filesql by providing data preprocessing before loading
// into SQLite. It uses struct tags for validation ("validate" tag) and
// preprocessing ("prep" tag).
//
// # Basic Usage
//
//	type Record struct {
//	    Name  string `prep:"trim" validate:"required"`
//	    Email string `prep:"trim,lowercase" validate:"email"`
//	    Age   int    `validate:"gte=0,lte=150"`
//	}
//
//	file, _ := os.Open("data.csv")
//	defer file.Close()
//
//	var records []Record
//	processor := prep.NewProcessor(prep.FileTypeCSV)
//	reader, result, err := processor.Process(file, &records)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// reader can be passed directly to filesql
//	// result.Errors contains validation errors with row/column information
//	// result.RowCount and result.ValidRowCount provide processing statistics
//
// # Streaming Output with ProcessToWriter
//
// For large datasets, ProcessToWriter writes preprocessed output directly
// to an io.Writer, avoiding the output buffer allocation:
//
//	var buf bytes.Buffer
//	result, err := processor.ProcessToWriter(file, &records, &buf)
//
// # Memory Usage
//
// prep holds the whole input in memory, so its peak scales with the size of the
// file rather than staying flat the way filesql's own chunked loading does.
// Three things are live at once: the parsed rows, one element of the struct
// slice per row, and, for Process, the buffer the preprocessed output is built
// in. ProcessToWriter removes the third of those and not the first two, since
// the struct slice is what the caller asked for and the rows are what a second
// pass over them needs.
//
// That is the design rather than an oversight: preprocessing and validation are
// separate passes over the same rows, and the row a validator reports on has to
// be somewhere when it does. A file large enough for this to matter is better
// loaded with filesql, which reads it in chunks, and validated afterwards with
// SQL over the loaded table.
//
// Format-specific limitations:
//   - XLSX: Only the first sheet is processed
//   - LTSV: Maximum line size is 10MB
//   - JSON/JSONL: Data has a single "data" column containing raw JSON strings
//
// A struct may cover a subset of the columns. A field naming a column the input
// does not have is refused with ErrUnknownColumn, since a zero-filled field
// cannot be told apart from a cell that is really empty; give such a field
// prep:"default=..." if it is meant to work without a column.
//
// # Supported File Formats
//
// prep supports the same formats as filesql:
//   - CSV (.csv)
//   - TSV (.tsv)
//   - LTSV (.ltsv)
//   - JSON (.json)
//   - JSONL (.jsonl)
//   - Parquet (.parquet)
//   - Excel (.xlsx)
//
// All formats support compression:
//   - gzip (.gz)
//   - bzip2 (.bz2)
//   - xz (.xz)
//   - zstd (.zst)
//   - zlib (.z)
//   - snappy (.snappy)
//   - s2 (.s2)
//   - lz4 (.lz4)
//
// # Prep Tags
//
// The "prep" tag specifies preprocessing operations applied before validation.
// A comma separates one tag from the next, so a tag's own parameters are
// separated by a colon rather than a comma:
//
//   - trim, ltrim, rtrim: remove whitespace from both ends, the left, the right
//   - collapse_space: replace each run of whitespace with one space
//   - strip_newline: remove carriage returns and line feeds
//   - strip_html: remove HTML tags
//   - lowercase, uppercase: fold case
//   - normalize_unicode: normalize to NFC
//   - default=value: use value when the cell is empty
//   - nullify=value: read value as an empty cell
//   - prefix=value, suffix=value: add value at the front or the back
//   - replace=old:new: replace every occurrence of old
//   - regex_replace=pattern:replacement: replace every match of pattern
//   - truncate=N: keep the first N characters
//   - pad_left=N:char, pad_right=N:char: pad to N characters, with char or a
//     space
//   - trim_set=chars: remove any of chars from both ends
//   - keep_digits, keep_alpha: keep only digits, or only letters
//   - remove_digits, remove_alpha: remove digits, or letters
//   - coerce=int|float|bool: normalize the written form of a number or a
//     boolean
//   - fix_scheme=scheme: add scheme:// to a URL that has no scheme
//
// A tag that needs a parameter and is given none is an invalid tag argument:
// WithStrictTagParsing reports it, and without that option it is ignored.
//
// # Validate Tags
//
// The "validate" tag specifies validation rules (following the
// go-playground/validator dialect):
//   - required: Field must not be empty
//   - email: Must be a valid email address
//   - url: Must be a valid URL
//   - And many more...
//
// A field tagged validate:"-" is validated by nothing, as in the dialect. Its
// prep tag still runs.
//
// An empty value passes every validator except required and the required
// family: an empty cell is how CSV spells a missing value, so a column is
// optional unless one of those tags says otherwise. This is where the dialect
// is deliberately left — that library fails most validators on an empty value.
// The rule reaches the cross-field comparisons too, on either side: a
// comparison is skipped as soon as one of the two cells is empty, since a value
// has no order against a value that is not there. The required family —
// required_if, required_unless, required_with, required_with_all,
// required_without and required_without_all — is the exception: each of those
// tags runs on an empty cell and reports it when its condition holds, since
// deciding whether an empty cell is allowed is what they are for.
//
// The comparison tags follow the field they land on, as the dialect defines
// them: on a string field, eq and ne compare the string itself and gt, gte,
// lt, lte, min and max compare the character count, while on any other field
// all of them compare the numeric value and len means the value equals the
// parameter. The cross-field tags eqfield, nefield, gtfield, gtefield, ltfield
// and ltefield follow the field the same way, except that on a string field
// they order the strings rather than measure them, so ltfield says a date range
// runs forwards. That is the second place this package leaves the dialect,
// which compares string lengths there and so cannot express a date range at
// all. required_if and required_unless take field and value pairs, as many as
// the tag names, and a value holding a space is written in single quotes:
// required_if=Kind paid Tier 'gold member' asks for a value only when both
// cells match. required_with and its three siblings take a list of field names
// instead: required_with fires when any of them carries a value and
// required_with_all when all of them do, while required_without fires when any
// of them is empty and required_without_all when all of them are. boolean accepts what strconv.ParseBool accepts, which is also how a
// bool struct field is filled. numeric accepts an optionally signed decimal
// and number accepts digits alone, as the dialect defines them. The dialect
// spells the letters-and-digits tag alphanum; alphanumeric names the same
// validator, because this package answered only to that spelling before.
//
// Some validators are deliberately stricter or wider than the dialect. uri
// requires a scheme, so a relative reference such as /a/b is not a URI. fqdn
// refuses a label ending in a hyphen, which the dialect's fqdn pattern allows
// though its hostname patterns do not. ulid requires a timestamp the format
// can hold. uuid3 requires the variant nibble that uuid4 and uuid5 require,
// and all three accept upper case, as uuid does. hostname_port takes a
// bracketed IPv6 address and refuses a bare port.
//
// See https://pkg.go.dev/github.com/nao1215/filesql/prep for the complete list of supported validators.
package prep
