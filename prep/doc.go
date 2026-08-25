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
// prep loads the entire file into memory for processing. This enables
// multi-pass operations (preprocessing then validation) but means memory
// usage scales with file size. For large files, use ProcessToWriter to
// reduce peak memory by avoiding the output buffer.
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
// The "prep" tag specifies preprocessing operations applied before validation:
//   - trim: Remove leading and trailing whitespace
//   - ltrim: Remove leading whitespace
//   - rtrim: Remove trailing whitespace
//   - lowercase: Convert to lowercase
//   - uppercase: Convert to uppercase
//   - default=value: Set default value if empty
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
// An empty value passes every validator except required: an empty cell is how
// CSV spells a missing value, so a column is optional unless required says
// otherwise. This is where the dialect is deliberately left — that library
// fails most validators on an empty value.
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
// all. boolean accepts what strconv.ParseBool accepts, which is also how a
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
