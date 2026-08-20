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
// # Struct Fields and Columns
//
// A struct may cover a subset of the input's columns; extra columns are
// ignored. A field naming a column the input does not have is refused with
// ErrUnknownColumn, because a zero-filled field cannot be told apart from a
// cell that is really empty: a typo would otherwise be reported as a missing
// value in a column that does not exist. A field that is meant to work without
// a column carries prep:"default=..." and is accepted, since the default is
// where its value comes from.
//
// This is what makes the JSON and JSONL limitation above visible rather than
// silent. Those formats expose one "data" column, so a struct written against
// the JSON object's own keys matches nothing and is now told so.
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
// The "validate" tag specifies validation rules (compatible with go-playground/validator):
//   - required: Field must not be empty
//   - email: Must be a valid email address
//   - url: Must be a valid URL
//   - And many more...
//
// See https://pkg.go.dev/github.com/nao1215/filesql/prep for the complete list of supported validators.
package prep
