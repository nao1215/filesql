// Package reader turns a file of a supported format into the rows of a table.
//
// It is the one implementation of every format filesql reads. The root package
// read files in chunks on the way into SQLite while the parser package read the
// same formats whole, so each format was written twice and the two drifted:
// XLSX dates were normalized on one path and not the other, and a codec or a
// rule fixed in one was still wrong in the other.
//
// A read hands the caller its rows a chunk at a time and returns the type every
// column requires once the last row has been seen. Reading in chunks is what a
// load needs; a caller that wants the whole table collects the chunks, which is
// cheaper than the reverse.
package reader

import (
	"fmt"
	"io"

	"github.com/nao1215/filesql/internal/infer"
)

// Format is the file format a source is read as. Compression is not part of it:
// a caller unwraps the codec before reading.
type Format int

const (
	// FormatCSV is comma-separated values.
	FormatCSV Format = iota
	// FormatTSV is tab-separated values.
	FormatTSV
	// FormatLTSV is labeled tab-separated values.
	FormatLTSV
	// FormatParquet is Apache Parquet.
	FormatParquet
	// FormatXLSX is an Excel workbook.
	FormatXLSX
	// FormatJSON is one JSON document.
	FormatJSON
	// FormatJSONL is one JSON value per line.
	FormatJSONL
)

// String names the format the way an error message does.
func (f Format) String() string {
	switch f {
	case FormatCSV:
		return "CSV"
	case FormatTSV:
		return "TSV"
	case FormatLTSV:
		return "LTSV"
	case FormatParquet:
		return "Parquet"
	case FormatXLSX:
		return "XLSX"
	case FormatJSON:
		return "JSON"
	case FormatJSONL:
		return "JSONL"
	default:
		return "Unsupported"
	}
}

// Rendering says how a value that is not already text is spelled.
type Rendering int

const (
	// RenderPlain spells a value the way the format reads it: a boolean as
	// "true" or "false", and a whole float without a decimal point.
	RenderPlain Rendering = iota
	// RenderSQLite spells a value so SQLite's column affinity converts it back
	// to what the file holds: a boolean as 1 or 0, a whole float with a ".0" so
	// its column stays REAL, and a NaN as NULL, which is what SQLite has in
	// place of one.
	RenderSQLite
)

// Chunk is one run of a table's rows on the way to its caller.
type Chunk struct {
	// Header names the columns, and is the same for every chunk of one read.
	Header []string
	// Records are the rows of this chunk. A record may be shorter than the
	// header, which means its last cells are missing; it is never longer.
	Records [][]string
	// Types is what every row read so far, this chunk included, requires of
	// each column. A format with a schema states it from the first chunk; one
	// without can only widen it as it reads, and the last chunk's is final.
	Types []infer.Type
	// Nulls, when non-nil, marks which cells hold SQL NULL rather than text
	// (Nulls[row][col]). Formats with no null of their own leave it nil.
	Nulls [][]bool
}

// Emit receives one chunk. Returning an error stops the read and is what the
// read returns.
type Emit func(*Chunk) error

// Reconcile decides what becomes of a delimited record whose field count
// differs from the header's: the record to keep, whether to drop it, or the
// error that ends the read. A nil Reconcile keeps every record as it is, which
// leaves a short one short and a long one long.
type Reconcile func(record []string, want, rowNum int) (out []string, skip bool, err error)

// Unlabeled decides what becomes of an LTSV record holding a field that is not
// a label and a value: whether to drop the record, or the error that ends the
// read. fields are the offending fields as the file wrote them, so the answer
// can quote them.
//
// LTSV names its columns on every record, so a field with no label is not a
// record of the wrong width, since there is no width to be wrong. It is the
// same event: the record carries something the table has no column for. It gets
// its own hook because the two are told apart by different questions, and
// because one answer has to be able to quote a field rather than count them.
type Unlabeled func(fields []string, rowNum int) (skip bool, err error)

// Options are the settings a read honors. The zero value reads with the
// default chunk size, keeps every record as it comes, and takes any sheet of a
// workbook.
type Options struct {
	// ChunkSize is how many rows a chunk holds. A value below one reads
	// DefaultChunkSize rows at a time; a chunk of no rows would read forever.
	ChunkSize int
	// Reconcile handles a delimited record of the wrong width.
	Reconcile Reconcile
	// Unlabeled handles an LTSV record holding a field that names no label. A
	// nil Unlabeled refuses such a record, since a read with no policy of its
	// own has only the strict answer available: the field is data the table has
	// no column for, and keeping the record would drop it without a word.
	Unlabeled Unlabeled
	// ExcelSheetPolicy decides which sheets of a workbook a read may take its
	// table from.
	ExcelSheetPolicy ExcelSheetPolicy
	// Rendering says how a value that is not already text is spelled.
	Rendering Rendering
	// maxRecord bounds one record, for a format whose records are lines. Zero
	// reads maxRecordSize; a test lowers it to reach the bound without
	// producing the whole of it.
	maxRecord int
}

// DefaultChunkSize is how many rows a chunk holds when the caller names no size.
const DefaultChunkSize = 1000

// chunkSizeOf is the chunk size a read uses, never below one.
func chunkSizeOf(opts Options) int {
	if opts.ChunkSize < 1 {
		return DefaultChunkSize
	}
	return opts.ChunkSize
}

// Result is what a read says about the table it produced.
type Result struct {
	// Header names the columns. For a format that carries no header line it is
	// what the data itself named.
	Header []string
	// Types is what every row of the input requires of each column. It is final:
	// no row was left unread when it was decided.
	Types []infer.Type
	// Rows is how many records were emitted.
	Rows int
	// Skipped is how many records Reconcile dropped, and Total how many data
	// records the input held, dropped ones included. Skipping is an instruction,
	// and an instruction that reports nothing leaves one dropped row and most of
	// the file dropped looking the same.
	Skipped int
	Total   int
	// EmptyInput reports a source that held no value at all, which JSON and
	// JSONL alone tell apart from a document saying there is nothing.
	EmptyInput bool
}

// Read reads src as format, handing each chunk of rows to emit, and returns
// what the whole input requires of its columns.
//
// The types come last because they are not known until the last row has been
// read: a column is declared once, after the whole input has been seen, so
// where a chunk boundary falls cannot change what a table holds.
//
// A read always emits at least one chunk, even for input that is nothing but a
// header, so a caller that creates its table from the first chunk always gets
// one to create it from.
func Read(src io.Reader, format Format, opts Options, emit Emit) (Result, error) {
	switch format {
	case FormatCSV, FormatTSV:
		return readDelimited(src, format, opts, emit)
	case FormatLTSV:
		return readLTSV(src, opts, emit)
	case FormatParquet:
		return readParquet(src, opts, emit)
	case FormatXLSX:
		return readXLSX(src, opts, emit)
	case FormatJSON:
		return readJSON(src, opts, emit)
	case FormatJSONL:
		return readJSONL(src, opts, emit)
	default:
		return Result{}, &Error{Kind: KindUnsupported, Msg: "unsupported file type"}
	}
}

// Kind classifies why a read failed, so a caller can report it with the
// sentinel error its own package already has. The message is separate from the
// kind for the same reason: the wording is this package's, the sentinel is the
// caller's.
type Kind int

const (
	// KindParse is input that does not describe a table of the format it claims.
	KindParse Kind = iota
	// KindEmpty is input that holds no table at all.
	KindEmpty
	// KindInvalidData is a value the format cannot hold.
	KindInvalidData
	// KindDuplicateColumn is a header that names one column twice.
	KindDuplicateColumn
	// KindUnsupported is a format this package does not read.
	KindUnsupported
)

// Error is a read that failed, and why.
type Error struct {
	// Kind is what went wrong, in a form a caller can map onto its own sentinel.
	Kind Kind
	// Msg says what went wrong, without naming a sentinel: a caller prefixes
	// its own.
	Msg string
	// Err is the cause, when the failure came from underneath.
	Err error
}

// Error renders the message and the cause under it.
func (e *Error) Error() string {
	if e.Err == nil {
		return e.Msg
	}
	return e.Msg + ": " + e.Err.Error()
}

// Unwrap returns the cause, so errors.Is reaches whatever it carries.
func (e *Error) Unwrap() error { return e.Err }

// parseError reports input that does not describe a table.
func parseError(cause error, format string, args ...any) error {
	return &Error{Kind: KindParse, Msg: fmt.Sprintf(format, args...), Err: cause}
}

// emptyError reports input holding no table at all.
func emptyError(format string, args ...any) error {
	return &Error{Kind: KindEmpty, Msg: fmt.Sprintf(format, args...)}
}

// invalidError reports a value the format cannot hold.
func invalidError(cause error, format string, args ...any) error {
	return &Error{Kind: KindInvalidData, Msg: fmt.Sprintf(format, args...), Err: cause}
}

// duplicateColumnError reports a header naming one column twice.
func duplicateColumnError(format string, args ...any) error {
	return &Error{Kind: KindDuplicateColumn, Msg: fmt.Sprintf(format, args...)}
}
