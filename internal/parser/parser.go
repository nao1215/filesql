package parser

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/nao1215/filesql/internal/codec"
	"github.com/nao1215/filesql/internal/reader"
	"github.com/nao1215/filesql/internal/textin"
)

// FileType names an input format. It says nothing about compression: a gzipped
// CSV and a plain CSV are both CSV, and a compressed stream is decompressed by
// the caller before it reaches Parse.
//
// It used to name a format and a codec together, one constant for each of the
// fifty-six combinations, which put the cross product in the type and left five
// switches to be written out by hand over all of it. Adding a codec meant seven
// new constants and an edit to every one of those switches.
type FileType int

const (
	// CSV represents CSV file type.
	CSV FileType = iota
	// TSV represents TSV file type.
	TSV
	// LTSV represents LTSV (Labeled Tab-Separated Values) file type.
	LTSV
	// Parquet represents Apache Parquet file type.
	Parquet
	// XLSX represents Excel XLSX file type.
	XLSX
	// JSON represents JSON file type.
	JSON
	// JSONL represents JSON Lines file type.
	JSONL

	// Unsupported represents an unsupported file type.
	Unsupported
)

// String returns a human-readable string representation of the FileType.
func (ft FileType) String() string {
	switch ft {
	case CSV:
		return "CSV"
	case TSV:
		return "TSV"
	case LTSV:
		return "LTSV"
	case Parquet:
		return "Parquet"
	case XLSX:
		return "XLSX"
	case JSON:
		return "JSON"
	case JSONL:
		return "JSONL"
	default:
		return "Unsupported"
	}
}

// ColumnType represents the inferred type of a column.
type ColumnType int

const (
	// TypeText represents text/string column type.
	TypeText ColumnType = iota
	// TypeInteger represents integer column type.
	TypeInteger
	// TypeReal represents floating-point column type.
	TypeReal
	// TypeDatetime represents datetime column type.
	TypeDatetime
)

// String returns the string representation of ColumnType.
func (ct ColumnType) String() string {
	switch ct {
	case TypeText:
		return "TEXT"
	case TypeInteger:
		return "INTEGER"
	case TypeReal:
		return "REAL"
	case TypeDatetime:
		return "DATETIME"
	default:
		return "TEXT"
	}
}

// TableData contains the parsed data from a file.
type TableData struct {
	// Headers contains the column names in order.
	Headers []string
	// Records contains the data rows. Each record is a slice of string values.
	Records [][]string
	// ColumnTypes contains the inferred types for each column.
	// The length matches Headers.
	ColumnTypes []ColumnType
	// Nulls marks which cells hold nothing rather than an empty value
	// (Nulls[row][column]), parallel to Records when it is not nil.
	//
	// It is nil for every format but Parquet, which is the only one here with a
	// null of its own; the rest spell a missing value as an empty field, which
	// is a value. A nil mask therefore says the format has no nulls rather than
	// that this file has none, and a caller that ignores the field reads what it
	// always read: a null renders as the empty string in Records either way.
	Nulls [][]bool
}

// ParseOption adjusts how Parse reads a source. Options only affect the formats
// they name; one that has nothing to say about the given fileType is ignored.
type ParseOption func(*parseConfig)

// parseConfig holds the settings the options above set. Its zero value is the
// behavior Parse has without any option.
type parseConfig struct {
	// excelSheetPolicy decides which sheets of a workbook Parse may take its
	// table from. The zero value takes any of them.
	excelSheetPolicy ExcelSheetPolicy
}

// WithExcelSheetPolicy decides which sheets of a workbook Parse may choose its
// one table from. It has no effect on other formats.
//
// Parse takes the first sheet the policy admits and does not read the rest, so
// this narrows the choice rather than widening what comes back.
//
// Example:
//
//	result, err := parser.Parse(f, parser.XLSX, parser.WithExcelSheetPolicy(parser.ExcelSheetPolicyVisibleOnly))
func WithExcelSheetPolicy(policy ExcelSheetPolicy) ParseOption {
	return func(cfg *parseConfig) {
		cfg.excelSheetPolicy = policy
	}
}

// readerFormat is the reader package's name for a base file type.
var readerFormats = map[FileType]reader.Format{ //nolint:gochecknoglobals // constant-like lookup table
	CSV:     reader.FormatCSV,
	TSV:     reader.FormatTSV,
	LTSV:    reader.FormatLTSV,
	Parquet: reader.FormatParquet,
	XLSX:    reader.FormatXLSX,
	JSON:    reader.FormatJSON,
	JSONL:   reader.FormatJSONL,
}

// Parse reads data from an io.Reader and returns parsed results.
// The fileType parameter specifies the format and compression of the data.
//
// A TableData is one table, so a workbook contributes one sheet: the first the
// sheet policy admits, in the workbook's own order. The others are not read and
// nothing here reports them. filesql.Open makes a table per sheet, and
// filesql.ExcelSheetsInReader says what a workbook holds without loading it.
//
// Values are spelled the way the format reads them rather than the way SQLite
// stores them, which shows in Parquet: a whole float comes back as "2" and a
// boolean as "true". A load spells the same cells for SQLite's affinity, so
// feeding Records into a CSV and loading that is not the same as loading the
// Parquet file.
//
// A compressed stream is the caller's to unwrap. Parse reads the bytes it is
// given as the format it is told, so a gzipped file is opened through
// filesql.NewCompressionFactory().CreateReaderForFile, or through
// filesql.NewCompressionHandler(...).CreateReader for a stream whose codec the
// caller already knows, and the reader that comes back is what Parse reads.
//
// A text encoding is not the caller's, and Parse reads one the way
// filesql.Open reads one. A leading byte-order mark decides it: a UTF-8
// mark is stripped rather than left in the first column name, and a UTF-16 file
// is transcoded. What follows has to be UTF-8, because a TableData holds
// characters and nothing in a byte stream says which other encoding it might
// be; a Shift-JIS file, or any stray byte, is refused with an error matching
// filesql.ErrInvalidUTF8 that names the byte and its offset. Transcode such a
// file before parsing it. Parquet and XLSX carry their own container and are
// read as bytes.
//
// Example:
//
//	f, _ := os.Open("data.csv")
//	defer f.Close()
//	result, err := parser.Parse(f, parser.CSV)
func Parse(input io.Reader, fileType FileType, opts ...ParseOption) (result *TableData, err error) {
	if input == nil {
		return nil, ErrNilReader
	}

	var cfg parseConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	format, supported := readerFormats[fileType]
	if !supported {
		return nil, ErrUnsupportedFileType
	}

	// A text source is read the way filesql.Open reads one: a leading
	// byte-order mark decides the encoding, and what follows has to be UTF-8.
	// Without this the two disagreed about the same file -- a Shift-JIS file
	// parsed here with no error into strings that are not characters, and a
	// UTF-16 file was read as single-byte data and refused for a field count,
	// which blames the caller's data for its encoding. A binary container and a
	// record format carry their own framing and are read as bytes.
	if format.IsText() {
		input = textin.Decode(input)
	}

	// Every chunk is collected, because a TableData is the whole table. Reading
	// in chunks is what a load needs, and collecting them is cheaper than the
	// reverse: a whole-table read cannot be handed out a chunk at a time.
	var headers []string
	records := [][]string{}
	var nulls [][]bool
	read, readErr := reader.Read(input, format, reader.Options{
		Reconcile:        strictFieldCount(fileType),
		ExcelSheetPolicy: cfg.excelSheetPolicy,
	}, func(chunk *reader.Chunk) error {
		headers = chunk.Header
		records = append(records, chunk.Records...)
		// A chunk carries a mask only for a format that has a null, and the
		// mask has to stay parallel to the records it belongs to: a chunk
		// without one contributes as many unmarked rows as it holds, so the
		// two do not slide apart across a chunk boundary.
		if chunk.Nulls != nil {
			for len(nulls) < len(records)-len(chunk.Records) {
				nulls = append(nulls, nil)
			}
			nulls = append(nulls, chunk.Nulls...)
		}
		return nil
	})
	if readErr != nil {
		return nil, parseError(readErr)
	}
	// JSON and JSONL alone tell a document holding nothing from one saying there
	// is nothing, and this package has always reported the first as an error.
	if read.EmptyInput {
		return nil, &emptyInputError{err: fmt.Errorf("empty %s data", fileType)}
	}

	return &TableData{
		Headers:     headers,
		Records:     records,
		ColumnTypes: columnTypesOf(read.Types),
		Nulls:       nulls,
	}, nil
}

// strictFieldCount refuses a delimited record whose field count differs from
// the header's, which is what every reader of a TableData needs: everything
// downstream reads a record by header position, so a record of another length
// is a table nothing can use.
//
// It is nil for the other formats, which settle their own widths.
func strictFieldCount(baseType FileType) reader.Reconcile {
	if baseType != CSV && baseType != TSV {
		return nil
	}
	syntaxError := ErrCSVSyntax
	if baseType == TSV {
		syntaxError = ErrTSVSyntax
	}
	return func(record []string, want, rowNum int) ([]string, bool, error) {
		return nil, false, fmt.Errorf("%w: record on line %d has %d fields, the header has %d",
			syntaxError, rowNum+1, len(record), want)
	}
}

// parseError gives a failed read this package's wording and sentinels for it.
// The reader reports what went wrong as a Kind rather than as an error a caller
// of this package would recognize, so the mapping happens here.
func parseError(err error) error {
	var readErr *reader.Error
	if !errors.As(err, &readErr) {
		return err
	}
	switch readErr.Kind {
	case reader.KindDuplicateColumn:
		return fmt.Errorf("duplicate column name: %s", readErr.Error())
	case reader.KindEmpty:
		return &emptyInputError{err: err}
	default:
		return err
	}
}

// File extensions
const (
	extCSV     = ".csv"
	extTSV     = ".tsv"
	extLTSV    = ".ltsv"
	extParquet = ".parquet"
	extXLSX    = ".xlsx"
	extJSON    = ".json"
	extJSONL   = ".jsonl"
)

// DetectFileType reports the format a path names, looking through any
// compression extension: "data.csv" and "data.csv.gz" are both CSV, since the
// codec says how to read the bytes and not what they spell. A path naming no
// format this package reads answers Unsupported.
func DetectFileType(path string) FileType {
	// Which codec a path names is the codec package's to say. Taking it off is
	// all this needs, because the format is what is under it.
	_, basePath := codec.FromPath(path)

	switch strings.ToLower(filepath.Ext(basePath)) {
	case extCSV:
		return CSV
	case extTSV:
		return TSV
	case extLTSV:
		return LTSV
	case extParquet:
		return Parquet
	case extXLSX:
		return XLSX
	case extJSON:
		return JSON
	case extJSONL:
		return JSONL
	default:
		return Unsupported
	}
}
