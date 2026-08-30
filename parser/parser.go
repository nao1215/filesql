package parser

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/nao1215/filesql/internal/codec"
	"github.com/nao1215/filesql/internal/reader"
)

// FileType represents supported file types including compression variants.
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

	// CSVGZ represents gzip-compressed CSV file type.
	CSVGZ
	// CSVBZ2 represents bzip2-compressed CSV file type.
	CSVBZ2
	// CSVXZ represents xz-compressed CSV file type.
	CSVXZ
	// CSVZSTD represents zstd-compressed CSV file type.
	CSVZSTD

	// TSVGZ represents gzip-compressed TSV file type.
	TSVGZ
	// TSVBZ2 represents bzip2-compressed TSV file type.
	TSVBZ2
	// TSVXZ represents xz-compressed TSV file type.
	TSVXZ
	// TSVZSTD represents zstd-compressed TSV file type.
	TSVZSTD

	// LTSVGZ represents gzip-compressed LTSV file type.
	LTSVGZ
	// LTSVBZ2 represents bzip2-compressed LTSV file type.
	LTSVBZ2
	// LTSVXZ represents xz-compressed LTSV file type.
	LTSVXZ
	// LTSVZSTD represents zstd-compressed LTSV file type.
	LTSVZSTD

	// ParquetGZ represents gzip-compressed Parquet file type.
	ParquetGZ
	// ParquetBZ2 represents bzip2-compressed Parquet file type.
	ParquetBZ2
	// ParquetXZ represents xz-compressed Parquet file type.
	ParquetXZ
	// ParquetZSTD represents zstd-compressed Parquet file type.
	ParquetZSTD

	// XLSXGZ represents gzip-compressed XLSX file type.
	XLSXGZ
	// XLSXBZ2 represents bzip2-compressed XLSX file type.
	XLSXBZ2
	// XLSXXZ represents xz-compressed XLSX file type.
	XLSXXZ
	// XLSXZSTD represents zstd-compressed XLSX file type.
	XLSXZSTD

	// CSVZLIB represents zlib-compressed CSV file type.
	CSVZLIB
	// TSVZLIB represents zlib-compressed TSV file type.
	TSVZLIB
	// LTSVZLIB represents zlib-compressed LTSV file type.
	LTSVZLIB
	// ParquetZLIB represents zlib-compressed Parquet file type.
	ParquetZLIB
	// XLSXZLIB represents zlib-compressed XLSX file type.
	XLSXZLIB

	// CSVSNAPPY represents snappy-compressed CSV file type.
	CSVSNAPPY
	// TSVSNAPPY represents snappy-compressed TSV file type.
	TSVSNAPPY
	// LTSVSNAPPY represents snappy-compressed LTSV file type.
	LTSVSNAPPY
	// ParquetSNAPPY represents snappy-compressed Parquet file type.
	ParquetSNAPPY
	// XLSXSNAPPY represents snappy-compressed XLSX file type.
	XLSXSNAPPY

	// CSVS2 represents s2-compressed CSV file type.
	CSVS2
	// TSVS2 represents s2-compressed TSV file type.
	TSVS2
	// LTSVS2 represents s2-compressed LTSV file type.
	LTSVS2
	// ParquetS2 represents s2-compressed Parquet file type.
	ParquetS2
	// XLSXS2 represents s2-compressed XLSX file type.
	XLSXS2

	// CSVLZ4 represents lz4-compressed CSV file type.
	CSVLZ4
	// TSVLZ4 represents lz4-compressed TSV file type.
	TSVLZ4
	// LTSVLZ4 represents lz4-compressed LTSV file type.
	LTSVLZ4
	// ParquetLZ4 represents lz4-compressed Parquet file type.
	ParquetLZ4
	// XLSXLZ4 represents lz4-compressed XLSX file type.
	XLSXLZ4

	// JSONGZ represents gzip-compressed JSON file type.
	JSONGZ
	// JSONBZ2 represents bzip2-compressed JSON file type.
	JSONBZ2
	// JSONXZ represents xz-compressed JSON file type.
	JSONXZ
	// JSONZSTD represents zstd-compressed JSON file type.
	JSONZSTD
	// JSONZLIB represents zlib-compressed JSON file type.
	JSONZLIB
	// JSONSNAPPY represents snappy-compressed JSON file type.
	JSONSNAPPY
	// JSONS2 represents s2-compressed JSON file type.
	JSONS2
	// JSONLZ4 represents lz4-compressed JSON file type.
	JSONLZ4

	// JSONLGZ represents gzip-compressed JSONL file type.
	JSONLGZ
	// JSONLBZ2 represents bzip2-compressed JSONL file type.
	JSONLBZ2
	// JSONLXZ represents xz-compressed JSONL file type.
	JSONLXZ
	// JSONLZSTD represents zstd-compressed JSONL file type.
	JSONLZSTD
	// JSONLZLIB represents zlib-compressed JSONL file type.
	JSONLZLIB
	// JSONLSNAPPY represents snappy-compressed JSONL file type.
	JSONLSNAPPY
	// JSONLS2 represents s2-compressed JSONL file type.
	JSONLS2
	// JSONLLZ4 represents lz4-compressed JSONL file type.
	JSONLLZ4

	// Unsupported represents unsupported file type.
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
	case CSVGZ:
		return "CSV (gzip)"
	case CSVBZ2:
		return "CSV (bzip2)"
	case CSVXZ:
		return "CSV (xz)"
	case CSVZSTD:
		return "CSV (zstd)"
	case TSVGZ:
		return "TSV (gzip)"
	case TSVBZ2:
		return "TSV (bzip2)"
	case TSVXZ:
		return "TSV (xz)"
	case TSVZSTD:
		return "TSV (zstd)"
	case LTSVGZ:
		return "LTSV (gzip)"
	case LTSVBZ2:
		return "LTSV (bzip2)"
	case LTSVXZ:
		return "LTSV (xz)"
	case LTSVZSTD:
		return "LTSV (zstd)"
	case ParquetGZ:
		return "Parquet (gzip)"
	case ParquetBZ2:
		return "Parquet (bzip2)"
	case ParquetXZ:
		return "Parquet (xz)"
	case ParquetZSTD:
		return "Parquet (zstd)"
	case XLSXGZ:
		return "XLSX (gzip)"
	case XLSXBZ2:
		return "XLSX (bzip2)"
	case XLSXXZ:
		return "XLSX (xz)"
	case XLSXZSTD:
		return "XLSX (zstd)"
	case CSVZLIB:
		return "CSV (zlib)"
	case TSVZLIB:
		return "TSV (zlib)"
	case LTSVZLIB:
		return "LTSV (zlib)"
	case ParquetZLIB:
		return "Parquet (zlib)"
	case XLSXZLIB:
		return "XLSX (zlib)"
	case CSVSNAPPY:
		return "CSV (snappy)"
	case TSVSNAPPY:
		return "TSV (snappy)"
	case LTSVSNAPPY:
		return "LTSV (snappy)"
	case ParquetSNAPPY:
		return "Parquet (snappy)"
	case XLSXSNAPPY:
		return "XLSX (snappy)"
	case CSVS2:
		return "CSV (s2)"
	case TSVS2:
		return "TSV (s2)"
	case LTSVS2:
		return "LTSV (s2)"
	case ParquetS2:
		return "Parquet (s2)"
	case XLSXS2:
		return "XLSX (s2)"
	case CSVLZ4:
		return "CSV (lz4)"
	case TSVLZ4:
		return "TSV (lz4)"
	case LTSVLZ4:
		return "LTSV (lz4)"
	case ParquetLZ4:
		return "Parquet (lz4)"
	case XLSXLZ4:
		return "XLSX (lz4)"
	case JSON:
		return "JSON"
	case JSONL:
		return "JSONL"
	case JSONGZ:
		return "JSON (gzip)"
	case JSONBZ2:
		return "JSON (bzip2)"
	case JSONXZ:
		return "JSON (xz)"
	case JSONZSTD:
		return "JSON (zstd)"
	case JSONZLIB:
		return "JSON (zlib)"
	case JSONSNAPPY:
		return "JSON (snappy)"
	case JSONS2:
		return "JSON (s2)"
	case JSONLZ4:
		return "JSON (lz4)"
	case JSONLGZ:
		return "JSONL (gzip)"
	case JSONLBZ2:
		return "JSONL (bzip2)"
	case JSONLXZ:
		return "JSONL (xz)"
	case JSONLZSTD:
		return "JSONL (zstd)"
	case JSONLZLIB:
		return "JSONL (zlib)"
	case JSONLSNAPPY:
		return "JSONL (snappy)"
	case JSONLS2:
		return "JSONL (s2)"
	case JSONLLZ4:
		return "JSONL (lz4)"
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
// nothing here reports them. filesql.OpenContext makes a table per sheet, and
// filesql.ExcelSheetsInReader says what a workbook holds without loading it.
//
// Values are spelled the way the format reads them rather than the way SQLite
// stores them, which shows in Parquet: a whole float comes back as "2" and a
// boolean as "true". A load spells the same cells for SQLite's affinity, so
// feeding Records into a CSV and loading that is not the same as loading the
// Parquet file.
//
// Example:
//
//	f, _ := os.Open("data.csv.gz")
//	defer f.Close()
//	result, err := parser.Parse(f, parser.CSVGZ)
func Parse(input io.Reader, fileType FileType, opts ...ParseOption) (result *TableData, err error) {
	if input == nil {
		return nil, ErrNilReader
	}

	var cfg parseConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	baseType := BaseFileType(fileType)
	format, supported := readerFormats[baseType]
	if !supported {
		return nil, ErrUnsupportedFileType
	}

	decompressed, closeFunc, decompErr := createDecompressedReader(input, fileType)
	if decompErr != nil {
		return nil, fmt.Errorf("failed to decompress: %w", decompErr)
	}
	defer func() {
		if closeErr := closeFunc(); closeErr != nil && err == nil {
			err = fmt.Errorf("failed to close decompressor: %w", closeErr)
		}
	}()

	// Every chunk is collected, because a TableData is the whole table. Reading
	// in chunks is what a load needs, and collecting them is cheaper than the
	// reverse: a whole-table read cannot be handed out a chunk at a time.
	var headers []string
	records := [][]string{}
	var nulls [][]bool
	read, readErr := reader.Read(decompressed, format, reader.Options{
		Reconcile:        strictFieldCount(baseType),
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
		return nil, &emptyInputError{err: fmt.Errorf("empty %s data", baseType)}
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

// Compression extensions: ".gz", ".bz2", ".xz", ".zst", ".z", ".snappy", ".s2"
// and ".lz4". Each is named by the codec that reads and writes it, so the two
// spellings cannot drift apart.
const (
	extGZ     = codec.ExtGZ
	extBZ2    = codec.ExtBZ2
	extXZ     = codec.ExtXZ
	extZSTD   = codec.ExtZSTD
	extZLIB   = codec.ExtZLIB
	extSNAPPY = codec.ExtSNAPPY
	extS2     = codec.ExtS2
	extLZ4    = codec.ExtLZ4
)

// Compression type identifiers
const (
	compGZ     = "gz"
	compBZ2    = "bz2"
	compXZ     = "xz"
	compZSTD   = "zstd"
	compZLIB   = "zlib"
	compSNAPPY = "snappy"
	compS2     = "s2"
	compLZ4    = "lz4"
)

// DetectFileType detects file type from path extension, including compression variants.
func DetectFileType(path string) FileType {
	basePath := path
	var compressionType string

	// Remove compression extensions
	lowerPath := strings.ToLower(path)
	switch {
	case strings.HasSuffix(lowerPath, extGZ):
		basePath = path[:len(path)-len(extGZ)]
		compressionType = compGZ
	case strings.HasSuffix(lowerPath, extBZ2):
		basePath = path[:len(path)-len(extBZ2)]
		compressionType = compBZ2
	case strings.HasSuffix(lowerPath, extXZ):
		basePath = path[:len(path)-len(extXZ)]
		compressionType = compXZ
	case strings.HasSuffix(lowerPath, extZSTD):
		basePath = path[:len(path)-len(extZSTD)]
		compressionType = compZSTD
	case strings.HasSuffix(lowerPath, extZLIB):
		basePath = path[:len(path)-len(extZLIB)]
		compressionType = compZLIB
	case strings.HasSuffix(lowerPath, extSNAPPY):
		basePath = path[:len(path)-len(extSNAPPY)]
		compressionType = compSNAPPY
	case strings.HasSuffix(lowerPath, extS2):
		basePath = path[:len(path)-len(extS2)]
		compressionType = compS2
	case strings.HasSuffix(lowerPath, extLZ4):
		basePath = path[:len(path)-len(extLZ4)]
		compressionType = compLZ4
	}

	ext := strings.ToLower(filepath.Ext(basePath))
	switch ext {
	case extCSV:
		switch compressionType {
		case compGZ:
			return CSVGZ
		case compBZ2:
			return CSVBZ2
		case compXZ:
			return CSVXZ
		case compZSTD:
			return CSVZSTD
		case compZLIB:
			return CSVZLIB
		case compSNAPPY:
			return CSVSNAPPY
		case compS2:
			return CSVS2
		case compLZ4:
			return CSVLZ4
		default:
			return CSV
		}
	case extTSV:
		switch compressionType {
		case compGZ:
			return TSVGZ
		case compBZ2:
			return TSVBZ2
		case compXZ:
			return TSVXZ
		case compZSTD:
			return TSVZSTD
		case compZLIB:
			return TSVZLIB
		case compSNAPPY:
			return TSVSNAPPY
		case compS2:
			return TSVS2
		case compLZ4:
			return TSVLZ4
		default:
			return TSV
		}
	case extLTSV:
		switch compressionType {
		case compGZ:
			return LTSVGZ
		case compBZ2:
			return LTSVBZ2
		case compXZ:
			return LTSVXZ
		case compZSTD:
			return LTSVZSTD
		case compZLIB:
			return LTSVZLIB
		case compSNAPPY:
			return LTSVSNAPPY
		case compS2:
			return LTSVS2
		case compLZ4:
			return LTSVLZ4
		default:
			return LTSV
		}
	case extParquet:
		switch compressionType {
		case compGZ:
			return ParquetGZ
		case compBZ2:
			return ParquetBZ2
		case compXZ:
			return ParquetXZ
		case compZSTD:
			return ParquetZSTD
		case compZLIB:
			return ParquetZLIB
		case compSNAPPY:
			return ParquetSNAPPY
		case compS2:
			return ParquetS2
		case compLZ4:
			return ParquetLZ4
		default:
			return Parquet
		}
	case extXLSX:
		switch compressionType {
		case compGZ:
			return XLSXGZ
		case compBZ2:
			return XLSXBZ2
		case compXZ:
			return XLSXXZ
		case compZSTD:
			return XLSXZSTD
		case compZLIB:
			return XLSXZLIB
		case compSNAPPY:
			return XLSXSNAPPY
		case compS2:
			return XLSXS2
		case compLZ4:
			return XLSXLZ4
		default:
			return XLSX
		}
	case extJSON:
		switch compressionType {
		case compGZ:
			return JSONGZ
		case compBZ2:
			return JSONBZ2
		case compXZ:
			return JSONXZ
		case compZSTD:
			return JSONZSTD
		case compZLIB:
			return JSONZLIB
		case compSNAPPY:
			return JSONSNAPPY
		case compS2:
			return JSONS2
		case compLZ4:
			return JSONLZ4
		default:
			return JSON
		}
	case extJSONL:
		switch compressionType {
		case compGZ:
			return JSONLGZ
		case compBZ2:
			return JSONLBZ2
		case compXZ:
			return JSONLXZ
		case compZSTD:
			return JSONLZSTD
		case compZLIB:
			return JSONLZLIB
		case compSNAPPY:
			return JSONLSNAPPY
		case compS2:
			return JSONLS2
		case compLZ4:
			return JSONLLZ4
		default:
			return JSONL
		}
	default:
		return Unsupported
	}
}

// IsCompressed returns true if the file type is compressed.
func IsCompressed(ft FileType) bool {
	switch ft {
	case CSVGZ, CSVBZ2, CSVXZ, CSVZSTD, CSVZLIB, CSVSNAPPY, CSVS2, CSVLZ4,
		TSVGZ, TSVBZ2, TSVXZ, TSVZSTD, TSVZLIB, TSVSNAPPY, TSVS2, TSVLZ4,
		LTSVGZ, LTSVBZ2, LTSVXZ, LTSVZSTD, LTSVZLIB, LTSVSNAPPY, LTSVS2, LTSVLZ4,
		ParquetGZ, ParquetBZ2, ParquetXZ, ParquetZSTD, ParquetZLIB, ParquetSNAPPY, ParquetS2, ParquetLZ4,
		XLSXGZ, XLSXBZ2, XLSXXZ, XLSXZSTD, XLSXZLIB, XLSXSNAPPY, XLSXS2, XLSXLZ4,
		JSONGZ, JSONBZ2, JSONXZ, JSONZSTD, JSONZLIB, JSONSNAPPY, JSONS2, JSONLZ4,
		JSONLGZ, JSONLBZ2, JSONLXZ, JSONLZSTD, JSONLZLIB, JSONLSNAPPY, JSONLS2, JSONLLZ4:
		return true
	default:
		return false
	}
}

// BaseFileType returns the base file type without compression.
func BaseFileType(ft FileType) FileType {
	switch ft {
	case CSV, CSVGZ, CSVBZ2, CSVXZ, CSVZSTD, CSVZLIB, CSVSNAPPY, CSVS2, CSVLZ4:
		return CSV
	case TSV, TSVGZ, TSVBZ2, TSVXZ, TSVZSTD, TSVZLIB, TSVSNAPPY, TSVS2, TSVLZ4:
		return TSV
	case LTSV, LTSVGZ, LTSVBZ2, LTSVXZ, LTSVZSTD, LTSVZLIB, LTSVSNAPPY, LTSVS2, LTSVLZ4:
		return LTSV
	case Parquet, ParquetGZ, ParquetBZ2, ParquetXZ, ParquetZSTD, ParquetZLIB, ParquetSNAPPY, ParquetS2, ParquetLZ4:
		return Parquet
	case XLSX, XLSXGZ, XLSXBZ2, XLSXXZ, XLSXZSTD, XLSXZLIB, XLSXSNAPPY, XLSXS2, XLSXLZ4:
		return XLSX
	case JSON, JSONGZ, JSONBZ2, JSONXZ, JSONZSTD, JSONZLIB, JSONSNAPPY, JSONS2, JSONLZ4:
		return JSON
	case JSONL, JSONLGZ, JSONLBZ2, JSONLXZ, JSONLZSTD, JSONLZLIB, JSONLSNAPPY, JSONLS2, JSONLLZ4:
		return JSONL
	default:
		return Unsupported
	}
}

// codecOf is the compression a fused file type carries.
func codecOf(fileType FileType) codec.Codec {
	switch fileType {
	case CSVGZ, TSVGZ, LTSVGZ, XLSXGZ, ParquetGZ, JSONGZ, JSONLGZ:
		return codec.GZ
	case CSVBZ2, TSVBZ2, LTSVBZ2, XLSXBZ2, ParquetBZ2, JSONBZ2, JSONLBZ2:
		return codec.BZ2
	case CSVXZ, TSVXZ, LTSVXZ, XLSXXZ, ParquetXZ, JSONXZ, JSONLXZ:
		return codec.XZ
	case CSVZSTD, TSVZSTD, LTSVZSTD, XLSXZSTD, ParquetZSTD, JSONZSTD, JSONLZSTD:
		return codec.ZSTD
	case CSVZLIB, TSVZLIB, LTSVZLIB, XLSXZLIB, ParquetZLIB, JSONZLIB, JSONLZLIB:
		return codec.ZLIB
	case CSVSNAPPY, TSVSNAPPY, LTSVSNAPPY, XLSXSNAPPY, ParquetSNAPPY, JSONSNAPPY, JSONLSNAPPY:
		return codec.SNAPPY
	case CSVS2, TSVS2, LTSVS2, XLSXS2, ParquetS2, JSONS2, JSONLS2:
		return codec.S2
	case CSVLZ4, TSVLZ4, LTSVLZ4, XLSXLZ4, ParquetLZ4, JSONLZ4, JSONLLZ4:
		return codec.LZ4
	default:
		return codec.None
	}
}

// createDecompressedReader wraps the reader with appropriate decompression.
func createDecompressedReader(reader io.Reader, fileType FileType) (io.Reader, func() error, error) {
	return codecOf(fileType).NewReader(reader)
}
