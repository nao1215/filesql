package filesql

import (
	"fmt"
	"io"
	"strings"

	"github.com/nao1215/filesql/parser"
)

// FileType names an input format. It says nothing about compression: a
// gzipped CSV and a plain CSV are both FileTypeCSV, and the codec wrapping a
// stream is stated separately with [WithCompression].
type FileType int

// String returns a human-readable string representation of the FileType.
func (ft FileType) String() string {
	switch ft {
	case FileTypeACH:
		return "ACH"
	case FileTypeFedWire:
		return "FedWire"
	default:
		return parserFileType(ft).String()
	}
}

const (
	// FileTypeCSV represents CSV file type
	FileTypeCSV FileType = iota
	// FileTypeTSV represents TSV file type
	FileTypeTSV
	// FileTypeLTSV represents LTSV file type
	FileTypeLTSV
	// FileTypeParquet represents Parquet file type
	FileTypeParquet
	// FileTypeXLSX represents Excel XLSX file type
	FileTypeXLSX
	// FileTypeJSON represents JSON file type
	FileTypeJSON
	// FileTypeJSONL represents JSON Lines file type
	FileTypeJSONL
	// FileTypeACH represents ACH (NACHA) file type
	FileTypeACH
	// FileTypeFedWire represents Fedwire file type
	FileTypeFedWire

	// FileTypeUnsupported represents unsupported file type
	FileTypeUnsupported
)

// File extension aliases from the parser package
const (
	extCSV     = parser.ExtCSV
	extTSV     = parser.ExtTSV
	extLTSV    = parser.ExtLTSV
	extParquet = parser.ExtParquet
	extXLSX    = parser.ExtXLSX
	extGZ      = parser.ExtGZ
	extBZ2     = parser.ExtBZ2
	extXZ      = parser.ExtXZ
	extZSTD    = parser.ExtZSTD
	extZLIB    = parser.ExtZLIB
	extSNAPPY  = parser.ExtSNAPPY
	extS2      = parser.ExtS2
	extLZ4     = parser.ExtLZ4
	extJSON    = parser.ExtJSON
	extJSONL   = parser.ExtJSONL
)

// file represents a file that can be converted to table
type file struct {
	path     string
	fileType FileType
	// excelSheetPolicy decides which sheets of a workbook toTable may take its
	// table from. The zero value is ExcelSheetPolicyAll.
	excelSheetPolicy ExcelSheetPolicy
}

// tableChunk represents a chunk of table data for streaming processing
type tableChunk struct {
	tableName  string
	headers    header
	records    []record
	columnInfo []columnInfo
	// nulls, when non-nil, marks which cells were SQL NULL (nulls[row][col]).
	// Formats without a null concept leave it nil, and the insert then treats
	// every cell as a string. Parquet sets it so a stored null reloads as SQL NULL
	// rather than an empty string.
	nulls [][]bool
}

// getTableName returns the name of the table
func (tc *tableChunk) getTableName() string {
	return tc.tableName
}

// getHeaders returns the table headers
func (tc *tableChunk) getHeaders() header {
	return tc.headers
}

// getRecords returns the records in this chunk
func (tc *tableChunk) getRecords() []record {
	return tc.records
}

// getColumnInfo returns the column information with inferred types
func (tc *tableChunk) getColumnInfo() []columnInfo {
	return tc.columnInfo
}

// getNulls returns the per-cell NULL mask, or nil when the source format has no
// null concept.
func (tc *tableChunk) getNulls() [][]bool {
	return tc.nulls
}

// chunkProcessor is a function type for processing table chunks
type chunkProcessor func(chunk *tableChunk) error

// streamingParser represents a parser that can read from io.Reader directly
type streamingParser struct {
	fileType FileType
	// compression is the codec wrapping the reader. A reader carries no path,
	// so the caller states it; files answer CompressionNone because the file
	// path is unwrapped before the parser sees it.
	compression CompressionType
	tableName   string
	chunkSize   chunkSizeValue
	memoryPool  *memoryPool  // Pool for reusable memory allocations
	memoryLimit *memoryLimit // Configurable memory limits
	// malformedRowPolicy controls how a CSV/TSV record whose field count differs
	// from the header is handled. The zero value is MalformedRowStop.
	malformedRowPolicy MalformedRowPolicy
	// excelSheetPolicy controls which sheets of a workbook this parser may take
	// its table from. The zero value is ExcelSheetPolicyAll.
	excelSheetPolicy ExcelSheetPolicy
	// skippedRows and totalRows record what MalformedRowSkip discarded and how
	// many data rows it was choosing from, so a caller can say how much of the
	// file it is holding. Skipping is an instruction, and an instruction that
	// reports nothing leaves one dropped row and most of the file dropped
	// looking the same.
	skippedRows int
	totalRows   int
}

// newFile creates a new file
func newFile(path string) *file {
	return &file{
		path:     path,
		fileType: detectFileType(path),
	}
}

// supportedFileExtPatterns returns all supported file patterns for glob matching
func supportedFileExtPatterns() []string {
	baseExts := []string{extCSV, extTSV, extLTSV, extParquet, extXLSX, extJSON, extJSONL}
	compressionExts := []string{"", extGZ, extBZ2, extXZ, extZSTD, extZLIB, extSNAPPY, extS2, extLZ4}

	patterns := make([]string, 0, len(compressionExts)*len(baseExts)+2)
	for _, baseExt := range baseExts {
		for _, compressionExt := range compressionExts {
			pattern := "*" + baseExt + compressionExt
			patterns = append(patterns, pattern)
		}
	}

	// ACH and Fedwire have no compression variants
	patterns = append(patterns, "*"+extACH, "*"+extFED)

	return patterns
}

// isSupportedFile checks if the file has a supported extension
func isSupportedFile(fileName string) bool {
	return detectFileType(fileName) != FileTypeUnsupported
}

// isSupportedExtension checks if the given extension is supported
// The extension should start with a dot (e.g., ".csv", ".tsv.gz")
func isSupportedExtension(ext string) bool {
	ext = strings.ToLower(ext)

	// Check if it's a simple extension or has compression
	return isSupportedFile("file" + ext)
}

// extension returns the file extension for the FileType. The codec's suffix is
// not part of it: a gzipped CSV is FileTypeCSV plus CompressionGZ.Extension().
func (ft FileType) extension() string {
	switch ft {
	case FileTypeCSV:
		return extCSV
	case FileTypeTSV:
		return extTSV
	case FileTypeLTSV:
		return extLTSV
	case FileTypeParquet:
		return extParquet
	case FileTypeXLSX:
		return extXLSX
	case FileTypeJSON:
		return extJSON
	case FileTypeJSONL:
		return extJSONL
	case FileTypeACH:
		return extACH
	case FileTypeFedWire:
		return extFED
	default:
		return ""
	}
}

// getFileExtension returns the file extension for a given FileType
// Deprecated: Use FileType.extension() method instead
func getFileExtension(fileType FileType) string {
	return fileType.extension()
}

// getPath returns file path
func (f *file) getPath() string {
	return f.path
}

// getFileType returns file type
func (f *file) getFileType() FileType {
	return f.fileType
}

// isCSV returns true if the file is CSV format
func (f *file) isCSV() bool {
	return f.getFileType() == FileTypeCSV
}

// isTSV returns true if the file is TSV format
func (f *file) isTSV() bool {
	return f.getFileType() == FileTypeTSV
}

// isLTSV returns true if the file is LTSV format
func (f *file) isLTSV() bool {
	return f.getFileType() == FileTypeLTSV
}

// isXLSX returns true if the file is XLSX format
func (f *file) isXLSX() bool {
	return f.getFileType() == FileTypeXLSX
}

// isCompressed returns true if file is compressed
func (f *file) isCompressed() bool {
	return f.isGZ() || f.isBZ2() || f.isXZ() || f.isZSTD() || f.isZLIB() || f.isSNAPPY() || f.isS2() || f.isLZ4()
}

// isGZ returns true if file is gzip compressed
func (f *file) isGZ() bool {
	return strings.HasSuffix(f.path, extGZ)
}

// isBZ2 returns true if file is bzip2 compressed
func (f *file) isBZ2() bool {
	return strings.HasSuffix(f.path, extBZ2)
}

// isXZ returns true if file is xz compressed
func (f *file) isXZ() bool {
	return strings.HasSuffix(f.path, extXZ)
}

// isZSTD returns true if file is zstd compressed
func (f *file) isZSTD() bool {
	return strings.HasSuffix(f.path, extZSTD)
}

// isZLIB returns true if file is zlib compressed
func (f *file) isZLIB() bool {
	return strings.HasSuffix(f.path, extZLIB)
}

// isSNAPPY returns true if file is snappy compressed
func (f *file) isSNAPPY() bool {
	return strings.HasSuffix(f.path, extSNAPPY)
}

// isS2 returns true if file is s2 compressed
func (f *file) isS2() bool {
	return strings.HasSuffix(f.path, extS2)
}

// isLZ4 returns true if file is lz4 compressed
func (f *file) isLZ4() bool {
	return strings.HasSuffix(f.path, extLZ4)
}

// toTable converts file to table structure.
//
// The reader is opened through openReader so the codec is unwrapped here. The
// parser is handed the format's own bytes, because fileType names the format
// only and no longer tells the parser what to decompress.
func (f *file) toTable() (*table, error) {
	reader, closeReader, err := f.openReader()
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", f.path, err)
	}
	defer handleCloseError(closeReader)()

	tableName := sanitizeTableName(tableFromFilePath(f.path))
	return parseWithParser(reader, f.fileType, tableName, f.excelSheetPolicy)
}

// detectFileType detects file type from extension, considering compressed files
func detectFileType(path string) FileType {
	// Check for ACH/Fedwire first (no compression support)
	if isACHFile(path) {
		return FileTypeACH
	}
	if isFedWireFile(path) {
		return FileTypeFedWire
	}

	return filesqlFileType(parser.DetectFileType(path))
}

// openReader opens file and returns a reader that handles compression
func (f *file) openReader() (io.Reader, func() error, error) {
	factory := NewCompressionFactory()
	return factory.CreateReaderForFile(f.path)
}

// convertXLSXRowsToTable converts XLSX rows to table headers and records
// First row becomes headers, remaining rows become records with padding
func convertXLSXRowsToTable(rows [][]string) (header, []record, error) {
	var headers header
	var records []record

	// First row as headers
	if len(rows) > 0 {
		headers = make(header, len(rows[0]))
		copy(headers, rows[0])
	}

	// Remaining rows as records
	if len(rows) > 1 {
		records = make([]record, len(rows)-1)
		for i, row := range rows[1:] {
			// A workbook stores no cell for a trailing empty one, so a row
			// ending in blanks arrives short and the padding says what it
			// means. More cells than the header has means the opposite — there
			// is data in a column the header does not name — and truncating it
			// dropped that data with no error and no count to say so.
			if len(row) > len(headers) {
				return nil, nil, fmt.Errorf("%w: row %d has %d cells where the header has %d",
					ErrParsing, i+2, len(row), len(headers))
			}
			record := make(record, len(headers))
			copy(record, row)
			records[i] = record
		}
	}

	return headers, records, nil
}
