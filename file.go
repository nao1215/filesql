package filesql

import (
	"strings"

	"github.com/nao1215/filesql/internal/codec"
	"github.com/nao1215/filesql/internal/parser"
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

// File extensions of the formats this package loads and writes.
const (
	extCSV     = ".csv"
	extTSV     = ".tsv"
	extLTSV    = ".ltsv"
	extParquet = ".parquet"
	extXLSX    = ".xlsx"
	extJSON    = ".json"
	extJSONL   = ".jsonl"
)

// file represents a file that can be converted to table
type file struct {
	path     string
	fileType FileType
}

// tableChunk is one run of a table's rows on its way to the database.
type tableChunk struct {
	tableName string
	headers   header
	records   []record
	// types is what every row read so far, this chunk included, requires of
	// each column. A format with a schema states it from the first chunk; one
	// without can only widen it as it reads, and the last chunk's is final.
	types columnInfoList
	// nulls, when non-nil, marks which cells were SQL NULL (nulls[row][col]).
	// Formats without a null concept leave it nil, and the insert then treats
	// every cell as a string. Parquet sets it so a stored null reloads as SQL NULL
	// rather than an empty string.
	nulls [][]bool
}

// getHeaders returns the table headers
func (tc *tableChunk) getHeaders() header {
	return tc.headers
}

// getRecords returns the records in this chunk
func (tc *tableChunk) getRecords() []record {
	return tc.records
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

	patterns := make([]string, 0, (len(codec.All)+1)*len(baseExts)+2)
	for _, baseExt := range baseExts {
		// The uncompressed form first, then one per codec.
		patterns = append(patterns, "*"+baseExt)
		for _, c := range codec.All {
			patterns = append(patterns, "*"+baseExt+c.Extension())
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

// getPath returns file path
func (f *file) getPath() string {
	return f.path
}

// getFileType returns file type
func (f *file) getFileType() FileType {
	return f.fileType
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
