package filesql

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/nao1215/filesql/parser"
)

// FileType represents supported file types including compression variants
type FileType int

// String returns a human-readable string representation of the FileType.
func (ft FileType) String() string {
	switch ft {
	case FileTypeCSV:
		return fileTypeNameCSV
	case FileTypeTSV:
		return fileTypeNameTSV
	case FileTypeLTSV:
		return fileTypeNameLTSV
	case FileTypeParquet:
		return fileTypeNameParquet
	case FileTypeXLSX:
		return fileTypeNameXLSX
	case FileTypeCSVGZ:
		return "CSV (gzip)"
	case FileTypeCSVBZ2:
		return "CSV (bzip2)"
	case FileTypeCSVXZ:
		return "CSV (xz)"
	case FileTypeCSVZSTD:
		return "CSV (zstd)"
	case FileTypeTSVGZ:
		return "TSV (gzip)"
	case FileTypeTSVBZ2:
		return "TSV (bzip2)"
	case FileTypeTSVXZ:
		return "TSV (xz)"
	case FileTypeTSVZSTD:
		return "TSV (zstd)"
	case FileTypeLTSVGZ:
		return "LTSV (gzip)"
	case FileTypeLTSVBZ2:
		return "LTSV (bzip2)"
	case FileTypeLTSVXZ:
		return "LTSV (xz)"
	case FileTypeLTSVZSTD:
		return "LTSV (zstd)"
	case FileTypeParquetGZ:
		return "Parquet (gzip)"
	case FileTypeParquetBZ2:
		return "Parquet (bzip2)"
	case FileTypeParquetXZ:
		return "Parquet (xz)"
	case FileTypeParquetZSTD:
		return "Parquet (zstd)"
	case FileTypeXLSXGZ:
		return "XLSX (gzip)"
	case FileTypeXLSXBZ2:
		return "XLSX (bzip2)"
	case FileTypeXLSXXZ:
		return "XLSX (xz)"
	case FileTypeXLSXZSTD:
		return "XLSX (zstd)"
	case FileTypeCSVZLIB:
		return "CSV (zlib)"
	case FileTypeTSVZLIB:
		return "TSV (zlib)"
	case FileTypeLTSVZLIB:
		return "LTSV (zlib)"
	case FileTypeParquetZLIB:
		return "Parquet (zlib)"
	case FileTypeXLSXZLIB:
		return "XLSX (zlib)"
	case FileTypeCSVSNAPPY:
		return "CSV (snappy)"
	case FileTypeTSVSNAPPY:
		return "TSV (snappy)"
	case FileTypeLTSVSNAPPY:
		return "LTSV (snappy)"
	case FileTypeParquetSNAPPY:
		return "Parquet (snappy)"
	case FileTypeXLSXSNAPPY:
		return "XLSX (snappy)"
	case FileTypeCSVS2:
		return "CSV (s2)"
	case FileTypeTSVS2:
		return "TSV (s2)"
	case FileTypeLTSVS2:
		return "LTSV (s2)"
	case FileTypeParquetS2:
		return "Parquet (s2)"
	case FileTypeXLSXS2:
		return "XLSX (s2)"
	case FileTypeCSVLZ4:
		return "CSV (lz4)"
	case FileTypeTSVLZ4:
		return "TSV (lz4)"
	case FileTypeLTSVLZ4:
		return "LTSV (lz4)"
	case FileTypeParquetLZ4:
		return "Parquet (lz4)"
	case FileTypeXLSXLZ4:
		return "XLSX (lz4)"
	case FileTypeJSON:
		return "JSON"
	case FileTypeJSONL:
		return "JSONL"
	case FileTypeJSONGZ:
		return "JSON (gzip)"
	case FileTypeJSONBZ2:
		return "JSON (bzip2)"
	case FileTypeJSONXZ:
		return "JSON (xz)"
	case FileTypeJSONZSTD:
		return "JSON (zstd)"
	case FileTypeJSONZLIB:
		return "JSON (zlib)"
	case FileTypeJSONSNAPPY:
		return "JSON (snappy)"
	case FileTypeJSONS2:
		return "JSON (s2)"
	case FileTypeJSONLZ4:
		return "JSON (lz4)"
	case FileTypeJSONLGZ:
		return "JSONL (gzip)"
	case FileTypeJSONLBZ2:
		return "JSONL (bzip2)"
	case FileTypeJSONLXZ:
		return "JSONL (xz)"
	case FileTypeJSONLZSTD:
		return "JSONL (zstd)"
	case FileTypeJSONLZLIB:
		return "JSONL (zlib)"
	case FileTypeJSONLSNAPPY:
		return "JSONL (snappy)"
	case FileTypeJSONLS2:
		return "JSONL (s2)"
	case FileTypeJSONLLZ4:
		return "JSONL (lz4)"
	case FileTypeACH:
		return "ACH"
	case FileTypeFedWire:
		return "FedWire"
	default:
		return fileTypeNameUnsupported
	}
}

// FileType human-readable display names returned by String.
const (
	fileTypeNameCSV         = "CSV"
	fileTypeNameTSV         = "TSV"
	fileTypeNameLTSV        = "LTSV"
	fileTypeNameParquet     = "Parquet"
	fileTypeNameXLSX        = "XLSX"
	fileTypeNameUnsupported = "Unsupported"
)

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
	// FileTypeCSVGZ represents gzip-compressed CSV file type
	FileTypeCSVGZ
	// FileTypeTSVGZ represents gzip-compressed TSV file type
	FileTypeTSVGZ
	// FileTypeLTSVGZ represents gzip-compressed LTSV file type
	FileTypeLTSVGZ
	// FileTypeParquetGZ represents gzip-compressed Parquet file type
	FileTypeParquetGZ
	// FileTypeCSVBZ2 represents bzip2-compressed CSV file type
	FileTypeCSVBZ2
	// FileTypeTSVBZ2 represents bzip2-compressed TSV file type
	FileTypeTSVBZ2
	// FileTypeLTSVBZ2 represents bzip2-compressed LTSV file type
	FileTypeLTSVBZ2
	// FileTypeParquetBZ2 represents bzip2-compressed Parquet file type
	FileTypeParquetBZ2
	// FileTypeCSVXZ represents xz-compressed CSV file type
	FileTypeCSVXZ
	// FileTypeTSVXZ represents xz-compressed TSV file type
	FileTypeTSVXZ
	// FileTypeLTSVXZ represents xz-compressed LTSV file type
	FileTypeLTSVXZ
	// FileTypeParquetXZ represents xz-compressed Parquet file type
	FileTypeParquetXZ
	// FileTypeCSVZSTD represents zstd-compressed CSV file type
	FileTypeCSVZSTD
	// FileTypeTSVZSTD represents zstd-compressed TSV file type
	FileTypeTSVZSTD
	// FileTypeLTSVZSTD represents zstd-compressed LTSV file type
	FileTypeLTSVZSTD
	// FileTypeParquetZSTD represents zstd-compressed Parquet file type
	FileTypeParquetZSTD
	// FileTypeXLSXGZ represents gzip-compressed Excel XLSX file type
	FileTypeXLSXGZ
	// FileTypeXLSXBZ2 represents bzip2-compressed Excel XLSX file type
	FileTypeXLSXBZ2
	// FileTypeXLSXXZ represents xz-compressed Excel XLSX file type
	FileTypeXLSXXZ
	// FileTypeXLSXZSTD represents zstd-compressed Excel XLSX file type
	FileTypeXLSXZSTD

	// FileTypeCSVZLIB represents zlib-compressed CSV file type
	FileTypeCSVZLIB
	// FileTypeTSVZLIB represents zlib-compressed TSV file type
	FileTypeTSVZLIB
	// FileTypeLTSVZLIB represents zlib-compressed LTSV file type
	FileTypeLTSVZLIB
	// FileTypeParquetZLIB represents zlib-compressed Parquet file type
	FileTypeParquetZLIB
	// FileTypeXLSXZLIB represents zlib-compressed XLSX file type
	FileTypeXLSXZLIB

	// FileTypeCSVSNAPPY represents snappy-compressed CSV file type
	FileTypeCSVSNAPPY
	// FileTypeTSVSNAPPY represents snappy-compressed TSV file type
	FileTypeTSVSNAPPY
	// FileTypeLTSVSNAPPY represents snappy-compressed LTSV file type
	FileTypeLTSVSNAPPY
	// FileTypeParquetSNAPPY represents snappy-compressed Parquet file type
	FileTypeParquetSNAPPY
	// FileTypeXLSXSNAPPY represents snappy-compressed XLSX file type
	FileTypeXLSXSNAPPY

	// FileTypeCSVS2 represents s2-compressed CSV file type
	FileTypeCSVS2
	// FileTypeTSVS2 represents s2-compressed TSV file type
	FileTypeTSVS2
	// FileTypeLTSVS2 represents s2-compressed LTSV file type
	FileTypeLTSVS2
	// FileTypeParquetS2 represents s2-compressed Parquet file type
	FileTypeParquetS2
	// FileTypeXLSXS2 represents s2-compressed XLSX file type
	FileTypeXLSXS2

	// FileTypeCSVLZ4 represents lz4-compressed CSV file type
	FileTypeCSVLZ4
	// FileTypeTSVLZ4 represents lz4-compressed TSV file type
	FileTypeTSVLZ4
	// FileTypeLTSVLZ4 represents lz4-compressed LTSV file type
	FileTypeLTSVLZ4
	// FileTypeParquetLZ4 represents lz4-compressed Parquet file type
	FileTypeParquetLZ4
	// FileTypeXLSXLZ4 represents lz4-compressed XLSX file type
	FileTypeXLSXLZ4

	// FileTypeJSON represents JSON file type
	FileTypeJSON
	// FileTypeJSONL represents JSON Lines file type
	FileTypeJSONL

	// FileTypeJSONGZ represents gzip-compressed JSON file type
	FileTypeJSONGZ
	// FileTypeJSONBZ2 represents bzip2-compressed JSON file type
	FileTypeJSONBZ2
	// FileTypeJSONXZ represents xz-compressed JSON file type
	FileTypeJSONXZ
	// FileTypeJSONZSTD represents zstd-compressed JSON file type
	FileTypeJSONZSTD
	// FileTypeJSONZLIB represents zlib-compressed JSON file type
	FileTypeJSONZLIB
	// FileTypeJSONSNAPPY represents snappy-compressed JSON file type
	FileTypeJSONSNAPPY
	// FileTypeJSONS2 represents s2-compressed JSON file type
	FileTypeJSONS2
	// FileTypeJSONLZ4 represents lz4-compressed JSON file type
	FileTypeJSONLZ4

	// FileTypeJSONLGZ represents gzip-compressed JSONL file type
	FileTypeJSONLGZ
	// FileTypeJSONLBZ2 represents bzip2-compressed JSONL file type
	FileTypeJSONLBZ2
	// FileTypeJSONLXZ represents xz-compressed JSONL file type
	FileTypeJSONLXZ
	// FileTypeJSONLZSTD represents zstd-compressed JSONL file type
	FileTypeJSONLZSTD
	// FileTypeJSONLZLIB represents zlib-compressed JSONL file type
	FileTypeJSONLZLIB
	// FileTypeJSONLSNAPPY represents snappy-compressed JSONL file type
	FileTypeJSONLSNAPPY
	// FileTypeJSONLS2 represents s2-compressed JSONL file type
	FileTypeJSONLS2
	// FileTypeJSONLLZ4 represents lz4-compressed JSONL file type
	FileTypeJSONLLZ4

	// FileTypeACH represents ACH (NACHA) file type
	FileTypeACH
	// FileTypeFedWire represents Fedwire file type
	FileTypeFedWire

	// FileTypeUnsupported represents unsupported file type
	FileTypeUnsupported
)

// File extension aliases from fileparser package
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
}

// tableChunk represents a chunk of table data for streaming processing
type tableChunk struct {
	tableName  string
	headers    header
	records    []Record
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
func (tc *tableChunk) getRecords() []Record {
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
	fileType    FileType
	tableName   string
	chunkSize   ChunkSize
	memoryPool  *MemoryPool  // Pool for reusable memory allocations
	memoryLimit *MemoryLimit // Configurable memory limits
	// malformedRowPolicy controls how a CSV/TSV record whose field count differs
	// from the header is handled. The zero value is MalformedRowStop.
	malformedRowPolicy MalformedRowPolicy
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
	// Check for ACH files first (case-sensitive)
	if isACHFile(fileName) {
		return true
	}

	// Check for Fedwire files
	if isFedWireFile(fileName) {
		return true
	}

	fileName = strings.ToLower(fileName)

	// Remove compression extensions
	for _, ext := range []string{extGZ, extBZ2, extXZ, extZSTD, extZLIB, extSNAPPY, extS2, extLZ4} {
		if strings.HasSuffix(fileName, ext) {
			fileName = strings.TrimSuffix(fileName, ext)
			break
		}
	}

	// Check for supported file extensions
	return strings.HasSuffix(fileName, extCSV) ||
		strings.HasSuffix(fileName, extTSV) ||
		strings.HasSuffix(fileName, extLTSV) ||
		strings.HasSuffix(fileName, extParquet) ||
		strings.HasSuffix(fileName, extXLSX) ||
		strings.HasSuffix(fileName, extJSON) ||
		strings.HasSuffix(fileName, extJSONL)
}

// isSupportedExtension checks if the given extension is supported
// The extension should start with a dot (e.g., ".csv", ".tsv.gz")
func isSupportedExtension(ext string) bool {
	ext = strings.ToLower(ext)

	// Check if it's a simple extension or has compression
	return isSupportedFile("file" + ext)
}

// extension returns the file extension for the FileType
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
	case FileTypeCSVGZ:
		return extCSV + extGZ
	case FileTypeTSVGZ:
		return extTSV + extGZ
	case FileTypeLTSVGZ:
		return extLTSV + extGZ
	case FileTypeParquetGZ:
		return extParquet + extGZ
	case FileTypeCSVBZ2:
		return extCSV + extBZ2
	case FileTypeTSVBZ2:
		return extTSV + extBZ2
	case FileTypeLTSVBZ2:
		return extLTSV + extBZ2
	case FileTypeParquetBZ2:
		return extParquet + extBZ2
	case FileTypeCSVXZ:
		return extCSV + extXZ
	case FileTypeTSVXZ:
		return extTSV + extXZ
	case FileTypeLTSVXZ:
		return extLTSV + extXZ
	case FileTypeParquetXZ:
		return extParquet + extXZ
	case FileTypeCSVZSTD:
		return extCSV + extZSTD
	case FileTypeTSVZSTD:
		return extTSV + extZSTD
	case FileTypeLTSVZSTD:
		return extLTSV + extZSTD
	case FileTypeParquetZSTD:
		return extParquet + extZSTD
	case FileTypeXLSXGZ:
		return extXLSX + extGZ
	case FileTypeXLSXBZ2:
		return extXLSX + extBZ2
	case FileTypeXLSXXZ:
		return extXLSX + extXZ
	case FileTypeXLSXZSTD:
		return extXLSX + extZSTD
	case FileTypeCSVZLIB:
		return extCSV + extZLIB
	case FileTypeTSVZLIB:
		return extTSV + extZLIB
	case FileTypeLTSVZLIB:
		return extLTSV + extZLIB
	case FileTypeParquetZLIB:
		return extParquet + extZLIB
	case FileTypeXLSXZLIB:
		return extXLSX + extZLIB
	case FileTypeCSVSNAPPY:
		return extCSV + extSNAPPY
	case FileTypeTSVSNAPPY:
		return extTSV + extSNAPPY
	case FileTypeLTSVSNAPPY:
		return extLTSV + extSNAPPY
	case FileTypeParquetSNAPPY:
		return extParquet + extSNAPPY
	case FileTypeXLSXSNAPPY:
		return extXLSX + extSNAPPY
	case FileTypeCSVS2:
		return extCSV + extS2
	case FileTypeTSVS2:
		return extTSV + extS2
	case FileTypeLTSVS2:
		return extLTSV + extS2
	case FileTypeParquetS2:
		return extParquet + extS2
	case FileTypeXLSXS2:
		return extXLSX + extS2
	case FileTypeCSVLZ4:
		return extCSV + extLZ4
	case FileTypeTSVLZ4:
		return extTSV + extLZ4
	case FileTypeLTSVLZ4:
		return extLTSV + extLZ4
	case FileTypeParquetLZ4:
		return extParquet + extLZ4
	case FileTypeXLSXLZ4:
		return extXLSX + extLZ4
	case FileTypeJSON:
		return extJSON
	case FileTypeJSONL:
		return extJSONL
	case FileTypeJSONGZ:
		return extJSON + extGZ
	case FileTypeJSONBZ2:
		return extJSON + extBZ2
	case FileTypeJSONXZ:
		return extJSON + extXZ
	case FileTypeJSONZSTD:
		return extJSON + extZSTD
	case FileTypeJSONZLIB:
		return extJSON + extZLIB
	case FileTypeJSONSNAPPY:
		return extJSON + extSNAPPY
	case FileTypeJSONS2:
		return extJSON + extS2
	case FileTypeJSONLZ4:
		return extJSON + extLZ4
	case FileTypeJSONLGZ:
		return extJSONL + extGZ
	case FileTypeJSONLBZ2:
		return extJSONL + extBZ2
	case FileTypeJSONLXZ:
		return extJSONL + extXZ
	case FileTypeJSONLZSTD:
		return extJSONL + extZSTD
	case FileTypeJSONLZLIB:
		return extJSONL + extZLIB
	case FileTypeJSONLSNAPPY:
		return extJSONL + extSNAPPY
	case FileTypeJSONLS2:
		return extJSONL + extS2
	case FileTypeJSONLLZ4:
		return extJSONL + extLZ4
	case FileTypeACH:
		return extACH
	case FileTypeFedWire:
		return extFED
	default:
		return ""
	}
}

// baseType returns the base file type without compression
func (ft FileType) baseType() FileType {
	switch ft {
	case FileTypeCSV, FileTypeCSVGZ, FileTypeCSVBZ2, FileTypeCSVXZ, FileTypeCSVZSTD,
		FileTypeCSVZLIB, FileTypeCSVSNAPPY, FileTypeCSVS2, FileTypeCSVLZ4:
		return FileTypeCSV
	case FileTypeTSV, FileTypeTSVGZ, FileTypeTSVBZ2, FileTypeTSVXZ, FileTypeTSVZSTD,
		FileTypeTSVZLIB, FileTypeTSVSNAPPY, FileTypeTSVS2, FileTypeTSVLZ4:
		return FileTypeTSV
	case FileTypeLTSV, FileTypeLTSVGZ, FileTypeLTSVBZ2, FileTypeLTSVXZ, FileTypeLTSVZSTD,
		FileTypeLTSVZLIB, FileTypeLTSVSNAPPY, FileTypeLTSVS2, FileTypeLTSVLZ4:
		return FileTypeLTSV
	case FileTypeParquet, FileTypeParquetGZ, FileTypeParquetBZ2, FileTypeParquetXZ, FileTypeParquetZSTD,
		FileTypeParquetZLIB, FileTypeParquetSNAPPY, FileTypeParquetS2, FileTypeParquetLZ4:
		return FileTypeParquet
	case FileTypeXLSX, FileTypeXLSXGZ, FileTypeXLSXBZ2, FileTypeXLSXXZ, FileTypeXLSXZSTD,
		FileTypeXLSXZLIB, FileTypeXLSXSNAPPY, FileTypeXLSXS2, FileTypeXLSXLZ4:
		return FileTypeXLSX
	case FileTypeJSON, FileTypeJSONGZ, FileTypeJSONBZ2, FileTypeJSONXZ, FileTypeJSONZSTD,
		FileTypeJSONZLIB, FileTypeJSONSNAPPY, FileTypeJSONS2, FileTypeJSONLZ4:
		return FileTypeJSON
	case FileTypeJSONL, FileTypeJSONLGZ, FileTypeJSONLBZ2, FileTypeJSONLXZ, FileTypeJSONLZSTD,
		FileTypeJSONLZLIB, FileTypeJSONLSNAPPY, FileTypeJSONLS2, FileTypeJSONLLZ4:
		return FileTypeJSONL
	case FileTypeACH:
		return FileTypeACH
	case FileTypeFedWire:
		return FileTypeFedWire
	default:
		return FileTypeUnsupported
	}
}

// getFileExtension returns the file extension for a given FileType
// Deprecated: Use FileType.extension() method instead
func getFileExtension(fileType FileType) string {
	return fileType.extension()
}

// getBaseFileType returns the base file type without compression
// Deprecated: Use FileType.baseType() method instead
func getBaseFileType(fileType FileType) FileType {
	return fileType.baseType()
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
	return f.getFileType().baseType() == FileTypeCSV
}

// isTSV returns true if the file is TSV format
func (f *file) isTSV() bool {
	return f.getFileType().baseType() == FileTypeTSV
}

// isLTSV returns true if the file is LTSV format
func (f *file) isLTSV() bool {
	return f.getFileType().baseType() == FileTypeLTSV
}

// isXLSX returns true if the file is XLSX format
func (f *file) isXLSX() bool {
	return f.getFileType().baseType() == FileTypeXLSX
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

// toTable converts file to table structure
func (f *file) toTable() (*table, error) {
	// Open file for reading
	file, err := os.Open(f.path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", f.path, err)
	}
	defer file.Close()

	tableName := sanitizeTableName(tableFromFilePath(f.path))
	return parseWithParser(file, f.fileType, tableName)
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

	basePath := path
	var compressionType string

	// Remove compression extensions
	if strings.HasSuffix(path, extGZ) {
		basePath = strings.TrimSuffix(path, extGZ)
		compressionType = compressionGZStr
	} else if strings.HasSuffix(path, extBZ2) {
		basePath = strings.TrimSuffix(path, extBZ2)
		compressionType = compressionBZ2Str
	} else if strings.HasSuffix(path, extXZ) {
		basePath = strings.TrimSuffix(path, extXZ)
		compressionType = compressionXZStr
	} else if strings.HasSuffix(path, extZSTD) {
		basePath = strings.TrimSuffix(path, extZSTD)
		compressionType = compressionZSTDStr
	} else if strings.HasSuffix(path, extZLIB) {
		basePath = strings.TrimSuffix(path, extZLIB)
		compressionType = compressionZLIBStr
	} else if strings.HasSuffix(path, extSNAPPY) {
		basePath = strings.TrimSuffix(path, extSNAPPY)
		compressionType = compressionSNAPPYStr
	} else if strings.HasSuffix(path, extS2) {
		basePath = strings.TrimSuffix(path, extS2)
		compressionType = compressionS2Str
	} else if strings.HasSuffix(path, extLZ4) {
		basePath = strings.TrimSuffix(path, extLZ4)
		compressionType = compressionLZ4Str
	}

	ext := strings.ToLower(filepath.Ext(basePath))
	switch ext {
	case extCSV:
		switch compressionType {
		case compressionGZStr:
			return FileTypeCSVGZ
		case compressionBZ2Str:
			return FileTypeCSVBZ2
		case compressionXZStr:
			return FileTypeCSVXZ
		case compressionZSTDStr:
			return FileTypeCSVZSTD
		case compressionZLIBStr:
			return FileTypeCSVZLIB
		case compressionSNAPPYStr:
			return FileTypeCSVSNAPPY
		case compressionS2Str:
			return FileTypeCSVS2
		case compressionLZ4Str:
			return FileTypeCSVLZ4
		default:
			return FileTypeCSV
		}
	case extTSV:
		switch compressionType {
		case compressionGZStr:
			return FileTypeTSVGZ
		case compressionBZ2Str:
			return FileTypeTSVBZ2
		case compressionXZStr:
			return FileTypeTSVXZ
		case compressionZSTDStr:
			return FileTypeTSVZSTD
		case compressionZLIBStr:
			return FileTypeTSVZLIB
		case compressionSNAPPYStr:
			return FileTypeTSVSNAPPY
		case compressionS2Str:
			return FileTypeTSVS2
		case compressionLZ4Str:
			return FileTypeTSVLZ4
		default:
			return FileTypeTSV
		}
	case extLTSV:
		switch compressionType {
		case compressionGZStr:
			return FileTypeLTSVGZ
		case compressionBZ2Str:
			return FileTypeLTSVBZ2
		case compressionXZStr:
			return FileTypeLTSVXZ
		case compressionZSTDStr:
			return FileTypeLTSVZSTD
		case compressionZLIBStr:
			return FileTypeLTSVZLIB
		case compressionSNAPPYStr:
			return FileTypeLTSVSNAPPY
		case compressionS2Str:
			return FileTypeLTSVS2
		case compressionLZ4Str:
			return FileTypeLTSVLZ4
		default:
			return FileTypeLTSV
		}
	case extParquet:
		switch compressionType {
		case compressionGZStr:
			return FileTypeParquetGZ
		case compressionBZ2Str:
			return FileTypeParquetBZ2
		case compressionXZStr:
			return FileTypeParquetXZ
		case compressionZSTDStr:
			return FileTypeParquetZSTD
		case compressionZLIBStr:
			return FileTypeParquetZLIB
		case compressionSNAPPYStr:
			return FileTypeParquetSNAPPY
		case compressionS2Str:
			return FileTypeParquetS2
		case compressionLZ4Str:
			return FileTypeParquetLZ4
		default:
			return FileTypeParquet
		}
	case extXLSX:
		switch compressionType {
		case compressionGZStr:
			return FileTypeXLSXGZ
		case compressionBZ2Str:
			return FileTypeXLSXBZ2
		case compressionXZStr:
			return FileTypeXLSXXZ
		case compressionZSTDStr:
			return FileTypeXLSXZSTD
		case compressionZLIBStr:
			return FileTypeXLSXZLIB
		case compressionSNAPPYStr:
			return FileTypeXLSXSNAPPY
		case compressionS2Str:
			return FileTypeXLSXS2
		case compressionLZ4Str:
			return FileTypeXLSXLZ4
		default:
			return FileTypeXLSX
		}
	case extJSON:
		switch compressionType {
		case compressionGZStr:
			return FileTypeJSONGZ
		case compressionBZ2Str:
			return FileTypeJSONBZ2
		case compressionXZStr:
			return FileTypeJSONXZ
		case compressionZSTDStr:
			return FileTypeJSONZSTD
		case compressionZLIBStr:
			return FileTypeJSONZLIB
		case compressionSNAPPYStr:
			return FileTypeJSONSNAPPY
		case compressionS2Str:
			return FileTypeJSONS2
		case compressionLZ4Str:
			return FileTypeJSONLZ4
		default:
			return FileTypeJSON
		}
	case extJSONL:
		switch compressionType {
		case compressionGZStr:
			return FileTypeJSONLGZ
		case compressionBZ2Str:
			return FileTypeJSONLBZ2
		case compressionXZStr:
			return FileTypeJSONLXZ
		case compressionZSTDStr:
			return FileTypeJSONLZSTD
		case compressionZLIBStr:
			return FileTypeJSONLZLIB
		case compressionSNAPPYStr:
			return FileTypeJSONLSNAPPY
		case compressionS2Str:
			return FileTypeJSONLS2
		case compressionLZ4Str:
			return FileTypeJSONLLZ4
		default:
			return FileTypeJSONL
		}
	default:
		return FileTypeUnsupported
	}
}

// openReader opens file and returns a reader that handles compression
func (f *file) openReader() (io.Reader, func() error, error) {
	factory := NewCompressionFactory()
	return factory.CreateReaderForFile(f.path)
}

// convertXLSXRowsToTable converts XLSX rows to table headers and records
// First row becomes headers, remaining rows become records with padding
func convertXLSXRowsToTable(rows [][]string) (header, []Record) {
	var headers header
	var records []Record

	// First row as headers
	if len(rows) > 0 {
		headers = make(header, len(rows[0]))
		copy(headers, rows[0])
	}

	// Remaining rows as records
	if len(rows) > 1 {
		records = make([]Record, len(rows)-1)
		for i, row := range rows[1:] {
			record := make(Record, len(headers))
			for j := range headers {
				if j < len(row) {
					record[j] = row[j]
				} else {
					record[j] = "" // Pad with empty string if row is shorter
				}
			}
			records[i] = record
		}
	}

	return headers, records
}
