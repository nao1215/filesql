// Package prep re-exports parser types for backward compatibility.
package prep

import "github.com/nao1215/filesql/parser"

// FileType is an alias for parser.FileType for backward compatibility.
type FileType = parser.FileType

// File type constants re-exported from parser for backward compatibility.
const (
	FileTypeCSV         = parser.CSV
	FileTypeTSV         = parser.TSV
	FileTypeLTSV        = parser.LTSV
	FileTypeParquet     = parser.Parquet
	FileTypeXLSX        = parser.XLSX
	FileTypeCSVGZ       = parser.CSVGZ
	FileTypeCSVBZ2      = parser.CSVBZ2
	FileTypeCSVXZ       = parser.CSVXZ
	FileTypeCSVZSTD     = parser.CSVZSTD
	FileTypeTSVGZ       = parser.TSVGZ
	FileTypeTSVBZ2      = parser.TSVBZ2
	FileTypeTSVXZ       = parser.TSVXZ
	FileTypeTSVZSTD     = parser.TSVZSTD
	FileTypeLTSVGZ      = parser.LTSVGZ
	FileTypeLTSVBZ2     = parser.LTSVBZ2
	FileTypeLTSVXZ      = parser.LTSVXZ
	FileTypeLTSVZSTD    = parser.LTSVZSTD
	FileTypeParquetGZ   = parser.ParquetGZ
	FileTypeParquetBZ2  = parser.ParquetBZ2
	FileTypeParquetXZ   = parser.ParquetXZ
	FileTypeParquetZSTD = parser.ParquetZSTD
	FileTypeXLSXGZ      = parser.XLSXGZ
	FileTypeXLSXBZ2     = parser.XLSXBZ2
	FileTypeXLSXXZ      = parser.XLSXXZ
	FileTypeXLSXZSTD    = parser.XLSXZSTD

	// zlib compression formats (v0.2.0+)
	FileTypeCSVZLIB     = parser.CSVZLIB
	FileTypeTSVZLIB     = parser.TSVZLIB
	FileTypeLTSVZLIB    = parser.LTSVZLIB
	FileTypeParquetZLIB = parser.ParquetZLIB
	FileTypeXLSXZLIB    = parser.XLSXZLIB

	// snappy compression formats (v0.2.0+)
	FileTypeCSVSNAPPY     = parser.CSVSNAPPY
	FileTypeTSVSNAPPY     = parser.TSVSNAPPY
	FileTypeLTSVSNAPPY    = parser.LTSVSNAPPY
	FileTypeParquetSNAPPY = parser.ParquetSNAPPY
	FileTypeXLSXSNAPPY    = parser.XLSXSNAPPY

	// s2 compression formats (v0.2.0+)
	FileTypeCSVS2     = parser.CSVS2
	FileTypeTSVS2     = parser.TSVS2
	FileTypeLTSVS2    = parser.LTSVS2
	FileTypeParquetS2 = parser.ParquetS2
	FileTypeXLSXS2    = parser.XLSXS2

	// lz4 compression formats (v0.2.0+)
	FileTypeCSVLZ4     = parser.CSVLZ4
	FileTypeTSVLZ4     = parser.TSVLZ4
	FileTypeLTSVLZ4    = parser.LTSVLZ4
	FileTypeParquetLZ4 = parser.ParquetLZ4
	FileTypeXLSXLZ4    = parser.XLSXLZ4

	// JSON/JSONL file types (v0.5.0+)
	FileTypeJSON  = parser.JSON
	FileTypeJSONL = parser.JSONL

	// JSON compression formats (v0.5.0+)
	FileTypeJSONGZ     = parser.JSONGZ
	FileTypeJSONBZ2    = parser.JSONBZ2
	FileTypeJSONXZ     = parser.JSONXZ
	FileTypeJSONZSTD   = parser.JSONZSTD
	FileTypeJSONZLIB   = parser.JSONZLIB
	FileTypeJSONSNAPPY = parser.JSONSNAPPY
	FileTypeJSONS2     = parser.JSONS2
	FileTypeJSONLZ4    = parser.JSONLZ4

	// JSONL compression formats (v0.5.0+)
	FileTypeJSONLGZ     = parser.JSONLGZ
	FileTypeJSONLBZ2    = parser.JSONLBZ2
	FileTypeJSONLXZ     = parser.JSONLXZ
	FileTypeJSONLZSTD   = parser.JSONLZSTD
	FileTypeJSONLZLIB   = parser.JSONLZLIB
	FileTypeJSONLSNAPPY = parser.JSONLSNAPPY
	FileTypeJSONLS2     = parser.JSONLS2
	FileTypeJSONLLZ4    = parser.JSONLLZ4

	FileTypeUnsupported = parser.Unsupported
)

// DetectFileType detects file type from extension.
// This is a convenience wrapper around parser.DetectFileType.
func DetectFileType(path string) FileType {
	return parser.DetectFileType(path)
}

// IsCompressed returns true if the file type is compressed.
// This is a convenience wrapper around parser.IsCompressed.
func IsCompressed(ft FileType) bool {
	return parser.IsCompressed(ft)
}
