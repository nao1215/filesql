package frame

import (
	"github.com/nao1215/filesql/parser"
)

// FileType represents supported file types including compression variants.
// This is an alias for parser.FileType.
type FileType = parser.FileType

// Supported file types (re-exported from parser)
const (
	// CSV represents CSV file type
	CSV = parser.CSV
	// TSV represents TSV file type
	TSV = parser.TSV
	// LTSV represents LTSV file type
	LTSV = parser.LTSV
	// Parquet represents Parquet file type
	Parquet = parser.Parquet
	// XLSX represents Excel XLSX file type
	XLSX = parser.XLSX

	// Compressed CSV variants
	CSVGZ   = parser.CSVGZ
	CSVBZ2  = parser.CSVBZ2
	CSVXZ   = parser.CSVXZ
	CSVZSTD = parser.CSVZSTD

	// Compressed TSV variants
	TSVGZ   = parser.TSVGZ
	TSVBZ2  = parser.TSVBZ2
	TSVXZ   = parser.TSVXZ
	TSVZSTD = parser.TSVZSTD

	// Compressed LTSV variants
	LTSVGZ   = parser.LTSVGZ
	LTSVBZ2  = parser.LTSVBZ2
	LTSVXZ   = parser.LTSVXZ
	LTSVZSTD = parser.LTSVZSTD

	// Compressed Parquet variants
	ParquetGZ   = parser.ParquetGZ
	ParquetBZ2  = parser.ParquetBZ2
	ParquetXZ   = parser.ParquetXZ
	ParquetZSTD = parser.ParquetZSTD

	// Compressed XLSX variants
	XLSXGZ   = parser.XLSXGZ
	XLSXBZ2  = parser.XLSXBZ2
	XLSXXZ   = parser.XLSXXZ
	XLSXZSTD = parser.XLSXZSTD

	// ZLIB compressed variants
	CSVZLIB     = parser.CSVZLIB
	TSVZLIB     = parser.TSVZLIB
	LTSVZLIB    = parser.LTSVZLIB
	ParquetZLIB = parser.ParquetZLIB
	XLSXZLIB    = parser.XLSXZLIB

	// Snappy compressed variants
	CSVSNAPPY     = parser.CSVSNAPPY
	TSVSNAPPY     = parser.TSVSNAPPY
	LTSVSNAPPY    = parser.LTSVSNAPPY
	ParquetSNAPPY = parser.ParquetSNAPPY
	XLSXSNAPPY    = parser.XLSXSNAPPY

	// S2 compressed variants
	CSVS2     = parser.CSVS2
	TSVS2     = parser.TSVS2
	LTSVS2    = parser.LTSVS2
	ParquetS2 = parser.ParquetS2
	XLSXS2    = parser.XLSXS2

	// LZ4 compressed variants
	CSVLZ4     = parser.CSVLZ4
	TSVLZ4     = parser.TSVLZ4
	LTSVLZ4    = parser.LTSVLZ4
	ParquetLZ4 = parser.ParquetLZ4
	XLSXLZ4    = parser.XLSXLZ4
)

// baseType returns the base file type without compression.
func baseType(ft FileType) FileType {
	return parser.BaseFileType(ft)
}

// delimiter returns the delimiter character for the file type.
func delimiter(ft FileType) rune {
	switch baseType(ft) {
	case parser.TSV:
		return '\t'
	default:
		return ','
	}
}
