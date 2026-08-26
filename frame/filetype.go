package frame

import (
	"github.com/nao1215/filesql/parser"
)

// FileType represents supported file types including compression variants.
// This is an alias for parser.FileType.
type FileType = parser.FileType

// Supported file types (re-exported from parser)
//
// A compressed input is named by the same constant as its uncompressed form --
// a gzipped CSV is CSV -- and a caller who wants to state the codec passes the
// parser constant for it, such as parser.CSVGZ.
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
