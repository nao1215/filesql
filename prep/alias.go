// Package prep re-exports parser types for backward compatibility.
package prep

import "github.com/nao1215/filesql/parser"

// FileType is an alias for parser.FileType for backward compatibility.
type FileType = parser.FileType

// File type constants re-exported from parser for backward compatibility.
//
// These are every file type there is. The parser's enum used to keep a constant
// for each format-and-codec combination as well, which this package
// deliberately did not mirror; it names formats only now, so there is nothing
// left to leave out.
const (
	FileTypeCSV     = parser.CSV
	FileTypeTSV     = parser.TSV
	FileTypeLTSV    = parser.LTSV
	FileTypeParquet = parser.Parquet
	FileTypeXLSX    = parser.XLSX

	// JSON/JSONL file types (v0.5.0+)
	FileTypeJSON  = parser.JSON
	FileTypeJSONL = parser.JSONL

	FileTypeUnsupported = parser.Unsupported
)
