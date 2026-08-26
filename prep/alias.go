// Package prep re-exports parser types for backward compatibility.
package prep

import "github.com/nao1215/filesql/parser"

// FileType is an alias for parser.FileType for backward compatibility.
type FileType = parser.FileType

// File type constants re-exported from parser for backward compatibility.
//
// Only formats are re-exported. The parser's own enum keeps a constant for
// every format-and-codec combination — it is the lower level, and that is the
// honest name for what it dispatches on — but mirroring all 56 of them here
// exposed the cross product a second time under a second spelling. A caller
// that needs one names it through the parser package directly, and
// parser.BaseFileType folds it back to a format.
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
