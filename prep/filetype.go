package prep

import "github.com/nao1215/filesql/internal/parser"

// FileType names the format of the input a Processor reads. It says nothing
// about compression: a gzipped CSV and a plain CSV are both FileTypeCSV, and a
// compressed stream is the caller's to unwrap before it reaches Process.
type FileType int

// The formats a Processor reads. These are every file type there is.
const (
	// FileTypeCSV is comma-separated text with a header row.
	FileTypeCSV FileType = iota
	// FileTypeTSV is tab-separated text with a header row.
	FileTypeTSV
	// FileTypeLTSV is labeled tab-separated text, where each field carries its
	// own label and a row needs no header.
	FileTypeLTSV
	// FileTypeParquet is Apache Parquet, which carries its own schema.
	FileTypeParquet
	// FileTypeXLSX is an Excel workbook.
	FileTypeXLSX
	// FileTypeJSON is a JSON array of objects.
	FileTypeJSON
	// FileTypeJSONL is one JSON object per line.
	FileTypeJSONL

	// FileTypeUnsupported is a format this package does not read. A Processor
	// built with it refuses every input with ErrUnsupportedFileType.
	FileTypeUnsupported
)

// String names the file type.
func (f FileType) String() string { return f.internal().String() }

// internal is the reading side's spelling of the same format. The two enums
// mirror each other rather than sharing one type, because the reading side is
// not part of this package's public surface; TestFileTypeMirrorsTheReader holds
// the pairs equal.
func (f FileType) internal() parser.FileType { return parser.FileType(f) }

// fileTypeOf is the reading side's format in this package's own type.
func fileTypeOf(f parser.FileType) FileType { return FileType(f) }
