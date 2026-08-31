package filesql

import (
	"github.com/nao1215/filesql/internal/codec"
)

// This file holds the vocabulary a dump is described in: OutputFormat,
// CompressionType and DumpOptions say what a dump writes and in what shape.
// They are values with no behavior beyond reading themselves back, and they
// are what DumpDatabase and the auto-save options take; they lived in save.go
// only because that is where the first of them was written.

// OutputFormat represents the output file format
type OutputFormat int

const (
	// OutputFormatCSV represents CSV output format
	OutputFormatCSV OutputFormat = iota
	// OutputFormatTSV represents TSV output format
	OutputFormatTSV
	// OutputFormatLTSV represents LTSV output format
	OutputFormatLTSV
	// OutputFormatParquet represents Parquet output format
	OutputFormatParquet
	// OutputFormatXLSX represents Excel XLSX output format
	OutputFormatXLSX
	// OutputFormatACH represents ACH (NACHA) output format
	OutputFormatACH
	// OutputFormatFedWire represents Fedwire output format
	OutputFormatFedWire
)

// String returns the string representation of OutputFormat
func (f OutputFormat) String() string {
	switch f {
	case OutputFormatCSV:
		return formatCSVStr
	case OutputFormatTSV:
		return formatTSVStr
	case OutputFormatLTSV:
		return formatLTSVStr
	case OutputFormatParquet:
		return formatParquetStr
	case OutputFormatXLSX:
		return formatXLSXStr
	case OutputFormatACH:
		return formatACHStr
	case OutputFormatFedWire:
		return formatFedWireStr
	default:
		return formatCSVStr
	}
}

// Extension returns the file extension for the format
func (f OutputFormat) Extension() string {
	switch f {
	case OutputFormatCSV:
		return extCSV
	case OutputFormatTSV:
		return extTSV
	case OutputFormatLTSV:
		return extLTSV
	case OutputFormatParquet:
		return extParquet
	case OutputFormatXLSX:
		return extXLSX
	case OutputFormatACH:
		return extACH
	case OutputFormatFedWire:
		return extFED
	default:
		return extCSV
	}
}

// CompressionType represents the compression type
type CompressionType int

const (
	// CompressionNone represents no compression
	CompressionNone CompressionType = iota
	// CompressionGZ represents gzip compression
	CompressionGZ
	// CompressionBZ2 represents bzip2 compression
	CompressionBZ2
	// CompressionXZ represents xz compression
	CompressionXZ
	// CompressionZSTD represents zstd compression
	CompressionZSTD
	// CompressionZLIB represents zlib compression
	CompressionZLIB
	// CompressionSNAPPY represents snappy compression
	CompressionSNAPPY
	// CompressionS2 represents s2 compression
	CompressionS2
	// CompressionLZ4 represents lz4 compression
	CompressionLZ4
)

// string constants for output format names
const (
	formatCSVStr     = "csv"
	formatTSVStr     = "tsv"
	formatLTSVStr    = "ltsv"
	formatParquetStr = "parquet"
	formatXLSXStr    = "xlsx"
	formatACHStr     = "ach"
	formatFedWireStr = "fed"
)

// String returns the string representation of CompressionType
func (c CompressionType) String() string {
	return codec.Codec(c).String()
}

// Extension returns the file extension for the compression type
func (c CompressionType) Extension() string {
	return codec.Codec(c).Extension()
}

// DumpOptions configures how database tables are exported to files.
//
// Example:
//
//	options := NewDumpOptions().
//		WithFormat(OutputFormatTSV).
//		WithCompression(CompressionGZ)
//
//	err := DumpDatabase(db, "./output", options)
type DumpOptions struct {
	// Format specifies the output file format
	Format OutputFormat
	// Compression specifies the compression type
	Compression CompressionType
	// Encoding specifies the text encoding of csv, tsv, and ltsv output. It has
	// no effect on Parquet and XLSX, which carry their own.
	Encoding Encoding
	// LineEnding specifies the line terminator of csv, tsv, and ltsv output. It
	// has no effect on Parquet and XLSX, which are not line-based.
	LineEnding LineEnding
}

// NewDumpOptions creates default export options (CSV, no compression).
//
// Modify with:
//   - WithFormat(): Change file format (CSV, TSV, LTSV)
//   - WithCompression(): Add compression (GZ, BZ2, XZ, ZSTD)
func NewDumpOptions() DumpOptions {
	return DumpOptions{
		Format:      OutputFormatCSV,
		Compression: CompressionNone,
		Encoding:    EncodingUTF8,
		LineEnding:  LineEndingLF,
	}
}

// WithFormat sets the output file format.
//
// Options:
//   - OutputFormatCSV: Comma-separated values
//   - OutputFormatTSV: Tab-separated values
//   - OutputFormatLTSV: Labeled tab-separated values
//   - OutputFormatParquet: Apache Parquet columnar format
func (o DumpOptions) WithFormat(format OutputFormat) DumpOptions {
	o.Format = format
	return o
}

// WithCompression adds compression to output files.
//
// Options:
//   - CompressionNone: No compression (default)
//   - CompressionGZ: Gzip compression (.gz)
//   - CompressionBZ2: Bzip2 compression (.bz2) - read only, writing not supported
//   - CompressionXZ: XZ compression (.xz)
//   - CompressionZSTD: Zstandard compression (.zst)
//   - CompressionZLIB: Zlib compression (.z)
//   - CompressionSNAPPY: Snappy compression (.snappy)
//   - CompressionS2: S2 compression (.s2) - Snappy compatible
//   - CompressionLZ4: LZ4 compression (.lz4)
func (o DumpOptions) WithCompression(compression CompressionType) DumpOptions {
	o.Compression = compression
	return o
}

// WithEncoding sets the text encoding of csv, tsv, and ltsv output.
//
// It exists so a caller that decoded a legacy source before loading can write
// one back: without it every save produced UTF-8, so an in-place save changed
// the file's encoding on disk and the caller's next read of the same file
// returned mojibake.
//
// filesql reads UTF-8 only, so a file written in another encoding is for other
// tools rather than for loading back.
//
// A value the encoding cannot write fails the save with ErrEncoding, naming the
// encoding, rather than being replaced — a substitution is the silent corruption
// the read side already refuses. Parquet and XLSX carry their own encoding and
// are unaffected.
//
// Options:
//   - EncodingUTF8: UTF-8 (default)
//   - EncodingShiftJIS: Shift-JIS (CP932)
//   - EncodingEUCJP: EUC-JP
//   - EncodingISO2022JP: ISO-2022-JP
//   - EncodingUTF16LE: UTF-16 little-endian, with a byte-order mark
//   - EncodingUTF16BE: UTF-16 big-endian, with a byte-order mark
func (o DumpOptions) WithEncoding(enc Encoding) DumpOptions {
	o.Encoding = enc
	return o
}

// WithLineEnding sets the line terminator of csv, tsv, and ltsv output.
//
// It exists for the same reason WithEncoding does: a save wrote "\n" whatever
// the source used, so a CRLF file saved in place came back LF throughout — every
// line of the file changed although the caller had edited one row. Writing back
// in place, which is EnableAutoSave with an empty output directory, detects the
// file's own terminator and keeps it. Every other save is an export and writes
// what it is told, including one aimed at the directory a source came from, so
// this option is how an export is asked for CRLF.
//
// Options:
//   - LineEndingLF: "\n" (default)
//   - LineEndingCRLF: "\r\n"
//
// Parquet and XLSX are not line-based and are unaffected.
func (o DumpOptions) WithLineEnding(lineEnding LineEnding) DumpOptions {
	o.LineEnding = lineEnding
	return o
}

// FileExtension returns the complete file extension including compression
func (o DumpOptions) FileExtension() string {
	baseExt := o.Format.Extension()
	compExt := o.Compression.Extension()
	return baseExt + compExt
}
