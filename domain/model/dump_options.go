// Package model provides domain model for filesql
package model

// OutputFormat represents the output file format
type OutputFormat int

const (
	// OutputFormatCSV represents CSV output format
	OutputFormatCSV OutputFormat = iota
	// OutputFormatTSV represents TSV output format
	OutputFormatTSV
	// OutputFormatLTSV represents LTSV output format
	OutputFormatLTSV
)

// String returns the string representation of OutputFormat
func (f OutputFormat) String() string {
	switch f {
	case OutputFormatCSV:
		return "csv"
	case OutputFormatTSV:
		return "tsv"
	case OutputFormatLTSV:
		return "ltsv"
	default:
		return "csv"
	}
}

// Extension returns the file extension for the format
func (f OutputFormat) Extension() string {
	switch f {
	case OutputFormatCSV:
		return ".csv"
	case OutputFormatTSV:
		return ".tsv"
	case OutputFormatLTSV:
		return ".ltsv"
	default:
		return ".csv"
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
)

// String returns the string representation of CompressionType
func (c CompressionType) String() string {
	switch c {
	case CompressionNone:
		return "none"
	case CompressionGZ:
		return "gz"
	case CompressionBZ2:
		return "bz2"
	case CompressionXZ:
		return "xz"
	case CompressionZSTD:
		return "zstd"
	default:
		return "none"
	}
}

// Extension returns the file extension for the compression type
func (c CompressionType) Extension() string {
	switch c {
	case CompressionNone:
		return ""
	case CompressionGZ:
		return ".gz"
	case CompressionBZ2:
		return ".bz2"
	case CompressionXZ:
		return ".xz"
	case CompressionZSTD:
		return ".zst"
	default:
		return ""
	}
}

// DumpOptions represents options for dumping database
type DumpOptions struct {
	// Format specifies the output file format
	Format OutputFormat
	// Compression specifies the compression type
	Compression CompressionType
}

// NewDumpOptions creates new DumpOptions with default values (CSV format, no compression)
func NewDumpOptions() DumpOptions {
	return DumpOptions{
		Format:      OutputFormatCSV,
		Compression: CompressionNone,
	}
}

// WithFormat sets the output format
func (o DumpOptions) WithFormat(format OutputFormat) DumpOptions {
	o.Format = format
	return o
}

// WithCompression sets the compression type
func (o DumpOptions) WithCompression(compression CompressionType) DumpOptions {
	o.Compression = compression
	return o
}

// FileExtension returns the complete file extension including compression
func (o DumpOptions) FileExtension() string {
	baseExt := o.Format.Extension()
	compExt := o.Compression.Extension()
	return baseExt + compExt
}
