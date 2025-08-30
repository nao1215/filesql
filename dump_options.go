package filesql

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
	}
}

// WithFormat sets the output file format.
//
// Options:
//   - OutputFormatCSV: Comma-separated values
//   - OutputFormatTSV: Tab-separated values
//   - OutputFormatLTSV: Labeled tab-separated values
func (o DumpOptions) WithFormat(format OutputFormat) DumpOptions {
	o.Format = format
	return o
}

// WithCompression adds compression to output files.
//
// Options:
//   - CompressionNone: No compression (default)
//   - CompressionGZ: Gzip compression (.gz)
//   - CompressionBZ2: Bzip2 compression (.bz2)
//   - CompressionXZ: XZ compression (.xz)
//   - CompressionZSTD: Zstandard compression (.zst)
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
