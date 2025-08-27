package filesql

import (
	"testing"
)

func TestOutputFormat_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		format OutputFormat
		want   string
	}{
		{
			name:   "CSV format",
			format: OutputFormatCSV,
			want:   "csv",
		},
		{
			name:   "TSV format",
			format: OutputFormatTSV,
			want:   "tsv",
		},
		{
			name:   "LTSV format",
			format: OutputFormatLTSV,
			want:   "ltsv",
		},
		{
			name:   "Unknown format defaults to csv",
			format: OutputFormat(999),
			want:   "csv",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.format.String(); got != tt.want {
				t.Errorf("OutputFormat.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOutputFormat_Extension(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		format OutputFormat
		want   string
	}{
		{
			name:   "CSV extension",
			format: OutputFormatCSV,
			want:   ".csv",
		},
		{
			name:   "TSV extension",
			format: OutputFormatTSV,
			want:   ".tsv",
		},
		{
			name:   "LTSV extension",
			format: OutputFormatLTSV,
			want:   ".ltsv",
		},
		{
			name:   "Unknown format defaults to csv",
			format: OutputFormat(999),
			want:   ".csv",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.format.Extension(); got != tt.want {
				t.Errorf("OutputFormat.Extension() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCompressionType_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		compression CompressionType
		want        string
	}{
		{
			name:        "No compression",
			compression: CompressionNone,
			want:        "none",
		},
		{
			name:        "GZ compression",
			compression: CompressionGZ,
			want:        "gz",
		},
		{
			name:        "BZ2 compression",
			compression: CompressionBZ2,
			want:        "bz2",
		},
		{
			name:        "XZ compression",
			compression: CompressionXZ,
			want:        "xz",
		},
		{
			name:        "ZSTD compression",
			compression: CompressionZSTD,
			want:        "zstd",
		},
		{
			name:        "Unknown compression defaults to none",
			compression: CompressionType(999),
			want:        "none",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.compression.String(); got != tt.want {
				t.Errorf("CompressionType.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCompressionType_Extension(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		compression CompressionType
		want        string
	}{
		{
			name:        "No compression",
			compression: CompressionNone,
			want:        "",
		},
		{
			name:        "GZ compression",
			compression: CompressionGZ,
			want:        ".gz",
		},
		{
			name:        "BZ2 compression",
			compression: CompressionBZ2,
			want:        ".bz2",
		},
		{
			name:        "XZ compression",
			compression: CompressionXZ,
			want:        ".xz",
		},
		{
			name:        "ZSTD compression",
			compression: CompressionZSTD,
			want:        ".zst",
		},
		{
			name:        "Unknown compression defaults to empty",
			compression: CompressionType(999),
			want:        "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.compression.Extension(); got != tt.want {
				t.Errorf("CompressionType.Extension() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewDumpOptions(t *testing.T) {
	t.Parallel()

	options := NewDumpOptions()

	if options.Format != OutputFormatCSV {
		t.Errorf("NewDumpOptions().Format = %v, want %v", options.Format, OutputFormatCSV)
	}

	if options.Compression != CompressionNone {
		t.Errorf("NewDumpOptions().Compression = %v, want %v", options.Compression, CompressionNone)
	}
}

func TestDumpOptions_WithFormat(t *testing.T) {
	t.Parallel()

	options := NewDumpOptions()
	newOptions := options.WithFormat(OutputFormatTSV)

	// Original options should not be modified
	if options.Format != OutputFormatCSV {
		t.Errorf("Original options modified: Format = %v, want %v", options.Format, OutputFormatCSV)
	}

	// New options should have the updated format
	if newOptions.Format != OutputFormatTSV {
		t.Errorf("WithFormat().Format = %v, want %v", newOptions.Format, OutputFormatTSV)
	}

	// Other fields should remain unchanged
	if newOptions.Compression != CompressionNone {
		t.Errorf("WithFormat().Compression = %v, want %v", newOptions.Compression, CompressionNone)
	}
}

func TestDumpOptions_WithCompression(t *testing.T) {
	t.Parallel()

	options := NewDumpOptions()
	newOptions := options.WithCompression(CompressionGZ)

	// Original options should not be modified
	if options.Compression != CompressionNone {
		t.Errorf("Original options modified: Compression = %v, want %v", options.Compression, CompressionNone)
	}

	// New options should have the updated compression
	if newOptions.Compression != CompressionGZ {
		t.Errorf("WithCompression().Compression = %v, want %v", newOptions.Compression, CompressionGZ)
	}

	// Other fields should remain unchanged
	if newOptions.Format != OutputFormatCSV {
		t.Errorf("WithCompression().Format = %v, want %v", newOptions.Format, OutputFormatCSV)
	}
}

func TestDumpOptions_FileExtension(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		format      OutputFormat
		compression CompressionType
		want        string
	}{
		{
			name:        "CSV with no compression",
			format:      OutputFormatCSV,
			compression: CompressionNone,
			want:        ".csv",
		},
		{
			name:        "CSV with gzip compression",
			format:      OutputFormatCSV,
			compression: CompressionGZ,
			want:        ".csv.gz",
		},
		{
			name:        "TSV with bzip2 compression",
			format:      OutputFormatTSV,
			compression: CompressionBZ2,
			want:        ".tsv.bz2",
		},
		{
			name:        "LTSV with xz compression",
			format:      OutputFormatLTSV,
			compression: CompressionXZ,
			want:        ".ltsv.xz",
		},
		{
			name:        "TSV with zstd compression",
			format:      OutputFormatTSV,
			compression: CompressionZSTD,
			want:        ".tsv.zst",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			options := DumpOptions{
				Format:      tt.format,
				Compression: tt.compression,
			}
			if got := options.FileExtension(); got != tt.want {
				t.Errorf("DumpOptions.FileExtension() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDumpOptions_ChainedMethods(t *testing.T) {
	t.Parallel()

	options := NewDumpOptions().
		WithFormat(OutputFormatLTSV).
		WithCompression(CompressionZSTD)

	if options.Format != OutputFormatLTSV {
		t.Errorf("Chained WithFormat().Format = %v, want %v", options.Format, OutputFormatLTSV)
	}

	if options.Compression != CompressionZSTD {
		t.Errorf("Chained WithCompression().Compression = %v, want %v", options.Compression, CompressionZSTD)
	}

	expectedExt := ".ltsv.zst"
	if got := options.FileExtension(); got != expectedExt {
		t.Errorf("Chained options FileExtension() = %v, want %v", got, expectedExt)
	}
}
