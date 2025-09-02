package filesql

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
			name:   "Parquet format",
			format: OutputFormatParquet,
			want:   "parquet",
		},
		{
			name:   "XLSX format",
			format: OutputFormatXLSX,
			want:   "xlsx",
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
			got := tt.format.String()
			assert.Equal(t, tt.want, got, "OutputFormat.String() returned unexpected value")
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
			name:   "Parquet extension",
			format: OutputFormatParquet,
			want:   ".parquet",
		},
		{
			name:   "XLSX extension",
			format: OutputFormatXLSX,
			want:   ".xlsx",
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
			got := tt.format.Extension()
			assert.Equal(t, tt.want, got, "OutputFormat.Extension() returned unexpected value")
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
			got := tt.compression.String()
			assert.Equal(t, tt.want, got, "CompressionType.String() returned unexpected value")
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
			got := tt.compression.Extension()
			assert.Equal(t, tt.want, got, "CompressionType.Extension() returned unexpected value")
		})
	}
}

func TestNewDumpOptions(t *testing.T) {
	t.Parallel()

	options := NewDumpOptions()

	assert.Equal(t, OutputFormatCSV, options.Format, "NewDumpOptions().Format should default to CSV")
	assert.Equal(t, CompressionNone, options.Compression, "NewDumpOptions().Compression should default to None")
}

func TestDumpOptions_WithFormat(t *testing.T) {
	t.Parallel()

	options := NewDumpOptions()
	newOptions := options.WithFormat(OutputFormatTSV)

	// Original options should not be modified
	assert.Equal(t, OutputFormatCSV, options.Format, "Original options should not be modified")

	// New options should have the updated format
	assert.Equal(t, OutputFormatTSV, newOptions.Format, "WithFormat() should update format")

	// Other fields should remain unchanged
	assert.Equal(t, CompressionNone, newOptions.Compression, "WithFormat() should not change compression")
}

func TestDumpOptions_WithCompression(t *testing.T) {
	t.Parallel()

	options := NewDumpOptions()
	newOptions := options.WithCompression(CompressionGZ)

	// Original options should not be modified
	assert.Equal(t, CompressionNone, options.Compression, "Original options should not be modified")

	// New options should have the updated compression
	assert.Equal(t, CompressionGZ, newOptions.Compression, "WithCompression() should update compression")

	// Other fields should remain unchanged
	assert.Equal(t, OutputFormatCSV, newOptions.Format, "WithCompression() should not change format")
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
		{
			name:        "Parquet with no compression",
			format:      OutputFormatParquet,
			compression: CompressionNone,
			want:        ".parquet",
		},
		{
			name:        "XLSX with gzip compression",
			format:      OutputFormatXLSX,
			compression: CompressionGZ,
			want:        ".xlsx.gz",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			options := DumpOptions{
				Format:      tt.format,
				Compression: tt.compression,
			}
			got := options.FileExtension()
			assert.Equal(t, tt.want, got, "DumpOptions.FileExtension() returned unexpected value")
		})
	}
}

// TestOutputFormatStringEdgeCases tests edge cases for OutputFormat.String()
func TestOutputFormatStringEdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		format OutputFormat
		want   string
	}{
		{
			name:   "Unknown format should default",
			format: OutputFormat(999), // Invalid format
			want:   "csv",             // Should default to CSV
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.format.String()
			assert.Equal(t, tt.want, got, "OutputFormat.String() returned unexpected value")
		})
	}
}

func TestDumpOptions_ChainedMethods(t *testing.T) {
	t.Parallel()

	options := NewDumpOptions().
		WithFormat(OutputFormatLTSV).
		WithCompression(CompressionZSTD)

	assert.Equal(t, OutputFormatLTSV, options.Format, "Chained WithFormat() should work")
	assert.Equal(t, CompressionZSTD, options.Compression, "Chained WithCompression() should work")

	expectedExt := ".ltsv.zst"
	got := options.FileExtension()
	assert.Equal(t, expectedExt, got, "Chained options FileExtension() should work")
}
