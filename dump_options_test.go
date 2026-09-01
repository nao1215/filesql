package filesql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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

// TestOutputFormatNamesOnlyWhatItHas pins that a value this enumeration has no
// name for says so rather than naming a member.
//
// It named CSV, so a dump given a format from a configuration file that had
// drifted refused with "unsupported output format: csv" -- a format this
// package writes every day -- and computed a ".csv" path for it on the way.
// assertWholeEnum says that a table covers the whole of an iota enumeration:
// its members run from zero without a gap, and the value after the last one has
// no name. A member added the only way these enumerations grow -- appended, so
// iota gives it the next value -- is named by String and fails the second half,
// whatever the table's length happens to be.
func assertWholeEnum[T ~int](t *testing.T, known map[T]bool, name func(T) string) {
	t.Helper()

	highest := T(0)
	for value := range known {
		if value > highest {
			highest = value
		}
	}
	assert.Len(t, known, int(highest)+1, "the table skips a value below %d", int(highest))
	assert.Equal(t, formatUnknownStr, name(highest+1),
		"the enumeration has a member past the table: %v", name(highest+1))
}

func TestOutputFormatNamesOnlyWhatItHas(t *testing.T) {
	t.Parallel()

	known := map[OutputFormat]struct {
		name string
		ext  string
	}{
		OutputFormatCSV:     {name: "csv", ext: ".csv"},
		OutputFormatTSV:     {name: "tsv", ext: ".tsv"},
		OutputFormatLTSV:    {name: "ltsv", ext: ".ltsv"},
		OutputFormatParquet: {name: "parquet", ext: ".parquet"},
		OutputFormatXLSX:    {name: "xlsx", ext: ".xlsx"},
		OutputFormatACH:     {name: "ach", ext: ".ach"},
		OutputFormatFedWire: {name: "fed", ext: ".fed"},
	}
	for format, want := range known {
		assert.Equal(t, want.name, format.String())
		assert.Equal(t, want.ext, format.Extension())
	}
	present := make(map[OutputFormat]bool, len(known))
	for format := range known {
		present[format] = true
	}
	assertWholeEnum(t, present, OutputFormat.String)

	for _, unknown := range []OutputFormat{OutputFormat(99), OutputFormat(-1)} {
		assert.Equal(t, "unknown", unknown.String())
		assert.Empty(t, unknown.Extension(), "a format with no writer has no extension of its own")
		for format, want := range known {
			assert.NotEqual(t, want.name, unknown.String(), "%v must not answer as %v", unknown, format)
		}
	}
}

// TestCompressionTypeNamesOnlyWhatItHas is the same rule for the codec, which
// answered "none" -- the codec a dump uses when nothing else is asked for.
func TestCompressionTypeNamesOnlyWhatItHas(t *testing.T) {
	t.Parallel()

	known := map[CompressionType]struct {
		name string
		ext  string
	}{
		CompressionNone:   {name: "none", ext: ""},
		CompressionGZ:     {name: "gz", ext: ".gz"},
		CompressionBZ2:    {name: "bz2", ext: ".bz2"},
		CompressionXZ:     {name: "xz", ext: ".xz"},
		CompressionZSTD:   {name: "zstd", ext: ".zst"},
		CompressionZLIB:   {name: "zlib", ext: ".z"},
		CompressionSNAPPY: {name: "snappy", ext: ".snappy"},
		CompressionS2:     {name: "s2", ext: ".s2"},
		CompressionLZ4:    {name: "lz4", ext: ".lz4"},
	}
	for codec, want := range known {
		assert.Equal(t, want.name, codec.String())
		assert.Equal(t, want.ext, codec.Extension())
	}
	present := make(map[CompressionType]bool, len(known))
	for codec := range known {
		present[codec] = true
	}
	assertWholeEnum(t, present, CompressionType.String)

	for _, unknown := range []CompressionType{CompressionType(99), CompressionType(-1)} {
		assert.Equal(t, "unknown", unknown.String())
		assert.Empty(t, unknown.Extension(), "a codec this package has no name for names no extension")
	}
}
