package reader

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/nao1215/filesql/internal/infer"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeParquet renders rows of T as an in-memory Parquet file.
func writeParquet[T any](t *testing.T, rows []T) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[T](&buf)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// collectParquet reads a Parquet file whole, gathering every chunk. It reads
// from src when one is given and from data otherwise, so a case can compare the
// route a file takes with the route a stream takes.
func collectParquet(t *testing.T, data []byte, opts Options, src ...io.Reader) (Result, []*Chunk) {
	t.Helper()
	var from io.Reader = bytes.NewReader(data)
	if len(src) == 1 && src[0] != nil {
		from = src[0]
	}
	var chunks []*Chunk
	result, err := readParquet(from, opts, func(c *Chunk) error {
		chunks = append(chunks, c)
		return nil
	})
	require.NoError(t, err)
	return result, chunks
}

func TestReadParquetFlatFile(t *testing.T) {
	t.Parallel()

	type row struct {
		ID    int64    `parquet:"id"`
		Name  string   `parquet:"name"`
		Price float64  `parquet:"price"`
		Fresh bool     `parquet:"fresh"`
		Note  *string  `parquet:"note,optional"`
		Score *float64 `parquet:"score,optional"`
	}
	note := "on sale"
	nan := math.NaN()
	data := writeParquet(t, []row{
		{ID: 1, Name: "Laptop", Price: 999.99, Fresh: true, Note: &note},
		{ID: 2, Name: "Mouse", Price: 30, Fresh: false, Score: &nan},
	})

	result, chunks := collectParquet(t, data, Options{Rendering: RenderSQLite})
	assert.Equal(t, []string{"id", "name", "price", "fresh", "note", "score"}, result.Header)
	assert.Equal(t, []infer.Type{infer.Integer, infer.Text, infer.Real, infer.Integer, infer.Text, infer.Real}, result.Types)
	assert.Equal(t, 2, result.Rows)
	require.Len(t, chunks, 1)
	assert.Equal(t, [][]string{
		{"1", "Laptop", "999.99", "1", "on sale", ""},
		{"2", "Mouse", "30.0", "0", "", ""},
	}, chunks[0].Records)
	// The missing note is a null, and so is the NaN: SQLite has no NaN, so a
	// null is what the value already means there.
	assert.Equal(t, [][]bool{
		{false, false, false, false, false, true},
		{false, false, false, false, true, true},
	}, chunks[0].Nulls)

	// A plain read spells the values as the format reads them.
	plain, plainChunks := collectParquet(t, data, Options{Rendering: RenderPlain})
	assert.Equal(t, []infer.Type{infer.Integer, infer.Text, infer.Real, infer.Text, infer.Text, infer.Real}, plain.Types)
	require.Len(t, plainChunks, 1)
	// A plain NaN is not a null, but it has no decimal spelling either, so the
	// cell is empty text, which is what the previous reader rendered.
	assert.Equal(t, []string{"2", "Mouse", "30", "false", "", ""}, plainChunks[0].Records[1])
	assert.False(t, plainChunks[0].Nulls[1][5], "only a load turns a NaN into a null")
}

func TestReadParquetHonorsChunkSize(t *testing.T) {
	t.Parallel()

	type row struct {
		N int64 `parquet:"n"`
	}
	rows := make([]row, 5)
	for i := range rows {
		rows[i].N = int64(i)
	}
	data := writeParquet(t, rows)

	result, chunks := collectParquet(t, data, Options{ChunkSize: 2, Rendering: RenderSQLite})
	assert.Equal(t, 5, result.Rows)
	require.Len(t, chunks, 3)
	assert.Len(t, chunks[0].Records, 2)
	assert.Len(t, chunks[1].Records, 2)
	assert.Len(t, chunks[2].Records, 1)
	assert.Equal(t, "4", chunks[2].Records[0][0])
}

func TestReadParquetEmptyTableStillNamesItsColumns(t *testing.T) {
	t.Parallel()

	type row struct {
		A int64  `parquet:"a"`
		B string `parquet:"b"`
	}
	data := writeParquet(t, []row{})

	result, chunks := collectParquet(t, data, Options{Rendering: RenderSQLite})
	assert.Equal(t, []string{"a", "b"}, result.Header)
	assert.Equal(t, []infer.Type{infer.Integer, infer.Text}, result.Types)
	assert.Equal(t, 0, result.Rows)
	require.Len(t, chunks, 1)
	assert.Empty(t, chunks[0].Records)
}

func TestReadParquetUnsignedColumns(t *testing.T) {
	t.Parallel()

	type row struct {
		U8  uint8  `parquet:"u8"`
		U32 uint32 `parquet:"u32"`
		U64 uint64 `parquet:"u64"`
	}
	data := writeParquet(t, []row{
		{U8: 255, U32: math.MaxUint32, U64: math.MaxUint64},
		{U8: 0, U32: 7, U64: 42},
	})

	result, chunks := collectParquet(t, data, Options{Rendering: RenderSQLite})
	require.Len(t, chunks, 1)
	assert.Equal(t, []string{"255", "4294967295", "18446744073709551615"}, chunks[0].Records[0])
	assert.Equal(t, []string{"0", "7", "42"}, chunks[0].Records[1])
	// The narrower unsigned types fit in int64, and UINT64 does not: declared
	// INTEGER, its upper half is converted to REAL by SQLite's affinity and the
	// exact value is gone, so the column is TEXT the way DECIMAL and UUID are.
	assert.Equal(t, []infer.Type{infer.Integer, infer.Integer, infer.Text}, result.Types)
}

func TestReadParquetDecimalColumn(t *testing.T) {
	t.Parallel()

	type row struct {
		Cost int64 `parquet:"cost,decimal(2:12)"`
	}
	data := writeParquet(t, []row{{Cost: 12345}, {Cost: -5}})

	result, chunks := collectParquet(t, data, Options{Rendering: RenderSQLite})
	assert.Equal(t, []infer.Type{infer.Text}, result.Types, "a decimal is text: neither INTEGER nor REAL holds its digits")
	require.Len(t, chunks, 1)
	assert.Equal(t, "123.45", chunks[0].Records[0][0])
	assert.Equal(t, "-0.05", chunks[0].Records[1][0])
}

func TestReadParquetNestedFieldRendersAsText(t *testing.T) {
	t.Parallel()

	type row struct {
		ID   int64   `parquet:"id"`
		Tags []int32 `parquet:"tags,list"`
	}
	data := writeParquet(t, []row{
		{ID: 1, Tags: []int32{1, 2, 3}},
		{ID: 2, Tags: nil},
	})

	result, chunks := collectParquet(t, data, Options{Rendering: RenderSQLite})
	assert.Equal(t, []infer.Type{infer.Integer, infer.Text}, result.Types)
	require.Len(t, chunks, 1)
	assert.Equal(t, "[1 2 3]", chunks[0].Records[0][1])
	assert.True(t, chunks[0].Nulls[1][1], "an absent list is a null cell")
	assert.Equal(t, "1", chunks[0].Records[0][0], "the flat column beside it is unaffected")
}

// TestReadParquetRefusesDuplicateColumnNames pins that the Parquet reader
// validates its header the way every other format does: two columns that are
// one column to SQLite -- the same after case folding, or after trimming
// surrounding whitespace -- are refused with the classified duplicate-column
// error rather than left to fail later as a raw CREATE TABLE error.
func TestReadParquetRefusesDuplicateColumnNames(t *testing.T) {
	t.Parallel()

	discard := func(*Chunk) error { return nil }

	t.Run("names differing only in case", func(t *testing.T) {
		t.Parallel()

		type row struct {
			Lower int64 `parquet:"id"`
			Upper int64 `parquet:"ID"`
		}
		data := writeParquet(t, []row{{Lower: 1, Upper: 2}})

		_, err := readParquet(bytes.NewReader(data), Options{Rendering: RenderSQLite}, discard)
		require.Error(t, err)
		var readErr *Error
		require.ErrorAs(t, err, &readErr)
		assert.Equal(t, KindDuplicateColumn, readErr.Kind)
	})

	t.Run("names differing only in surrounding whitespace", func(t *testing.T) {
		t.Parallel()

		schema := parquet.NewSchema("t", parquet.Group{
			"x":  parquet.Optional(parquet.String()),
			" x": parquet.Optional(parquet.String()),
		})
		var buf bytes.Buffer
		w := parquet.NewGenericWriter[map[string]any](&buf, schema)
		_, err := w.Write([]map[string]any{{"x": "a", " x": "b"}})
		require.NoError(t, err)
		require.NoError(t, w.Close())

		_, err = readParquet(bytes.NewReader(buf.Bytes()), Options{Rendering: RenderSQLite}, discard)
		require.Error(t, err)
		var readErr *Error
		require.ErrorAs(t, err, &readErr)
		assert.Equal(t, KindDuplicateColumn, readErr.Kind)
	})
}

// TestReadParquetKeepsColumnsSQLiteKeepsApart pins the boundary of the
// validation above: names that differ by both whitespace and case -- " x" and
// "X" -- are merged by neither the trim rule nor the case fold on its own, and
// SQLite keeps them as two columns, so the header is accepted rather than
// folded into a false duplicate that refuses a file SQLite would load.
func TestReadParquetKeepsColumnsSQLiteKeepsApart(t *testing.T) {
	t.Parallel()

	schema := parquet.NewSchema("t", parquet.Group{
		"X":  parquet.Optional(parquet.String()),
		" x": parquet.Optional(parquet.String()),
	})
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[map[string]any](&buf, schema)
	_, err := w.Write([]map[string]any{{"X": "a", " x": "b"}})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	result, _ := collectParquet(t, buf.Bytes(), Options{Rendering: RenderSQLite})
	assert.ElementsMatch(t, []string{"X", " x"}, result.Header)
}

func TestReadParquetRefusesWhatIsNotParquet(t *testing.T) {
	t.Parallel()

	discard := func(*Chunk) error { return nil }

	_, err := readParquet(bytes.NewReader(nil), Options{}, discard)
	assert.Error(t, err)

	_, err = readParquet(bytes.NewReader([]byte("not a parquet file")), Options{}, discard)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a parquet file")

	// The right magic with nothing behind it is damage, not a table.
	_, err = readParquet(bytes.NewReader([]byte("PAR1PAR1")), Options{}, discard)
	assert.Error(t, err)
}

// TestReadParquetCostsNoMoreThanItsOwnSize holds the rule this reader was
// chosen for, on the number a file states about itself. The last eight bytes of
// a Parquet file are the footer's length and the magic, and the library
// allocates that length before checking the file is that big, so eight bytes
// reading "PAR1PAR1" -- whose second magic reads as a footer of 826364240 bytes
// -- allocated 789 MiB before failing with "negative offset". The error was
// always right; what it cost was not, which is why this asserts on allocation
// rather than on the error alone.
func TestReadParquetCostsNoMoreThanItsOwnSize(t *testing.T) {
	// Not parallel, and neither are its cases: the measurement is this
	// process's total allocation, so anything running beside it is counted in.

	// Wide next to the tens of bytes each input is, and far below the 789 MiB
	// the first of them used to cost.
	const ceiling = 8 << 20

	for _, tc := range []struct {
		name string
		data []byte
	}{
		{name: "the trailing magic read as a footer length", data: []byte("PAR1PAR1")},
		{name: "a footer of the largest length there is", data: append([]byte("PAR1"), 0xFF, 0xFF, 0xFF, 0xFF, 'P', 'A', 'R', '1')},
		{name: "a footer one byte past the end", data: append([]byte("PAR1"), 0x01, 0x00, 0x00, 0x00, 'P', 'A', 'R', '1')},
		{name: "too short to hold a footer at all", data: []byte("PAR1")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)
			_, err := readParquet(bytes.NewReader(tc.data), Options{}, func(*Chunk) error { return nil })
			runtime.ReadMemStats(&after)

			require.Error(t, err)
			if allocated := after.TotalAlloc - before.TotalAlloc; allocated > ceiling {
				t.Errorf("refusing %d bytes allocated %d MiB", len(tc.data), allocated>>20)
			}
		})
	}

	t.Run("a real file whose footer fits still loads", func(t *testing.T) {
		type row struct {
			ID int64 `parquet:"id"`
		}
		data := writeParquet(t, []row{{ID: 1}})

		result, err := readParquet(bytes.NewReader(data), Options{}, func(*Chunk) error { return nil })

		require.NoError(t, err)
		assert.Equal(t, []string{"id"}, result.Header)
	})
}

// slowParquet is a 433-byte file whose metadata declares column chunks outside
// the file. The Arrow page decoder allocated hundreds of megabytes a second
// trying to read them, until a metadata bounds check began refusing the file
// before any decoding; that check left with the decoder it defended, so the
// reader itself must refuse the file, quickly.
const slowParquet = "MDAwMBUYFQAVCBUILBUGFRAVMBUwHBhvFTAZQTkEGAYwMDAwMDAVBjAVBCMwGAIwMDcwMDAw" +
	"MDAwMDAVDCMwGAQwMDAwIzBMHDAwMBUKJTAYBTAwMDAwMBYwGRwZMCbaMBwVMBkwMDAwGRgC" +
	"MDAVABYwFtIwFtIBJjAmCBwYIDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwWAgw" +
	"MDAwMDAwMDAZIBUwFTAVMDAVMBUwFTAwMDAmjDAcFTAZMDAwMBkYBDAwMDAVABYwFrIwFrIB" +
	"JrQwJtoBHDYwFjAYBTAwMDAwGAgwMDAwMDAwMDAZIBUwFTAVMDAVMBUwFTAwMDAm3jAcFTAZ" +
	"MDAwMBkYBTAwMDAwFQAWMBbSMBbSASbYMCaMAxwYCDAwMDAwMDAwGAgwMDAwMDAwMBYwFjAY" +
	"CDAwMDAwMDAwGAgwMDAwMDAwMDAZIBUwFTAVMDAVMBUwFTAwMDAW1jAWBiYwFtYwFDAwGQwY" +
	"IjAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAZMBwwMBwwMDAwMDCRAQAAUEFS" +
	"MQ=="

func TestReadParquetRefusesTheFileThatHungTheArrowDecoder(t *testing.T) {
	t.Parallel()

	data, err := base64.StdEncoding.DecodeString(slowParquet)
	require.NoError(t, err)

	start := time.Now()
	_, err = readParquet(bytes.NewReader(data), Options{Rendering: RenderSQLite}, func(*Chunk) error { return nil })
	elapsed := time.Since(start)

	assert.Error(t, err)
	assert.Less(t, elapsed, 5*time.Second, "a 433-byte file must fail in time bounded by its size")
}

// mapLeafParquet is a 458-byte file found by fuzzing whose schema annotates a
// node that has a physical type -- a leaf -- as a MAP, which is inconsistent
// metadata: asking that node's type its kind panics inside the library. The
// read must answer with an error instead.
const mapLeafParquet = "UEFSMRUEFQAAUrgehes/j0A9CtejcP09QI/C9ShcejJAFQAVCBUILBUGFRAVBhUkABUEGUw1" +
	"BBgGc2NoZW1hFQYAFQQlABgCJDA3JEysE0ARAAAAFQwlABgEbmFtZSUATCwwMDAVMDgAOAUw" +
	"MDAwMDAWMBkcGTwm2jAsOTcwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAsODIwMDAwMDAwMDAw" +
	"MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDgAOABDMDA4ADAwJoww" +
	"LDgwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMTgA" +
	"OSwxMTgAMDgAOABDMDA4ADAwJt4wLDExNzAwMDAwMDAwMTE3MDAwMDAwMDAxMTExODEwMDAw" +
	"MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwOSwxMTgAMDgA" +
	"OABDMDA4ADAwFtYwFjA5MTAwMDgxMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAw" +
	"MDAwMDAwMDAwMDAwMDAwMDAwkQEAAFBBUjE="

func TestReadParquetRefusesAMapAnnotatedLeaf(t *testing.T) {
	t.Parallel()

	data, err := base64.StdEncoding.DecodeString(mapLeafParquet)
	require.NoError(t, err)

	_, err = readParquet(bytes.NewReader(data), Options{Rendering: RenderSQLite}, func(*Chunk) error { return nil })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parquet data is damaged")
}

func TestFloat16Leaves(t *testing.T) {
	t.Parallel()

	valid := func(t format.Type) thrift.Null[format.Type] { return thrift.Null[format.Type]{V: t, Valid: true} }
	length := func(n int32) thrift.Null[int32] { return thrift.Null[int32]{V: n, Valid: true} }
	half := format.LogicalType{Value: &format.Float16Type{}}
	elements := []format.SchemaElement{
		{}, // the root group has no physical type
		{Type: valid(format.FixedLenByteArray), TypeLength: length(2), LogicalType: half},
		{Type: valid(format.Int64)},
		{}, // a nested group
		{Type: valid(format.FixedLenByteArray), TypeLength: length(2), LogicalType: half},
		// The annotation on any shape but two fixed bytes is inconsistent
		// metadata and is not a half float: another width, a missing width,
		// or a variable-length BYTE_ARRAY whose TypeLength means a bit cap.
		{Type: valid(format.FixedLenByteArray), TypeLength: length(4), LogicalType: half},
		{Type: valid(format.FixedLenByteArray), LogicalType: half},
		{Type: valid(format.ByteArray), TypeLength: length(2), LogicalType: half},
	}
	assert.Equal(t, map[int]bool{0: true, 2: true}, float16Leaves(elements))
	assert.Empty(t, float16Leaves(nil))
}

func TestInt96EpochNanos(t *testing.T) {
	t.Parallel()

	const julianUnixEpoch = 2440588
	assert.Equal(t, int64(0), int96EpochNanos([3]uint32{0, 0, julianUnixEpoch}))
	assert.Equal(t, int64(1), int96EpochNanos([3]uint32{1, 0, julianUnixEpoch}))
	assert.Equal(t, int64(86400_000_000_000), int96EpochNanos([3]uint32{0, 0, julianUnixEpoch + 1}))
	assert.Equal(t, int64(-86400_000_000_000), int96EpochNanos([3]uint32{0, 0, julianUnixEpoch - 1}))
	// The nanoseconds of the day span both low words.
	assert.Equal(t, int64(1)<<32, int96EpochNanos([3]uint32{0, 1, julianUnixEpoch}))
}

func TestDecimalText(t *testing.T) {
	t.Parallel()

	tests := []struct {
		unscaled int64
		scale    int32
		want     string
	}{
		{12345, 2, "123.45"},
		{-12345, 2, "-123.45"},
		{5, 2, "0.05"},
		{-5, 2, "-0.05"},
		{0, 2, "0.00"},
		{7, 0, "7"},
		// A negative scale multiplies: 12 at scale -2 is 1200.
		{12, -2, "1200"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, renderParquetValue(parquet.Int64Value(tt.unscaled),
			parquetColumn{leaf: true, decimal: true, scale: tt.scale}, RenderSQLite),
			"unscaled %d at scale %d", tt.unscaled, tt.scale)
	}
}

func TestFloat16To32AgreesWithTheStandard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		bits uint16
		want float32
	}{
		{0x0000, 0},
		{0x3c00, 1},
		{0x3e00, 1.5},
		{0xc000, -2},
		{0x7bff, 65504},           // the largest finite half
		{0x0400, 1.0 / (1 << 14)}, // the smallest normal half
		{0x0001, 1.0 / (1 << 24)}, // the smallest subnormal half
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, float16To32(tt.bits), "bits %#04x", tt.bits)
	}
	assert.True(t, math.IsInf(float64(float16To32(0x7c00)), 1))
	assert.True(t, math.IsInf(float64(float16To32(0xfc00)), -1))
	assert.True(t, math.IsNaN(float64(float16To32(0x7e01))))
	assert.Equal(t, float32(math.Copysign(0, -1)), float16To32(0x8000))
}

// TestReadParquetReadsAFileWhereItLies covers the decision that keeps a load
// from holding a second copy of the file it is reading. The format is read at
// both ends and then by column chunk, so a source that can already serve a read
// at an offset is used as it stands; anything else has to be buffered, because
// a stream cannot go back.
func TestReadParquetReadsAFileWhereItLies(t *testing.T) {
	t.Parallel()

	type row struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}
	data := writeParquet(t, []row{{ID: 1, Name: "alice"}, {ID: 2, Name: "bob"}})

	path := filepath.Join(t.TempDir(), "people.parquet")
	require.NoError(t, os.WriteFile(path, data, 0o600))

	t.Run("a file is read where it lies", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(path) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, f.Close()) })

		at, size, ok := wholeFileAt(f)
		require.True(t, ok, "an open file is addressable")
		assert.Equal(t, int64(len(data)), size)
		assert.NotNil(t, at)
	})

	t.Run("a file already read from is not", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(path) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, f.Close()) })
		_, err = f.Read(make([]byte, 1))
		require.NoError(t, err)

		// What was taken is unknown here, so the file is no longer the whole
		// input and reading it from the start would read those bytes twice.
		_, _, ok := wholeFileAt(f)
		assert.False(t, ok)
	})

	t.Run("a stream is not", func(t *testing.T) {
		t.Parallel()

		_, _, ok := wholeFileAt(bufio.NewReader(bytes.NewReader(data)))
		assert.False(t, ok)
	})

	t.Run("both routes read the same table", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(path) //nolint:gosec // Test path from t.TempDir()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, f.Close()) })

		fromFile, fileChunks := collectParquet(t, nil, Options{}, f)
		fromStream, streamChunks := collectParquet(t, data, Options{}, nil)

		assert.Equal(t, fromStream.Header, fromFile.Header)
		assert.Equal(t, fromStream.Types, fromFile.Types)
		assert.Equal(t, fromStream.Rows, fromFile.Rows)
		require.Len(t, fileChunks, len(streamChunks))
		for i := range fileChunks {
			assert.Equal(t, streamChunks[i].Records, fileChunks[i].Records)
		}
	})
}
