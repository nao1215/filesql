package reader

import (
	"bytes"
	"io"
	"math"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet"
	pqfile "github.com/apache/arrow/go/v18/parquet/file"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBytesReaderAt_ReadAt(t *testing.T) {
	t.Parallel()

	t.Run("reads data at specified offset", func(t *testing.T) {
		t.Parallel()

		data := []byte("hello world")
		br := &bytesReaderAt{data: data}
		buf := make([]byte, 5)

		n, err := br.ReadAt(buf, 0)

		assert.NoError(t, err)
		assert.Equal(t, 5, n)
		assert.Equal(t, []byte("hello"), buf)
	})

	t.Run("reads data from middle offset", func(t *testing.T) {
		t.Parallel()

		data := []byte("hello world")
		br := &bytesReaderAt{data: data}
		buf := make([]byte, 5)

		n, err := br.ReadAt(buf, 6)

		assert.NoError(t, err)
		assert.Equal(t, 5, n)
		assert.Equal(t, []byte("world"), buf)
	})

	t.Run("returns EOF when offset beyond data", func(t *testing.T) {
		t.Parallel()

		data := []byte("hello")
		br := &bytesReaderAt{data: data}
		buf := make([]byte, 5)

		n, err := br.ReadAt(buf, 100)

		assert.Equal(t, io.EOF, err)
		assert.Equal(t, 0, n)
	})

	t.Run("returns EOF when buffer larger than remaining data", func(t *testing.T) {
		t.Parallel()

		data := []byte("hi")
		br := &bytesReaderAt{data: data}
		buf := make([]byte, 10)

		n, err := br.ReadAt(buf, 0)

		assert.Equal(t, io.EOF, err)
		assert.Equal(t, 2, n)
		assert.Equal(t, []byte("hi"), buf[:2])
	})
}

func TestBytesReaderAt_Size(t *testing.T) {
	t.Parallel()

	t.Run("returns correct size", func(t *testing.T) {
		t.Parallel()

		data := []byte("hello world")
		br := &bytesReaderAt{data: data}

		assert.Equal(t, int64(11), br.Size())
	})

	t.Run("returns zero for empty data", func(t *testing.T) {
		t.Parallel()

		br := &bytesReaderAt{data: []byte{}}

		assert.Equal(t, int64(0), br.Size())
	})
}

func TestBytesReaderAt_Read(t *testing.T) {
	t.Parallel()

	t.Run("reads data sequentially", func(t *testing.T) {
		t.Parallel()

		data := []byte("hello world")
		br := &bytesReaderAt{data: data}
		buf := make([]byte, 5)

		n, err := br.Read(buf)

		assert.NoError(t, err)
		assert.Equal(t, 5, n)
		assert.Equal(t, []byte("hello"), buf)

		n, err = br.Read(buf)

		assert.NoError(t, err)
		assert.Equal(t, 5, n)
		assert.Equal(t, []byte(" worl"), buf)
	})

	t.Run("returns EOF when offset beyond data", func(t *testing.T) {
		t.Parallel()

		data := []byte("hi")
		br := &bytesReaderAt{data: data, offset: 10}
		buf := make([]byte, 5)

		n, err := br.Read(buf)

		assert.Equal(t, io.EOF, err)
		assert.Equal(t, 0, n)
	})
}

func TestBytesReaderAt_Seek(t *testing.T) {
	t.Parallel()

	t.Run("seeks from start", func(t *testing.T) {
		t.Parallel()

		data := []byte("hello world")
		br := &bytesReaderAt{data: data}

		newOffset, err := br.Seek(5, io.SeekStart)

		assert.NoError(t, err)
		assert.Equal(t, int64(5), newOffset)
		assert.Equal(t, int64(5), br.offset)
	})

	t.Run("seeks from current position", func(t *testing.T) {
		t.Parallel()

		data := []byte("hello world")
		br := &bytesReaderAt{data: data, offset: 3}

		newOffset, err := br.Seek(2, io.SeekCurrent)

		assert.NoError(t, err)
		assert.Equal(t, int64(5), newOffset)
	})

	t.Run("seeks from end", func(t *testing.T) {
		t.Parallel()

		data := []byte("hello world")
		br := &bytesReaderAt{data: data}

		newOffset, err := br.Seek(-5, io.SeekEnd)

		assert.NoError(t, err)
		assert.Equal(t, int64(6), newOffset)
	})

	t.Run("returns error for invalid whence", func(t *testing.T) {
		t.Parallel()

		data := []byte("hello")
		br := &bytesReaderAt{data: data}

		_, err := br.Seek(0, 999)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid whence")
	})

	t.Run("returns error for negative position", func(t *testing.T) {
		t.Parallel()

		data := []byte("hello")
		br := &bytesReaderAt{data: data}

		_, err := br.Seek(-10, io.SeekStart)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "negative position")
	})
}

func TestExtractValueFromArrowArray(t *testing.T) {
	t.Parallel()

	pool := memory.NewGoAllocator()

	t.Run("extracts int64 value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewInt64Builder(pool)
		defer builder.Release()
		builder.AppendValues([]int64{42, 100, -5}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "42", extractValueFromArrowArray(arr, 0, RenderPlain))
		assert.Equal(t, "100", extractValueFromArrowArray(arr, 1, RenderPlain))
		assert.Equal(t, "-5", extractValueFromArrowArray(arr, 2, RenderPlain))
	})

	t.Run("extracts string value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewStringBuilder(pool)
		defer builder.Release()
		builder.AppendValues([]string{"hello", "world"}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "hello", extractValueFromArrowArray(arr, 0, RenderPlain))
		assert.Equal(t, "world", extractValueFromArrowArray(arr, 1, RenderPlain))
	})

	t.Run("extracts float64 value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewFloat64Builder(pool)
		defer builder.Release()
		builder.AppendValues([]float64{3.14, 2.71}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "3.14", extractValueFromArrowArray(arr, 0, RenderPlain))
		assert.Equal(t, "2.71", extractValueFromArrowArray(arr, 1, RenderPlain))
	})

	t.Run("extracts boolean value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewBooleanBuilder(pool)
		defer builder.Release()
		builder.AppendValues([]bool{true, false}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "true", extractValueFromArrowArray(arr, 0, RenderPlain))
		assert.Equal(t, "false", extractValueFromArrowArray(arr, 1, RenderPlain))
	})

	t.Run("returns empty string for null value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewInt64Builder(pool)
		defer builder.Release()
		builder.AppendNull()
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "", extractValueFromArrowArray(arr, 0, RenderPlain))
	})

	t.Run("extracts int8 value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewInt8Builder(pool)
		defer builder.Release()
		builder.AppendValues([]int8{127, -128}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "127", extractValueFromArrowArray(arr, 0, RenderPlain))
		assert.Equal(t, "-128", extractValueFromArrowArray(arr, 1, RenderPlain))
	})

	t.Run("extracts int16 value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewInt16Builder(pool)
		defer builder.Release()
		builder.AppendValues([]int16{32767, -32768}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "32767", extractValueFromArrowArray(arr, 0, RenderPlain))
		assert.Equal(t, "-32768", extractValueFromArrowArray(arr, 1, RenderPlain))
	})

	t.Run("extracts int32 value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewInt32Builder(pool)
		defer builder.Release()
		builder.AppendValues([]int32{2147483647, -2147483648}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "2147483647", extractValueFromArrowArray(arr, 0, RenderPlain))
		assert.Equal(t, "-2147483648", extractValueFromArrowArray(arr, 1, RenderPlain))
	})

	t.Run("extracts uint8 value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewUint8Builder(pool)
		defer builder.Release()
		builder.AppendValues([]uint8{0, 255}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "0", extractValueFromArrowArray(arr, 0, RenderPlain))
		assert.Equal(t, "255", extractValueFromArrowArray(arr, 1, RenderPlain))
	})

	t.Run("extracts uint16 value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewUint16Builder(pool)
		defer builder.Release()
		builder.AppendValues([]uint16{0, 65535}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "0", extractValueFromArrowArray(arr, 0, RenderPlain))
		assert.Equal(t, "65535", extractValueFromArrowArray(arr, 1, RenderPlain))
	})

	t.Run("extracts uint32 value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewUint32Builder(pool)
		defer builder.Release()
		builder.AppendValues([]uint32{0, 4294967295}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "0", extractValueFromArrowArray(arr, 0, RenderPlain))
		assert.Equal(t, "4294967295", extractValueFromArrowArray(arr, 1, RenderPlain))
	})

	t.Run("extracts uint64 value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewUint64Builder(pool)
		defer builder.Release()
		builder.AppendValues([]uint64{0, 18446744073709551615}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "0", extractValueFromArrowArray(arr, 0, RenderPlain))
		assert.Equal(t, "18446744073709551615", extractValueFromArrowArray(arr, 1, RenderPlain))
	})

	t.Run("extracts float32 value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewFloat32Builder(pool)
		defer builder.Release()
		builder.AppendValues([]float32{1.5, 2.5}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "1.5", extractValueFromArrowArray(arr, 0, RenderPlain))
		assert.Equal(t, "2.5", extractValueFromArrowArray(arr, 1, RenderPlain))
	})

	// An infinity has no decimal spelling, and SQLite's REAL affinity cannot read
	// "+Inf": rendered that way it lands in a column declared REAL as text. The
	// literal below is one SQLite overflows to an infinity.
	t.Run("renders a float32 infinity as a literal SQLite reads as one", func(t *testing.T) {
		t.Parallel()

		builder := array.NewFloat32Builder(pool)
		defer builder.Release()
		builder.AppendValues([]float32{float32(math.Inf(1)), float32(math.Inf(-1)), float32(math.NaN()), 3.14159}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "9e999", extractValueFromArrowArray(arr, 0, RenderPlain))
		assert.Equal(t, "-9e999", extractValueFromArrowArray(arr, 1, RenderPlain))
		assert.Equal(t, "", extractValueFromArrowArray(arr, 2, RenderPlain))
		// The width is the array's, not float64's: through a float64 this would be
		// its expansion, 3.141590118408203.
		assert.Equal(t, "3.14159", extractValueFromArrowArray(arr, 3, RenderPlain))
	})

	t.Run("renders a float64 infinity the same way", func(t *testing.T) {
		t.Parallel()

		builder := array.NewFloat64Builder(pool)
		defer builder.Release()
		builder.AppendValues([]float64{math.Inf(1), math.Inf(-1), math.NaN(), 1.5}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "9e999", extractValueFromArrowArray(arr, 0, RenderPlain))
		assert.Equal(t, "-9e999", extractValueFromArrowArray(arr, 1, RenderPlain))
		assert.Equal(t, "", extractValueFromArrowArray(arr, 2, RenderPlain))
		assert.Equal(t, "1.5", extractValueFromArrowArray(arr, 3, RenderPlain))
	})

	t.Run("extracts binary value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary)
		defer builder.Release()
		builder.AppendValues([][]byte{[]byte("hello"), []byte("world")}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "hello", extractValueFromArrowArray(arr, 0, RenderPlain))
		assert.Equal(t, "world", extractValueFromArrowArray(arr, 1, RenderPlain))
	})

	t.Run("extracts date32 value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewDate32Builder(pool)
		defer builder.Release()
		builder.AppendValues([]arrow.Date32{19000, 19001}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "19000", extractValueFromArrowArray(arr, 0, RenderPlain))
		assert.Equal(t, "19001", extractValueFromArrowArray(arr, 1, RenderPlain))
	})

	t.Run("extracts date64 value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewDate64Builder(pool)
		defer builder.Release()
		builder.AppendValues([]arrow.Date64{1641024000000, 1641110400000}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "1641024000000", extractValueFromArrowArray(arr, 0, RenderPlain))
		assert.Equal(t, "1641110400000", extractValueFromArrowArray(arr, 1, RenderPlain))
	})

	t.Run("extracts timestamp value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Millisecond})
		defer builder.Release()
		builder.AppendValues([]arrow.Timestamp{1641024000000, 1641110400000}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "1641024000000", extractValueFromArrowArray(arr, 0, RenderPlain))
		assert.Equal(t, "1641110400000", extractValueFromArrowArray(arr, 1, RenderPlain))
	})
}

// TestParquetChunksLieInTheFile pins the bound that keeps a damaged file from
// being decoded: a column chunk has to name bytes the file actually holds.
//
// The undamaged case says the bound is loose enough for a real file, and the
// short case says it catches a chunk running past the end. The size is varied
// rather than the file, because a chunk that overruns is a disagreement between
// the metadata and the length, and this is the honest way to state it without
// hand-editing a Thrift footer.
func TestParquetChunksLieInTheFile(t *testing.T) {
	t.Parallel()

	data := buildParquetForBounds(t)
	reader, err := pqfile.NewParquetReader(&bytesReaderAt{data: data})
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })

	assert.NoError(t, parquetChunksLieInTheFile(reader, int64(len(data))),
		"a file that holds its own chunks was refused")

	chunk, err := reader.MetaData().RowGroup(0).ColumnChunk(0)
	require.NoError(t, err)

	tests := map[string]int64{
		// Short enough that the data page begins past the end.
		"a page declared outside the file": chunk.DataPageOffset() - 1,
		// Long enough to hold both page offsets and too short to hold the
		// bytes the chunk says it occupies.
		"a chunk running past the end": chunk.DataPageOffset() + 1,
	}

	for name, size := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := parquetChunksLieInTheFile(reader, size)
			require.Error(t, err, "a file of %d bytes was allowed to hold this chunk", size)
			var readErr *Error
			require.ErrorAs(t, err, &readErr)
			assert.Equal(t, KindParse, readErr.Kind)
		})
	}
}

// buildParquetForBounds writes a small Parquet file with one column of values,
// which is enough to have a dictionary page and a data page to bound.
func buildParquetForBounds(t *testing.T) []byte {
	t.Helper()

	schema := arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil)
	pool := memory.NewGoAllocator()
	builder := array.NewInt64Builder(pool)
	defer builder.Release()
	builder.AppendValues([]int64{1, 2, 3}, nil)
	column := builder.NewArray()
	defer column.Release()
	record := array.NewRecord(schema, []arrow.Array{column}, 3)
	defer record.Release()
	table := array.NewTableFromRecords(schema, []arrow.Record{record})
	defer table.Release()

	var buf bytes.Buffer
	require.NoError(t, pqarrow.WriteTable(table, &buf, 1024, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps()))
	return buf.Bytes()
}
