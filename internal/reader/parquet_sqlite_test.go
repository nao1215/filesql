package reader

import (
	"math"
	"testing"

	"github.com/nao1215/filesql/internal/infer"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
)

// half encodes a float32 as the two little-endian bytes of an IEEE 754
// half-precision float, for the values these tests use. Only the values a half
// can hold exactly are encoded; the tests pick such values.
//
//nolint:gosec // the narrowing conversions are the bit packing itself
func half(f float32) []byte {
	bits := math.Float32bits(f)
	sign := uint16(bits>>31) << 15
	exponent := int32(bits>>23&0xff) - 127
	mantissa := bits & 0x7fffff
	switch {
	case exponent == 128: // infinities and NaN
		h := sign | 0x7c00
		if mantissa != 0 {
			h |= 0x200 // a quiet NaN
		}
		return []byte{byte(h), byte(h >> 8)}
	case f == 0:
		return []byte{byte(sign), byte(sign >> 8)}
	default:
		h := sign | uint16(exponent+15)<<10 | uint16(mantissa>>13)
		return []byte{byte(h), byte(h >> 8)}
	}
}

func TestRenderParquetValue_SQLiteRendering(t *testing.T) {
	t.Parallel()

	plain := parquetColumn{leaf: true}
	tests := []struct {
		name  string
		value parquet.Value
		col   parquetColumn
		want  string
	}{
		{name: "true is 1", value: parquet.BooleanValue(true), col: plain, want: "1"},
		{name: "false is 0", value: parquet.BooleanValue(false), col: plain, want: "0"},
		{name: "int32", value: parquet.Int32Value(100000), col: plain, want: "100000"},
		{name: "negative int32", value: parquet.Int32Value(-42), col: plain, want: "-42"},
		{name: "max int64", value: parquet.Int64Value(math.MaxInt64), col: plain, want: "9223372036854775807"},
		// An unsigned column stores its value in the physical int's bits, so
		// the all-ones patterns must come back as the unsigned maximums.
		{name: "max uint32", value: parquet.Int32Value(-1), col: parquetColumn{leaf: true, unsigned: true}, want: "4294967295"},
		{name: "max uint64", value: parquet.Int64Value(-1), col: parquetColumn{leaf: true, unsigned: true}, want: "18446744073709551615"},
		{name: "small unsigned", value: parquet.Int32Value(255), col: parquetColumn{leaf: true, unsigned: true}, want: "255"},
		{name: "float32", value: parquet.FloatValue(3.14159), col: plain, want: "3.14159"},
		{name: "float64", value: parquet.DoubleValue(2.718281828459045), col: plain, want: "2.718281828459045"},
		{name: "string", value: parquet.ByteArrayValue([]byte("Hello, World!")), col: plain, want: "Hello, World!"},
		{name: "empty string", value: parquet.ByteArrayValue(nil), col: plain, want: ""},
		{name: "binary", value: parquet.ByteArrayValue([]byte("binary data")), col: plain, want: "binary data"},
		// The temporal annotations keep the raw count the file stores: days
		// for DATE, and ticks of the schema's unit for TIMESTAMP.
		{name: "date days", value: parquet.Int32Value(18628), col: plain, want: "18628"},
		{name: "timestamp millis", value: parquet.Int64Value(1609459200000), col: plain, want: "1609459200000"},
		// The deprecated INT96 timestamp renders as nanoseconds since the
		// epoch: Julian day 2440589 is 1970-01-02, and one nanosecond of it.
		{name: "int96", value: parquet.Int96Value([3]uint32{1, 0, 2440589}), col: plain, want: "86400000000001"},
		// A DECIMAL renders its unscaled integer at its scale.
		{name: "decimal int32", value: parquet.Int32Value(12345), col: parquetColumn{leaf: true, decimal: true, scale: 2}, want: "123.45"},
		{name: "decimal int64 negative", value: parquet.Int64Value(-12345), col: parquetColumn{leaf: true, decimal: true, scale: 2}, want: "-123.45"},
		{name: "decimal below its scale", value: parquet.Int32Value(5), col: parquetColumn{leaf: true, decimal: true, scale: 2}, want: "0.05"},
		{name: "decimal at scale zero", value: parquet.Int64Value(7), col: parquetColumn{leaf: true, decimal: true, scale: 0}, want: "7"},
		{name: "decimal from bytes", value: parquet.FixedLenByteArrayValue([]byte{0x00, 0x00, 0x00, 0x30, 0x39}), col: parquetColumn{leaf: true, decimal: true, scale: 2}, want: "123.45"},
		{name: "negative decimal from bytes", value: parquet.FixedLenByteArrayValue([]byte{0xff, 0xff, 0xff, 0xcf, 0xc7}), col: parquetColumn{leaf: true, decimal: true, scale: 2}, want: "-123.45"},
		{name: "uuid", value: parquet.FixedLenByteArrayValue([]byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef}), col: parquetColumn{leaf: true, uuid: true}, want: "01234567-89ab-cdef-0123-456789abcdef"},
		{name: "fixed bytes stay bytes", value: parquet.FixedLenByteArrayValue([]byte("raw")), col: plain, want: "raw"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, renderParquetValue(tt.value, tt.col, RenderSQLite))
		})
	}
}

func TestParquetCellIsNull(t *testing.T) {
	t.Parallel()

	plain := parquetColumn{leaf: true}
	assert.True(t, parquetCellIsNull(parquet.NullValue(), plain, RenderSQLite))
	assert.True(t, parquetCellIsNull(parquet.NullValue(), plain, RenderPlain))
	assert.False(t, parquetCellIsNull(parquet.Int64Value(0), plain, RenderSQLite))

	// SQLite has no NaN, so a NaN is a null there and only there.
	nan32 := parquet.FloatValue(float32(math.NaN()))
	nan64 := parquet.DoubleValue(math.NaN())
	assert.True(t, parquetCellIsNull(nan32, plain, RenderSQLite))
	assert.True(t, parquetCellIsNull(nan64, plain, RenderSQLite))
	assert.False(t, parquetCellIsNull(nan32, plain, RenderPlain))
	assert.False(t, parquetCellIsNull(nan64, plain, RenderPlain))
	assert.False(t, parquetCellIsNull(parquet.DoubleValue(1.5), plain, RenderSQLite))
}

// TestFloat16RendersLikeEveryOtherReal covers the half-float column a Parquet
// file may hold, annotated FLOAT16 over two fixed bytes. Its type is read as a
// real number, a whole value keeps the point that keeps its column REAL, and a
// NaN is a null under the SQLite rendering, the same as the wider floats.
func TestFloat16RendersLikeEveryOtherReal(t *testing.T) {
	t.Parallel()

	col := parquetColumn{leaf: true, kind: parquet.FixedLenByteArray, float16: true}
	assert.Equal(t, infer.Real, col.columnType(RenderSQLite))

	assert.Equal(t, "1.5", renderParquetValue(parquet.FixedLenByteArrayValue(half(1.5)), col, RenderSQLite))
	assert.Equal(t, "3.0", renderParquetValue(parquet.FixedLenByteArrayValue(half(3)), col, RenderSQLite), "a whole real keeps its column REAL")
	assert.Equal(t, "-2.25", renderParquetValue(parquet.FixedLenByteArrayValue(half(-2.25)), col, RenderSQLite))

	nan := parquet.FixedLenByteArrayValue(half(float32(math.NaN())))
	assert.True(t, parquetCellIsNull(nan, col, RenderSQLite), "SQLite has no NaN, so a NaN is a null there")
	assert.False(t, parquetCellIsNull(nan, col, RenderPlain), "only a load turns a NaN into a null")
	assert.True(t, parquetCellIsNull(parquet.NullValue(), col, RenderSQLite))

	assert.Equal(t, "3", renderParquetValue(parquet.FixedLenByteArrayValue(half(3)), col, RenderPlain), "a plain read spells the number as it is")

	// The infinities survive as the literal SQLite overflows back to one, the
	// same spelling the wider floats use.
	assert.Equal(t, "9e999", renderParquetValue(parquet.FixedLenByteArrayValue(half(float32(math.Inf(1)))), col, RenderSQLite))
	assert.Equal(t, "-9e999", renderParquetValue(parquet.FixedLenByteArrayValue(half(float32(math.Inf(-1)))), col, RenderSQLite))

	// A subnormal half is a normal float32 and still a number.
	assert.Equal(t, "5.9604645e-08", renderParquetValue(parquet.FixedLenByteArrayValue([]byte{0x01, 0x00}), col, RenderPlain))
}
