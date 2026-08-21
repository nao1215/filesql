package reader

import (
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestExtractValueFromArrowArray_SQLiteRendering(t *testing.T) {
	t.Parallel()
	pool := memory.NewGoAllocator()

	t.Run("Boolean array", func(t *testing.T) {
		builder := array.NewBooleanBuilder(pool)
		defer builder.Release()

		// Add test values: true, false, null
		builder.Append(true)
		builder.Append(false)
		builder.AppendNull()

		arr := builder.NewBooleanArray()
		defer arr.Release()

		// Test true value
		result := extractValueFromArrowArray(arr, 0, RenderSQLite)
		if result != "1" {
			assert.Fail(t, "Expected '1' for true, got '%s'", result)
		}

		// Test false value
		result = extractValueFromArrowArray(arr, 1, RenderSQLite)
		if result != "0" {
			assert.Fail(t, "Expected '0' for false, got '%s'", result)
		}

		// Test null value
		result = extractValueFromArrowArray(arr, 2, RenderSQLite)
		if result != "" {
			assert.Fail(t, "Expected empty string for null, got '%s'", result)
		}
	})

	t.Run("Integer arrays", func(t *testing.T) {
		// Test Int8
		int8Builder := array.NewInt8Builder(pool)
		defer int8Builder.Release()
		int8Builder.Append(42)
		int8Builder.AppendNull()
		int8Arr := int8Builder.NewInt8Array()
		defer int8Arr.Release()

		result := extractValueFromArrowArray(int8Arr, 0, RenderSQLite)
		if result != "42" {
			assert.Fail(t, "Expected '42' for int8, got '%s'", result)
		}
		result = extractValueFromArrowArray(int8Arr, 1, RenderSQLite)
		if result != "" {
			assert.Fail(t, "Expected empty string for null int8, got '%s'", result)
		}

		// Test Int16
		int16Builder := array.NewInt16Builder(pool)
		defer int16Builder.Release()
		int16Builder.Append(1000)
		int16Arr := int16Builder.NewInt16Array()
		defer int16Arr.Release()

		result = extractValueFromArrowArray(int16Arr, 0, RenderSQLite)
		if result != "1000" {
			assert.Fail(t, "Expected '1000' for int16, got '%s'", result)
		}

		// Test Int32
		int32Builder := array.NewInt32Builder(pool)
		defer int32Builder.Release()
		int32Builder.Append(100000)
		int32Arr := int32Builder.NewInt32Array()
		defer int32Arr.Release()

		result = extractValueFromArrowArray(int32Arr, 0, RenderSQLite)
		if result != "100000" {
			assert.Fail(t, "Expected '100000' for int32, got '%s'", result)
		}

		// Test Int64
		int64Builder := array.NewInt64Builder(pool)
		defer int64Builder.Release()
		int64Builder.Append(9223372036854775807) // Max int64
		int64Arr := int64Builder.NewInt64Array()
		defer int64Arr.Release()

		result = extractValueFromArrowArray(int64Arr, 0, RenderSQLite)
		if result != "9223372036854775807" {
			assert.Fail(t, "Expected '9223372036854775807' for int64, got '%s'", result)
		}
	})

	t.Run("Unsigned integer arrays", func(t *testing.T) {
		// Test Uint8
		uint8Builder := array.NewUint8Builder(pool)
		defer uint8Builder.Release()
		uint8Builder.Append(255)
		uint8Arr := uint8Builder.NewUint8Array()
		defer uint8Arr.Release()

		result := extractValueFromArrowArray(uint8Arr, 0, RenderSQLite)
		if result != "255" {
			assert.Fail(t, "Expected '255' for uint8, got '%s'", result)
		}

		// Test Uint16
		uint16Builder := array.NewUint16Builder(pool)
		defer uint16Builder.Release()
		uint16Builder.Append(65535)
		uint16Arr := uint16Builder.NewUint16Array()
		defer uint16Arr.Release()

		result = extractValueFromArrowArray(uint16Arr, 0, RenderSQLite)
		if result != "65535" {
			assert.Fail(t, "Expected '65535' for uint16, got '%s'", result)
		}

		// Test Uint32
		uint32Builder := array.NewUint32Builder(pool)
		defer uint32Builder.Release()
		uint32Builder.Append(4294967295)
		uint32Arr := uint32Builder.NewUint32Array()
		defer uint32Arr.Release()

		result = extractValueFromArrowArray(uint32Arr, 0, RenderSQLite)
		if result != "4294967295" {
			assert.Fail(t, "Expected '4294967295' for uint32, got '%s'", result)
		}

		// Test Uint64
		uint64Builder := array.NewUint64Builder(pool)
		defer uint64Builder.Release()
		uint64Builder.Append(18446744073709551615) // Max uint64
		uint64Arr := uint64Builder.NewUint64Array()
		defer uint64Arr.Release()

		result = extractValueFromArrowArray(uint64Arr, 0, RenderSQLite)
		if result != "18446744073709551615" {
			assert.Fail(t, "Expected '18446744073709551615' for uint64, got '%s'", result)
		}
	})

	t.Run("Float arrays", func(t *testing.T) {
		// Test Float32
		float32Builder := array.NewFloat32Builder(pool)
		defer float32Builder.Release()
		float32Builder.Append(3.14159)
		float32Builder.AppendNull()
		float32Arr := float32Builder.NewFloat32Array()
		defer float32Arr.Release()

		result := extractValueFromArrowArray(float32Arr, 0, RenderSQLite)
		if result != "3.14159" {
			assert.Fail(t, "Expected '3.14159' for float32, got '%s'", result)
		}
		result = extractValueFromArrowArray(float32Arr, 1, RenderSQLite)
		if result != "" {
			assert.Fail(t, "Expected empty string for null float32, got '%s'", result)
		}

		// Test Float64
		float64Builder := array.NewFloat64Builder(pool)
		defer float64Builder.Release()
		float64Builder.Append(2.718281828459045)
		float64Arr := float64Builder.NewFloat64Array()
		defer float64Arr.Release()

		result = extractValueFromArrowArray(float64Arr, 0, RenderSQLite)
		if result != "2.718281828459045" {
			assert.Fail(t, "Expected '2.718281828459045' for float64, got '%s'", result)
		}
	})

	t.Run("String array", func(t *testing.T) {
		stringBuilder := array.NewStringBuilder(pool)
		defer stringBuilder.Release()

		stringBuilder.Append("Hello, World!")
		stringBuilder.Append("")
		stringBuilder.AppendNull()

		stringArr := stringBuilder.NewStringArray()
		defer stringArr.Release()

		// Test normal string
		result := extractValueFromArrowArray(stringArr, 0, RenderSQLite)
		if result != "Hello, World!" {
			assert.Fail(t, "Expected 'Hello, World!', got '%s'", result)
		}

		// Test empty string
		result = extractValueFromArrowArray(stringArr, 1, RenderSQLite)
		if result != "" {
			assert.Fail(t, "Expected empty string, got '%s'", result)
		}

		// Test null string
		result = extractValueFromArrowArray(stringArr, 2, RenderSQLite)
		if result != "" {
			assert.Fail(t, "Expected empty string for null, got '%s'", result)
		}
	})

	t.Run("Binary array", func(t *testing.T) {
		binaryBuilder := array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary)
		defer binaryBuilder.Release()

		testData := []byte("binary data")
		binaryBuilder.Append(testData)
		binaryBuilder.AppendNull()

		binaryArr := binaryBuilder.NewBinaryArray()
		defer binaryArr.Release()

		// Test binary data
		result := extractValueFromArrowArray(binaryArr, 0, RenderSQLite)
		if result != "binary data" {
			assert.Fail(t, "Expected 'binary data', got '%s'", result)
		}

		// Test null binary
		result = extractValueFromArrowArray(binaryArr, 1, RenderSQLite)
		if result != "" {
			assert.Fail(t, "Expected empty string for null binary, got '%s'", result)
		}
	})

	t.Run("Date arrays", func(t *testing.T) {
		// Test Date32
		date32Builder := array.NewDate32Builder(pool)
		defer date32Builder.Release()
		date32Builder.Append(arrow.Date32(18628)) // Some arbitrary date
		date32Arr := date32Builder.NewDate32Array()
		defer date32Arr.Release()

		result := extractValueFromArrowArray(date32Arr, 0, RenderSQLite)
		if result != "18628" {
			assert.Fail(t, "Expected '18628' for date32, got '%s'", result)
		}

		// Test Date64
		date64Builder := array.NewDate64Builder(pool)
		defer date64Builder.Release()
		date64Builder.Append(arrow.Date64(1609459200000)) // 2021-01-01 in milliseconds
		date64Arr := date64Builder.NewDate64Array()
		defer date64Arr.Release()

		result = extractValueFromArrowArray(date64Arr, 0, RenderSQLite)
		if result != "1609459200000" {
			assert.Fail(t, "Expected '1609459200000' for date64, got '%s'", result)
		}
	})

	t.Run("Timestamp array", func(t *testing.T) {
		timestampBuilder := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Millisecond})
		defer timestampBuilder.Release()
		timestampBuilder.Append(arrow.Timestamp(1609459200000)) // 2021-01-01 in milliseconds
		timestampArr := timestampBuilder.NewTimestampArray()
		defer timestampArr.Release()

		result := extractValueFromArrowArray(timestampArr, 0, RenderSQLite)
		if result != "1609459200000" {
			assert.Fail(t, "Expected '1609459200000' for timestamp, got '%s'", result)
		}
	})

	t.Run("Default case with unsupported type", func(t *testing.T) {
		// Create a list array (unsupported type)
		listBuilder := array.NewListBuilder(pool, arrow.PrimitiveTypes.Int32)
		defer listBuilder.Release()

		valueBuilder, ok := listBuilder.ValueBuilder().(*array.Int32Builder)
		if !ok {
			t.Fatal("Failed to cast value builder to Int32Builder")
		}
		listBuilder.Append(true)
		valueBuilder.Append(1)
		valueBuilder.Append(2)
		valueBuilder.Append(3)

		listArr := listBuilder.NewListArray()
		defer listArr.Release()

		// This should hit the default case
		result := extractValueFromArrowArray(listArr, 0, RenderSQLite)

		// The result should be some string representation - we don't check exact format
		// since it uses GetOneForMarshal which may vary
		if result == "" {
			t.Error("Expected some string representation for unsupported type, got empty string")
		}
	})
}
