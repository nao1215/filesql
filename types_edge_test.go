package filesql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestNewChunkSize_BelowTheMinimum checks the floor on a chunk size. A chunk of
// zero or fewer rows would read a file forever, so anything under the minimum
// falls back to the default.
func TestNewChunkSize_BelowTheMinimum(t *testing.T) {
	t.Parallel()

	assert.Equal(t, chunkSizeValue(DefaultRowsPerChunk), newChunkSize(0))
	assert.Equal(t, chunkSizeValue(DefaultRowsPerChunk), newChunkSize(-1))
	assert.Equal(t, chunkSizeValue(MinChunkSize), newChunkSize(MinChunkSize))
}

// TestNewColumnInfoList_NoColumns covers a header with nothing in it, which is
// what an input with no columns produces.
func TestNewColumnInfoList_NoColumns(t *testing.T) {
	t.Parallel()

	assert.Nil(t, newColumnInfoList(newHeader(nil), nil))
	assert.Nil(t, inferColumnsInfo(newHeader(nil), nil))
}

// TestColumnInfoList_EqualTypes covers the comparison that decides whether a
// later chunk widens the table already created.
func TestColumnInfoList_EqualTypes(t *testing.T) {
	t.Parallel()

	integers := columnInfoList{{Name: "a", Type: columnTypeInteger}}
	texts := columnInfoList{{Name: "a", Type: columnTypeText}}

	assert.True(t, integers.equalTypes(columnInfoList{{Name: "a", Type: columnTypeInteger}}))
	assert.False(t, integers.equalTypes(texts), "a widened column is not the same schema")
	assert.False(t, integers.equalTypes(columnInfoList{}), "a different column count is not the same schema")
}

// TestInferColumnType_NoValues covers a column with no values to judge by. Text
// is the only type that holds anything a later row can bring.
func TestInferColumnType_NoValues(t *testing.T) {
	t.Parallel()

	assert.Equal(t, columnTypeText, inferColumnType(nil))
}

// TestSelectColumnType_WithoutAConfidentMajority covers the fallbacks used when
// no type reaches the confidence threshold. The column still has to be declared
// as something, and the numeric types are preferred over text in the order that
// keeps values readable.
func TestSelectColumnType_WithoutAConfidentMajority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		typeCounts map[columnType]int
		totalCount int
		want       columnType
	}{
		{
			name:       "a few reals among empty values",
			typeCounts: map[columnType]int{columnTypeReal: 1},
			totalCount: 100,
			want:       columnTypeReal,
		},
		{
			name:       "a few integers among empty values",
			typeCounts: map[columnType]int{columnTypeInteger: 1},
			totalCount: 100,
			want:       columnTypeInteger,
		},
		{
			name:       "a few datetimes among empty values",
			typeCounts: map[columnType]int{columnTypeDatetime: 1},
			totalCount: 100,
			want:       columnTypeDatetime,
		},
		{
			name:       "nothing classified at all",
			typeCounts: map[columnType]int{},
			totalCount: 100,
			want:       columnTypeText,
		},
		{
			name:       "a datetime beside a number has no type covering both",
			typeCounts: map[columnType]int{columnTypeDatetime: 5, columnTypeInteger: 5},
			totalCount: 10,
			want:       columnTypeText,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, selectColumnType(tt.typeCounts, tt.totalCount))
		})
	}
}

// TestIsIntegerLiteralOverflowingInt64_SignOnly covers a value that is a sign
// and nothing else, which is not a number at all.
func TestIsIntegerLiteralOverflowingInt64_SignOnly(t *testing.T) {
	t.Parallel()

	assert.False(t, isIntegerLiteralOverflowingInt64("+"))
	assert.False(t, isIntegerLiteralOverflowingInt64("-"))
}
