package parser

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// csvColumnTypes is the type Parse gives each column of a CSV holding the given
// records. The rule these cases describe belongs to the reader now, and this is
// the path a caller takes to it.
func csvColumnTypes(t *testing.T, headers []string, records [][]string) []ColumnType {
	t.Helper()

	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	require.NoError(t, writer.Write(headers))
	require.NoError(t, writer.WriteAll(records))
	require.NoError(t, writer.Error())

	table, err := Parse(&buf, CSV)
	require.NoError(t, err)
	return table.ColumnTypes
}

func TestInferColumnTypes(t *testing.T) {
	t.Parallel()

	t.Run("infers integer type", func(t *testing.T) {
		t.Parallel()

		headers := []string{"count"}
		records := [][]string{{"1"}, {"2"}, {"3"}, {"4"}, {"5"}}

		types := csvColumnTypes(t, headers, records)

		assert.Equal(t, TypeInteger, types[0])
	})

	t.Run("infers real type", func(t *testing.T) {
		t.Parallel()

		headers := []string{"price"}
		records := [][]string{{"1.99"}, {"2.50"}, {"3.14"}, {"4.0"}, {"5.5"}}

		types := csvColumnTypes(t, headers, records)

		assert.Equal(t, TypeReal, types[0])
	})

	t.Run("infers text type for mixed data", func(t *testing.T) {
		t.Parallel()

		headers := []string{"mixed"}
		records := [][]string{{"hello"}, {"42"}, {"world"}, {"100"}, {"test"}}

		types := csvColumnTypes(t, headers, records)

		assert.Equal(t, TypeText, types[0])
	})

	t.Run("returns text for empty records", func(t *testing.T) {
		t.Parallel()

		headers := []string{"col"}
		records := [][]string{}

		types := csvColumnTypes(t, headers, records)

		assert.Equal(t, TypeText, types[0])
	})

	t.Run("promotes mixed integer and real values to real", func(t *testing.T) {
		t.Parallel()

		headers := []string{"amount"}
		records := [][]string{{"1"}, {"2"}, {"3"}, {"4"}, {"1.5"}}

		types := csvColumnTypes(t, headers, records)

		assert.Equal(t, TypeReal, types[0])
	})

	t.Run("a value the chosen type cannot hold decides the column", func(t *testing.T) {
		t.Parallel()

		// Each of these is a single value among plain integers. Weighing them
		// against the integers left a numeric column holding a value that is not
		// one, so the odd value is what the type has to follow.
		tests := map[string]string{
			"text":                "abc",
			"a zero padded code":  "007",
			"an int64 overflow":   "11040320260000000000",
			"a Go only literal":   "1_000",
			"a hexadecimal float": "0x1p4",
			"a padded number":     "  42",
			"a datetime":          "2026-08-20T10:00:00Z",
		}
		for name, odd := range tests {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				records := [][]string{{"1"}, {"2"}, {"3"}, {"4"}, {"5"}, {"6"}, {"7"}, {"8"}, {"9"}, {odd}}
				assert.Equal(t, TypeText, csvColumnTypes(t, []string{"v"}, records)[0])
			})
		}
	})

	t.Run("one decimal among integers makes the column real", func(t *testing.T) {
		t.Parallel()

		records := [][]string{{"1"}, {"2"}, {"3"}, {"4"}, {"5"}, {"6"}, {"7"}, {"8"}, {"9"}, {"10"}, {"11.5"}}

		assert.Equal(t, TypeReal, csvColumnTypes(t, []string{"v"}, records)[0])
	})

	t.Run("a column longer than a sample is read to its end", func(t *testing.T) {
		t.Parallel()

		const rows = 2000
		build := func(oddAt int) [][]string {
			records := make([][]string, 0, rows+1)
			for i := range rows {
				if i == oddAt {
					records = append(records, []string{"abc"})
				}
				records = append(records, []string{strconv.Itoa(i + 1)})
			}
			if oddAt == rows {
				records = append(records, []string{"abc"})
			}
			return records
		}

		// Neither end of a column is privileged: reading only the head made the
		// answer depend on where the value sat.
		for _, oddAt := range []int{0, rows / 2, rows} {
			assert.Equal(t, TypeText, csvColumnTypes(t, []string{"v"}, build(oddAt))[0],
				"a text value at position %d among %d integers", oddAt, rows)
		}
	})

	t.Run("every value of a column converts to the type the column was given", func(t *testing.T) {
		t.Parallel()

		columns := map[string][]string{
			"integers":                {"1", "2", "3"},
			"decimals":                {"1.5", "2", "3"},
			"text among integers":     {"1", "2", "3", "4", "5", "6", "7", "8", "9", "abc"},
			"a code among integers":   {"1", "2", "3", "4", "5", "6", "7", "8", "9", "007"},
			"datetimes":               {"2026-08-20", "2026-08-21"},
			"a datetime among counts": {"1", "2", "3", "4", "5", "6", "7", "8", "9", "2026-08-20"},
		}
		want := map[ColumnType]string{
			TypeInteger:  "int64",
			TypeReal:     "float64",
			TypeText:     "string",
			TypeDatetime: "string",
		}

		for name, values := range columns {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				records := make([][]string, 0, len(values))
				for _, v := range values {
					records = append(records, []string{v})
				}
				columnType := csvColumnTypes(t, []string{"v"}, records)[0]

				for _, v := range values {
					assert.Equal(t, want[columnType], fmt.Sprintf("%T", ParseValue(v, columnType)),
						"%q in a column typed %v", v, columnType)
				}
			})
		}
	})
}

func TestParseValue(t *testing.T) {
	t.Parallel()

	t.Run("parses integer", func(t *testing.T) {
		t.Parallel()

		result := ParseValue("42", TypeInteger)

		assert.Equal(t, int64(42), result)
	})

	t.Run("parses float", func(t *testing.T) {
		t.Parallel()

		result := ParseValue("3.14", TypeReal)

		assert.Equal(t, 3.14, result)
	})

	t.Run("a saturating spelling parses to the infinity it saturates to", func(t *testing.T) {
		t.Parallel()

		// 9e999 is a REAL to the inference because SQLite's affinity saturates
		// it, so the parsed value has to be that float rather than the string.
		assert.Equal(t, math.Inf(1), ParseValue("9e999", TypeReal))
		assert.Equal(t, math.Inf(-1), ParseValue("-9e999", TypeReal))
	})

	t.Run("returns string for text type", func(t *testing.T) {
		t.Parallel()

		result := ParseValue("hello", TypeText)

		assert.Equal(t, "hello", result)
	})

	t.Run("preserves whitespace for text type", func(t *testing.T) {
		t.Parallel()

		result := ParseValue("  hello  ", TypeText)

		assert.Equal(t, "  hello  ", result)
	})

	t.Run("preserves whitespace for datetime type", func(t *testing.T) {
		t.Parallel()

		result := ParseValue(" 2024-01-15 ", TypeDatetime)

		assert.Equal(t, " 2024-01-15 ", result)
	})

	t.Run("returns nil for empty value", func(t *testing.T) {
		t.Parallel()

		result := ParseValue("", TypeInteger)

		assert.Nil(t, result)
	})

	t.Run("returns original string for invalid integer", func(t *testing.T) {
		t.Parallel()

		result := ParseValue("not-a-number", TypeInteger)

		assert.Equal(t, "not-a-number", result)
	})
}
