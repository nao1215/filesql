package parser

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse_JSON(t *testing.T) {
	t.Parallel()

	t.Run("parses JSON array with objects", func(t *testing.T) {
		t.Parallel()

		input := `[{"name":"Alice","age":30},{"name":"Bob","age":25}]`
		reader := strings.NewReader(input)

		result, err := Parse(reader, JSON)

		require.NoError(t, err)
		assert.Equal(t, []string{"data"}, result.Headers)
		assert.Equal(t, 2, len(result.Records))
		// Each record contains raw JSON
		assert.True(t, json.Valid([]byte(result.Records[0][0])))
		assert.True(t, json.Valid([]byte(result.Records[1][0])))
	})

	t.Run("parses JSON array with primitives", func(t *testing.T) {
		t.Parallel()

		input := `[1, "hello", true, null]`
		reader := strings.NewReader(input)

		result, err := Parse(reader, JSON)

		require.NoError(t, err)
		assert.Equal(t, []string{"data"}, result.Headers)
		assert.Equal(t, 4, len(result.Records))
		assert.Equal(t, "1", result.Records[0][0])
		assert.Equal(t, `"hello"`, result.Records[1][0])
		assert.Equal(t, "true", result.Records[2][0])
		assert.Equal(t, "null", result.Records[3][0])
	})

	t.Run("parses JSON single object", func(t *testing.T) {
		t.Parallel()

		input := `{"name":"Alice","age":30}`
		reader := strings.NewReader(input)

		result, err := Parse(reader, JSON)

		require.NoError(t, err)
		assert.Equal(t, []string{"data"}, result.Headers)
		assert.Equal(t, 1, len(result.Records))
		assert.True(t, json.Valid([]byte(result.Records[0][0])))
	})

	t.Run("preserves nested JSON structure", func(t *testing.T) {
		t.Parallel()

		input := `[{"id":1,"address":{"city":"Tokyo","country":"Japan"},"tags":["dev","go"]}]`
		reader := strings.NewReader(input)

		result, err := Parse(reader, JSON)

		require.NoError(t, err)
		assert.Equal(t, 1, len(result.Records))

		// Verify the nested structure is preserved as valid JSON
		var parsed map[string]any
		err = json.Unmarshal([]byte(result.Records[0][0]), &parsed)
		require.NoError(t, err)

		address, ok := parsed["address"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "Tokyo", address["city"])
	})

	t.Run("column type is TEXT", func(t *testing.T) {
		t.Parallel()

		input := `[{"name":"Alice"}]`
		reader := strings.NewReader(input)

		result, err := Parse(reader, JSON)

		require.NoError(t, err)
		assert.Equal(t, []ColumnType{TypeText}, result.ColumnTypes)
	})

	t.Run("handles whitespace around JSON", func(t *testing.T) {
		t.Parallel()

		input := `  [{"name":"Alice"}]  `
		reader := strings.NewReader(input)

		result, err := Parse(reader, JSON)

		require.NoError(t, err)
		assert.Equal(t, 1, len(result.Records))
	})

	t.Run("returns error for empty input", func(t *testing.T) {
		t.Parallel()

		reader := strings.NewReader("")

		_, err := Parse(reader, JSON)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "empty JSON data")
	})

	t.Run("returns error for whitespace-only input", func(t *testing.T) {
		t.Parallel()

		reader := strings.NewReader("   \n\t  ")

		_, err := Parse(reader, JSON)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "empty JSON data")
	})

	t.Run("an empty array is a table with no rows", func(t *testing.T) {
		t.Parallel()

		// It used to be an error, where loading the same bytes through filesql
		// gives a table with no rows: an array with nothing in it is a document
		// that says there is nothing, not a document that cannot be read.
		reader := strings.NewReader("[]")

		td, err := Parse(reader, JSON)

		require.NoError(t, err)
		assert.Equal(t, []string{"data"}, td.Headers)
		assert.Empty(t, td.Records)
	})

	t.Run("a null root is one row holding null", func(t *testing.T) {
		t.Parallel()

		// "null" unmarshals into a slice as the empty slice, so the array branch
		// swallowed it and answered "empty JSON array" about a document holding
		// no array at all.
		reader := strings.NewReader("null")

		td, err := Parse(reader, JSON)

		require.NoError(t, err)
		assert.Equal(t, [][]string{{"null"}}, td.Records)
	})

	t.Run("returns error for invalid JSON", func(t *testing.T) {
		t.Parallel()

		reader := strings.NewReader("{invalid json}")

		_, err := Parse(reader, JSON)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse JSON")
	})

	t.Run("returns error for nil reader", func(t *testing.T) {
		t.Parallel()

		_, err := Parse(nil, JSON)

		assert.Error(t, err)
	})
}

func TestParse_JSONL(t *testing.T) {
	t.Parallel()

	t.Run("parses JSONL with multiple lines", func(t *testing.T) {
		t.Parallel()

		input := `{"name":"Alice","age":30}
{"name":"Bob","age":25}
{"name":"Charlie","age":35}`
		reader := strings.NewReader(input)

		result, err := Parse(reader, JSONL)

		require.NoError(t, err)
		assert.Equal(t, []string{"data"}, result.Headers)
		assert.Equal(t, 3, len(result.Records))
		for _, rec := range result.Records {
			assert.True(t, json.Valid([]byte(rec[0])))
		}
	})

	t.Run("skips empty lines", func(t *testing.T) {
		t.Parallel()

		input := `{"name":"Alice"}

{"name":"Bob"}

`
		reader := strings.NewReader(input)

		result, err := Parse(reader, JSONL)

		require.NoError(t, err)
		assert.Equal(t, 2, len(result.Records))
	})

	t.Run("column type is TEXT", func(t *testing.T) {
		t.Parallel()

		input := `{"name":"Alice"}`
		reader := strings.NewReader(input)

		result, err := Parse(reader, JSONL)

		require.NoError(t, err)
		assert.Equal(t, []ColumnType{TypeText}, result.ColumnTypes)
	})

	t.Run("preserves nested structure", func(t *testing.T) {
		t.Parallel()

		input := `{"id":1,"address":{"city":"Tokyo"},"tags":["dev","go"]}`
		reader := strings.NewReader(input)

		result, err := Parse(reader, JSONL)

		require.NoError(t, err)
		assert.Equal(t, 1, len(result.Records))

		var parsed map[string]any
		err = json.Unmarshal([]byte(result.Records[0][0]), &parsed)
		require.NoError(t, err)
		assert.Equal(t, float64(1), parsed["id"])
	})

	t.Run("returns error for empty input", func(t *testing.T) {
		t.Parallel()

		reader := strings.NewReader("")

		_, err := Parse(reader, JSONL)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "empty JSONL data")
	})

	t.Run("returns error for invalid JSON line", func(t *testing.T) {
		t.Parallel()

		input := `{"name":"Alice"}
not valid json
{"name":"Bob"}`
		reader := strings.NewReader(input)

		_, err := Parse(reader, JSONL)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid JSON on line")
	})

	t.Run("returns error for nil reader", func(t *testing.T) {
		t.Parallel()

		_, err := Parse(nil, JSONL)

		assert.Error(t, err)
	})
}

func TestParse_JSON_FromTestdata(t *testing.T) {
	t.Parallel()

	testdataDir := "testdata"
	if _, err := os.Stat(testdataDir); os.IsNotExist(err) {
		t.Skip("testdata directory not found")
	}

	t.Run("parses sample.json", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(filepath.Join(testdataDir, "sample.json"))
		require.NoError(t, err)
		defer f.Close()

		result, err := Parse(f, JSON)

		require.NoError(t, err)
		assert.Equal(t, []string{"data"}, result.Headers)
		assert.Equal(t, 3, len(result.Records))
		for _, rec := range result.Records {
			assert.True(t, json.Valid([]byte(rec[0])))
		}
	})

	t.Run("parses nested.json", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(filepath.Join(testdataDir, "nested.json"))
		require.NoError(t, err)
		defer f.Close()

		result, err := Parse(f, JSON)

		require.NoError(t, err)
		assert.Equal(t, []string{"data"}, result.Headers)
		assert.Equal(t, 2, len(result.Records))

		// Verify nested structure is preserved
		var parsed map[string]any
		err = json.Unmarshal([]byte(result.Records[0][0]), &parsed)
		require.NoError(t, err)

		address, ok := parsed["address"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "Tokyo", address["city"])

		tags, ok := parsed["tags"].([]any)
		require.True(t, ok)
		assert.Equal(t, 2, len(tags))
	})
}

func TestParse_JSONL_FromTestdata(t *testing.T) {
	t.Parallel()

	testdataDir := "testdata"
	if _, err := os.Stat(testdataDir); os.IsNotExist(err) {
		t.Skip("testdata directory not found")
	}

	t.Run("parses sample.jsonl", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(filepath.Join(testdataDir, "sample.jsonl"))
		require.NoError(t, err)
		defer f.Close()

		result, err := Parse(f, JSONL)

		require.NoError(t, err)
		assert.Equal(t, []string{"data"}, result.Headers)
		assert.Equal(t, 3, len(result.Records))
		for _, rec := range result.Records {
			assert.True(t, json.Valid([]byte(rec[0])))
		}
	})
}

func TestParse_JSON_Compressed(t *testing.T) {
	t.Parallel()

	testdataDir := "testdata"
	if _, err := os.Stat(testdataDir); os.IsNotExist(err) {
		t.Skip("testdata directory not found")
	}

	testCases := []struct {
		file     string
		fileType FileType
	}{
		{"sample.json.gz", JSONGZ},
		{"sample.json.bz2", JSONBZ2},
		{"sample.json.xz", JSONXZ},
		{"sample.json.zst", JSONZSTD},
		{"sample.json.z", JSONZLIB},
		{"sample.json.snappy", JSONSNAPPY},
		{"sample.json.s2", JSONS2},
		{"sample.json.lz4", JSONLZ4},
	}

	for _, tc := range testCases {
		t.Run(tc.file, func(t *testing.T) {
			t.Parallel()

			f, err := os.Open(filepath.Join(testdataDir, tc.file))
			require.NoError(t, err)
			defer f.Close()

			result, err := Parse(f, tc.fileType)

			require.NoError(t, err)
			assert.Equal(t, []string{"data"}, result.Headers)
			assert.Equal(t, 3, len(result.Records))
			for _, rec := range result.Records {
				assert.True(t, json.Valid([]byte(rec[0])))
			}
		})
	}
}

func TestParse_JSONL_Compressed(t *testing.T) {
	t.Parallel()

	testdataDir := "testdata"
	if _, err := os.Stat(testdataDir); os.IsNotExist(err) {
		t.Skip("testdata directory not found")
	}

	testCases := []struct {
		file     string
		fileType FileType
	}{
		{"sample.jsonl.gz", JSONLGZ},
		{"sample.jsonl.bz2", JSONLBZ2},
		{"sample.jsonl.xz", JSONLXZ},
		{"sample.jsonl.zst", JSONLZSTD},
		{"sample.jsonl.z", JSONLZLIB},
		{"sample.jsonl.snappy", JSONLSNAPPY},
		{"sample.jsonl.s2", JSONLS2},
		{"sample.jsonl.lz4", JSONLLZ4},
	}

	for _, tc := range testCases {
		t.Run(tc.file, func(t *testing.T) {
			t.Parallel()

			f, err := os.Open(filepath.Join(testdataDir, tc.file))
			require.NoError(t, err)
			defer f.Close()

			result, err := Parse(f, tc.fileType)

			require.NoError(t, err)
			assert.Equal(t, []string{"data"}, result.Headers)
			assert.Equal(t, 3, len(result.Records))
			for _, rec := range result.Records {
				assert.True(t, json.Valid([]byte(rec[0])))
			}
		})
	}
}

func TestParse_JSONL_LargeLines(t *testing.T) {
	t.Parallel()

	t.Run("handles line larger than 1MB", func(t *testing.T) {
		t.Parallel()

		// Build a JSON object with a value larger than 1MB
		// to confirm there is no arbitrary line-size limit.
		bigValue := strings.Repeat("x", 2*1024*1024) // 2 MB
		line := `{"big":"` + bigValue + `"}`
		input := line + "\n" + `{"small":"ok"}`
		reader := strings.NewReader(input)

		result, err := Parse(reader, JSONL)

		require.NoError(t, err)
		assert.Equal(t, 2, len(result.Records))
		assert.True(t, json.Valid([]byte(result.Records[0][0])))
		assert.True(t, json.Valid([]byte(result.Records[1][0])))
	})
}
