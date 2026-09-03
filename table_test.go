package filesql

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewTable(t *testing.T) {
	t.Parallel()

	t.Run("Create table with header and records", func(t *testing.T) {
		t.Parallel()

		header := newHeader([]string{"col1", "col2"})
		records := []record{
			newRecord([]string{"val1", "val2"}),
			newRecord([]string{"val3", "val4"}),
		}

		table := newTable("test", header, records)

		assert.Equal(t, "test", table.getName(), "Table name mismatch")

		assert.True(t, table.getHeader().equal(header), "Header mismatch")

		assert.Len(t, table.getRecords(), 2, "Record count mismatch")

		assert.True(t, table.getRecords()[0].equal(records[0]), "First record mismatch")
	})
}

func TestTable_Equal(t *testing.T) {
	t.Parallel()

	header := newHeader([]string{"col1", "col2"})
	records := []record{
		newRecord([]string{"val1", "val2"}),
		newRecord([]string{"val3", "val4"}),
	}

	table1 := newTable("test", header, records)
	table2 := newTable("test", header, records)
	table3 := newTable("different", header, records)

	t.Run("Equal tables", func(t *testing.T) {
		t.Parallel()

		assert.True(t, table1.equal(table2), "Tables should be equal")
	})

	t.Run("Different names", func(t *testing.T) {
		t.Parallel()

		assert.False(t, table1.equal(table3), "Tables with different names should not be equal")
	})

	t.Run("Different header", func(t *testing.T) {
		t.Parallel()

		differentHeader := newHeader([]string{"col1", "col3"})
		table4 := newTable("test", differentHeader, records)
		assert.False(t, table1.equal(table4), "Tables with different headers should not be equal")
	})

	t.Run("Different record count", func(t *testing.T) {
		t.Parallel()

		differentRecords := []record{
			newRecord([]string{"val1", "val2"}),
		}
		table5 := newTable("test", header, differentRecords)
		assert.False(t, table1.equal(table5), "Tables with different record count should not be equal")
	})

	t.Run("Different record values", func(t *testing.T) {
		t.Parallel()

		differentValueRecords := []record{
			newRecord([]string{"val1", "val2"}),
			newRecord([]string{"val3", "different"}),
		}
		table6 := newTable("test", header, differentValueRecords)
		assert.False(t, table1.equal(table6), "Tables with different record values should not be equal")
	})
}

func TestTableFromFilePath_Additional(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		filePath string
		expected string
	}{
		{
			name:     "Simple file with extension",
			filePath: "data.csv",
			expected: "data",
		},
		{
			name:     "File with path",
			filePath: filepath.Join("home", "user", "documents", "data.csv"),
			expected: "data",
		},
		{
			name:     "File with multiple dots",
			filePath: "data.backup.csv",
			expected: "data.backup",
		},
		{
			name:     "File without extension",
			filePath: "data",
			expected: "data",
		},
		{
			name:     "File with path and no extension",
			filePath: filepath.Join("home", "user", "data"),
			expected: "data",
		},
		{
			name:     "Hidden file",
			filePath: ".hidden",
			expected: "",
		},
		{
			name:     "Hidden file with extension",
			filePath: ".gitignore",
			expected: "",
		},
		{
			name:     "Compressed file (gz)",
			filePath: "data.csv.gz",
			expected: "data",
		},
		{
			name:     "Compressed file (bz2)",
			filePath: "data.csv.bz2",
			expected: "data",
		},
		{
			name:     "Compressed file (xz)",
			filePath: "data.csv.xz",
			expected: "data",
		},
		{
			name:     "Compressed file (zst)",
			filePath: "data.csv.zst",
			expected: "data",
		},
		{
			name:     "Compressed file (zlib)",
			filePath: "data.csv.z",
			expected: "data",
		},
		{
			name:     "Compressed file (snappy)",
			filePath: "data.csv.snappy",
			expected: "data",
		},
		{
			name:     "Compressed file (s2)",
			filePath: "data.csv.s2",
			expected: "data",
		},
		{
			name:     "Compressed file (lz4)",
			filePath: "data.csv.lz4",
			expected: "data",
		},
		// Case-insensitive tests
		{
			name:     "Compressed file (GZ uppercase)",
			filePath: "data.csv.GZ",
			expected: "data",
		},
		{
			name:     "Compressed file (BZ2 uppercase)",
			filePath: "data.csv.BZ2",
			expected: "data",
		},
		{
			name:     "Compressed file (XZ uppercase)",
			filePath: "data.csv.XZ",
			expected: "data",
		},
		{
			name:     "Compressed file (ZST uppercase)",
			filePath: "data.csv.ZST",
			expected: "data",
		},
		{
			name:     "Compressed file (Z uppercase - zlib)",
			filePath: "data.csv.Z",
			expected: "data",
		},
		{
			name:     "Compressed file (SNAPPY uppercase)",
			filePath: "data.csv.SNAPPY",
			expected: "data",
		},
		{
			name:     "Compressed file (S2 uppercase)",
			filePath: "data.csv.S2",
			expected: "data",
		},
		{
			name:     "Compressed file (LZ4 uppercase)",
			filePath: "data.csv.LZ4",
			expected: "data",
		},
		{
			name:     "Compressed file (mixed case Gz)",
			filePath: "data.csv.Gz",
			expected: "data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := tableFromFilePath(tt.filePath)
			assert.Equal(t, tt.expected, result, "tableFromFilePath failed for %s", tt.filePath)
		})
	}
}

func TestSanitizeTableName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"plain name", "users", "users"},
		{"hyphen becomes underscore", "user-data", "user_data"},
		{"space becomes underscore", "user data", "user_data"},
		{"dot becomes underscore", "data.backup", "data_backup"},
		{"punctuation is dropped", "user@#$data", "userdata"},
		{"leading digit is prefixed", "123table", "sheet_123table"},
		{"only punctuation falls back", "@#$%", "sheet"},
		{"empty falls back", "", "sheet"},
		{"uppercase is preserved", "UserData", "UserData"},
		// A table name is always emitted double-quoted, so any letter SQLite
		// accepts inside quotes survives. Dropping non-ASCII letters used to
		// erase a Japanese, Chinese, Korean, Cyrillic, or accented-Latin file
		// name down to the fallback, making two such files collide.
		{"japanese is preserved", "売上", "売上"},
		{"japanese kana is preserved", "テーブル", "テーブル"},
		{"cyrillic is preserved", "Данные", "Данные"},
		{"accented latin is preserved", "café", "café"},
		{"decomposed accent is preserved", "café", "café"},
		{"mixed script keeps both", "売上-2026", "売上_2026"},
		{"non-ascii digit leads", "١٢", "sheet_١٢"},
		{"emoji is dropped", "data\U0001F600", "data"},
		{"quote is dropped", `a"b`, "ab"},
		// A separator in a name is a separator whichever space writes it. The
		// no-break space is what a name copied out of a web page or a
		// spreadsheet cell carries, and U+3000 is ordinary in a Japanese file
		// name; dropping either joined two words into one and could collide
		// with a different file.
		{"tab becomes underscore", "user\tdata", "user_data"},
		{"vertical tab becomes underscore", "user\vdata", "user_data"},
		{"form feed becomes underscore", "user\fdata", "user_data"},
		{"carriage return becomes underscore", "user\rdata", "user_data"},
		{"newline becomes underscore", "user\ndata", "user_data"},
		{"no-break space becomes underscore", "user\u00a0data", "user_data"},
		{"next line becomes underscore", "user\u0085data", "user_data"},
		{"em space becomes underscore", "user\u2003data", "user_data"},
		{"ideographic space becomes underscore", "user\u3000data", "user_data"},
		{"a run of mixed spaces is one underscore each", "a \tb", "a__b"},
		// A dash that is not the ASCII hyphen is not a separator this rule
		// names, and is dropped like any other punctuation.
		{"en dash is dropped", "user\u2013data", "userdata"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, sanitizeTableName(tt.input), "sanitizeTableName(%q)", tt.input)
		})
	}
}

func TestXLSXSheetTableName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		base     string
		sheet    string
		expected string
	}{
		{"a differing sheet name is appended", "sample", "Sheet1", "sample_Sheet1"},
		{"a second sheet keeps its own table", "sample", "Sheet2", "sample_Sheet2"},
		{"a sheet named after the file is not repeated", "people", "people", "people"},
		{"the comparison is made after sanitizing", "my_data", "my-data", "my_data"},
		{"a sheet name that only differs in case is still appended", "people", "People", "people_People"},
		{"an unusable sheet name falls back and is appended", "sample", "!!!", "sample_" + sheetFallbackName},
		// Excel caps a sheet name at 31 characters and forbids seven of them,
		// so a dump of a table named beyond either writes a sheet spelled the
		// way Excel allows. That sheet is still named after the file.
		{
			name:     "a sheet named after a file too long for Excel is not repeated",
			base:     "quarterly_revenue_by_region_2026",
			sheet:    "quarterly_revenue_by_region_202",
			expected: "quarterly_revenue_by_region_2026",
		},
		{
			name:     "a shorter sheet name that is not the file's is still appended",
			base:     "quarterly_revenue_by_region_2026",
			sheet:    "summary",
			expected: "quarterly_revenue_by_region_2026_summary",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, xlsxSheetTableName(tt.base, tt.sheet))
		})
	}
}

// TestTableNameFollowsTheDocumentedRule holds the table a file becomes to the
// rule doc.go states, one case per class of character the rule distinguishes.
// Open's own godoc said the characters an identifier cannot hold are replaced
// by underscores, and they are dropped: a caller who believed it wrote FROM a_b
// for a file named a!b.csv and met "no such table".
func TestTableNameFollowsTheDocumentedRule(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		file string
		want string
	}{
		{"users.csv", "users"},
		{"user-data.csv", "user_data"},
		{"my file.csv", "my_file"},
		{"a.b.csv", "a_b"},
		{"売上 2026.csv", "売上_2026"},
		// Every other character an identifier cannot hold is dropped.
		{"a!b.csv", "ab"},
		{"a@b.csv", "ab"},
		{"a(b).csv", "ab"},
		{"a+b.csv", "ab"},
		{"a%b.csv", "ab"},
		// The two names that cannot be written as an identifier at all.
		{"2024.csv", "sheet_2024"},
		{"!!.csv", "sheet"},
	} {
		t.Run(tt.file, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			path := filepath.Join(dir, tt.file)
			if err := os.WriteFile(path, []byte("v\n1\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			db, err := Open(t.Context(), path)
			if err != nil {
				t.Fatalf("load %s: %v", tt.file, err)
			}
			defer func() { _ = db.Close() }()

			var name string
			if err := db.QueryRowContext(t.Context(),
				`SELECT name FROM sqlite_master WHERE type='table' LIMIT 1`).Scan(&name); err != nil {
				t.Fatal(err)
			}
			if name != tt.want {
				t.Errorf("%s loads as table %q, want %q", tt.file, name, tt.want)
			}
		})
	}
}
