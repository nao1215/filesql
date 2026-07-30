package filesql

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestExcelSheetName pins how a table name is adapted to Excel's worksheet-name
// rules. A table name comes from a file name, so one that is long or punctuated
// is ordinary input rather than a mistake: the dump used to hand it to Excel
// as-is and fail, so a table named after monthly_sales_report_2026_q3_final.csv
// could not be exported to XLSX at all.
func TestExcelSheetName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		table string
		want  string
	}{
		{name: "a short name is unchanged", table: "sales", want: "sales"},
		{name: "exactly the limit is unchanged", table: strings.Repeat("a", 31), want: strings.Repeat("a", 31)},
		{name: "one over the limit is cut to it", table: strings.Repeat("a", 32), want: strings.Repeat("a", 31)},
		{name: "the limit counts runes, not bytes", table: strings.Repeat("売", 32), want: strings.Repeat("売", 31)},
		{name: "a bracket becomes an underscore", table: "sales[2026]", want: "sales_2026_"},
		{name: "a question mark becomes an underscore", table: "what?", want: "what_"},
		{name: "a star becomes an underscore", table: "a*b", want: "a_b"},
		{name: "a colon becomes an underscore", table: "12:34", want: "12_34"},
		{name: "a slash becomes an underscore", table: "a/b", want: "a_b"},
		{name: "a backslash becomes an underscore", table: `a\b`, want: "a_b"},
		{name: "surrounding apostrophes are dropped", table: "'sales'", want: "sales"},
		{name: "a name that leaves nothing usable falls back", table: "'", want: defaultSheetName},
		{name: "an empty name falls back", table: "", want: defaultSheetName},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, excelSheetName(tt.table))
		})
	}
}

// TestDumpXLSXAdaptsSheetName pins that a dump of such a table succeeds and can
// be read back.
func TestDumpXLSXAdaptsSheetName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		table     string
		wantSheet string
	}{
		{name: "a long name", table: "monthly_sales_report_2026_q3_final", wantSheet: "monthly_sales_report_2026_q3_fi"},
		{name: "a punctuated name", table: "sales[2026]", wantSheet: "sales_2026_"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := openWithTable(t,
				`CREATE TABLE "`+tt.table+`" (x TEXT)`,
				`INSERT INTO "`+tt.table+`" VALUES ('kept')`)

			outDir := t.TempDir()
			require.NoError(t, DumpDatabase(db, outDir, NewDumpOptions().WithFormat(OutputFormatXLSX)))

			reloaded, err := OpenContext(t.Context(), filepath.Join(outDir, tt.table+".xlsx"))
			require.NoError(t, err)
			defer reloaded.Close()

			rows, err := reloaded.QueryContext(t.Context(), "SELECT name FROM sqlite_master WHERE type='table'")
			require.NoError(t, err)
			defer rows.Close()
			names := make([]string, 0, 1)
			for rows.Next() {
				var n string
				require.NoError(t, rows.Scan(&n))
				names = append(names, n)
			}
			require.NoError(t, rows.Err())

			// The sheet name is what a reader turns into a table name, and Excel
			// cannot hold the original, so the reloaded name is the adapted one.
			require.Len(t, names, 1)
			assert.Contains(t, names[0], sanitizeTableName(tt.wantSheet))
		})
	}
}
