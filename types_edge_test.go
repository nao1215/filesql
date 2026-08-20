package filesql

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/nao1215/filesql/frame"
	"github.com/nao1215/filesql/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewChunkSize_BelowTheMinimum checks the floor on a chunk size. A chunk of
// zero or fewer rows would read a file forever, so anything under the minimum
// falls back to the default.
func TestNewChunkSize_BelowTheMinimum(t *testing.T) {
	t.Parallel()

	assert.Equal(t, chunkSizeValue(DefaultChunkSize), newChunkSize(0))
	assert.Equal(t, chunkSizeValue(DefaultChunkSize), newChunkSize(-1))
	assert.Equal(t, chunkSizeValue(1), newChunkSize(1))
}

// TestNewColumnInfoList_NoColumns covers a header with nothing in it, which is
// what an input with no columns produces.
func TestNewColumnInfoList_NoColumns(t *testing.T) {
	t.Parallel()

	assert.Nil(t, newColumnInfoList(newHeader(nil), nil))
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

// TestColumnTypeEvidence_ChoosesTheTypeThatHoldsEveryValue covers the rule that
// turns what a column was seen to hold into the type it is declared as. The
// answer follows from which kinds of value were present, not from how many of
// each, so a column cannot be typed against the values it holds least often.
func TestColumnTypeEvidence_ChoosesTheTypeThatHoldsEveryValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		values []string
		want   columnType
	}{
		{
			name:   "nothing to judge by",
			values: nil,
			want:   columnTypeText,
		},
		{
			name:   "empty cells only",
			values: []string{"", "  ", ""},
			want:   columnTypeText,
		},
		{
			name:   "integers alone",
			values: []string{"1", "2", "3"},
			want:   columnTypeInteger,
		},
		{
			name:   "one decimal among integers",
			values: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11.5"},
			want:   columnTypeReal,
		},
		{
			name:   "one text value among integers",
			values: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "abc"},
			want:   columnTypeText,
		},
		{
			name:   "datetimes alone",
			values: []string{"2026-08-20", "2026-08-21"},
			want:   columnTypeDatetime,
		},
		{
			// A datetime is stored as text, so a column that also holds a number
			// has no type covering both.
			name:   "a datetime beside a number",
			values: []string{"2026-08-20", "5"},
			want:   columnTypeText,
		},
		{
			// Empty cells say nothing, so they cannot outvote the values present.
			name:   "one integer among empty cells",
			values: []string{"", "", "", "7"},
			want:   columnTypeInteger,
		},
		{
			name:   "a zero padded code among integers",
			values: []string{"1", "2", "007"},
			want:   columnTypeText,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, inferColumnType(tt.values))
		})
	}
}

// TestColumnTypeEvidence_DoesNotDependOnOrder pins the property the streaming
// loader relies on: evidence folded in any order gives the same type, so a chunk
// boundary cannot change what a column is declared as.
func TestColumnTypeEvidence_DoesNotDependOnOrder(t *testing.T) {
	t.Parallel()

	values := []string{"1", "2", "3.5", "2026-08-20", "abc", "", "007"}
	want := inferColumnType(values)

	for i := range values {
		rotated := append(append([]string{}, values[i:]...), values[:i]...)
		assert.Equal(t, want, inferColumnType(rotated), "rotated by %d", i)
	}
}

// TestIsIntegerLiteralOverflowingInt64_SignOnly covers a value that is a sign
// and nothing else, which is not a number at all.
func TestIsIntegerLiteralOverflowingInt64_SignOnly(t *testing.T) {
	t.Parallel()

	assert.False(t, isIntegerLiteralOverflowingInt64("+"))
	assert.False(t, isIntegerLiteralOverflowingInt64("-"))
}

// loadColumnForTypeTest loads a one-column CSV whose header is "v" and returns
// the declared column type together with every value, rendered with the Go type
// it scanned as. The chunk size is the caller's, so the same body can be read
// as one chunk or as many.
func loadColumnForTypeTest(t *testing.T, values []string, chunkSize int) (string, []string) {
	t.Helper()

	body := "v\n" + strings.Join(values, "\n") + "\n"
	ctx := context.Background()
	validated, err := NewBuilder().
		AddReader(strings.NewReader(body), "t", FileTypeCSV).
		SetDefaultChunkSize(chunkSize).
		Build(ctx)
	require.NoError(t, err)
	db, err := validated.Open(ctx)
	require.NoError(t, err)
	defer db.Close()

	var declared string
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT type FROM pragma_table_info('t') WHERE name = 'v'`).Scan(&declared))

	rows, err := db.QueryContext(ctx, `SELECT v FROM t`)
	require.NoError(t, err)
	defer rows.Close()

	var got []string
	for rows.Next() {
		var v any
		require.NoError(t, rows.Scan(&v))
		got = append(got, fmt.Sprintf("%T(%v)", v, v))
	}
	require.NoError(t, rows.Err())
	sort.Strings(got)

	return declared, got
}

// TestColumnType_IsTheSameWhereverTheAwkwardValueSits pins the type of a column
// to the values it holds and not to the row one of them happens to sit on. A
// value that arrives after the first chunk used to meet a type already decided
// without it, so the same multiset of values loaded as two different columns.
func TestColumnType_IsTheSameWhereverTheAwkwardValueSits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		awkward  string
		wantType string
	}{
		{name: "text among integers", awkward: "abc", wantType: "TEXT"},
		{name: "a decimal among integers", awkward: "3.5", wantType: "REAL"},
		{name: "an integral decimal among integers", awkward: "4.0", wantType: "REAL"},
		{name: "a datetime among integers", awkward: "2026-08-20T10:00:00Z", wantType: "TEXT"},
		{name: "a zero padded code among integers", awkward: "007", wantType: "TEXT"},
		{name: "an int64 overflow among integers", awkward: "11040320260000000000", wantType: "TEXT"},
		{name: "a literal SQLite will not convert", awkward: "1_000", wantType: "TEXT"},
		{name: "a padded number among integers", awkward: " 5 ", wantType: "TEXT"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			const integers = 8
			positions := []int{0, integers / 2, integers}
			for _, chunkSize := range []int{1, 2, 4, DefaultChunkSize} {
				var wantValues []string
				for _, at := range positions {
					// The awkward value is inserted rather than substituted, so
					// every position loads the same multiset of values and only
					// their order differs.
					values := make([]string, 0, integers+1)
					for i := range integers {
						if i == at {
							values = append(values, tt.awkward)
						}
						values = append(values, strconv.Itoa(i+1))
					}
					if at == integers {
						values = append(values, tt.awkward)
					}

					declared, got := loadColumnForTypeTest(t, values, chunkSize)
					assert.Equal(t, tt.wantType, declared,
						"%q at row %d, chunk size %d", tt.awkward, at+1, chunkSize)
					if wantValues == nil {
						wantValues = got
						continue
					}
					assert.Equal(t, wantValues, got,
						"%q at row %d, chunk size %d loaded different values", tt.awkward, at+1, chunkSize)
				}
			}
		})
	}
}

// TestColumnType_TextAfterTheDefaultChunkBoundary covers the same defect on the
// path a caller reaches without configuring anything: the default chunk is 1000
// rows, so a text value on row 1001 used to leave the column INTEGER, and the
// answers to ORDER BY and to a comparison changed with it.
func TestColumnType_TextAfterTheDefaultChunkBoundary(t *testing.T) {
	t.Parallel()

	const rows = 1000
	early := make([]string, 0, rows+1)
	late := make([]string, 0, rows+1)
	early = append(early, "1", "abc")
	for i := 2; i <= rows; i++ {
		early = append(early, strconv.Itoa(i))
	}
	for i := 1; i <= rows; i++ {
		late = append(late, strconv.Itoa(i))
	}
	late = append(late, "abc")

	earlyType, earlyValues := loadColumnForTypeTest(t, early, DefaultChunkSize)
	lateType, lateValues := loadColumnForTypeTest(t, late, DefaultChunkSize)

	assert.Equal(t, "TEXT", earlyType)
	assert.Equal(t, "TEXT", lateType, "a text value on row 1001 has to reach the column type")
	assert.Equal(t, earlyValues, lateValues, "the same values loaded as different Go types")
}

// TestColumnType_ADecimalMakesTheColumnReal pins a numeric column holding any
// decimal to REAL. Deciding it by how many decimals the file happens to hold
// left an INTEGER column that either rewrote them or stored them against its own
// declared type, and adding one more decimal row changed the arithmetic of rows
// nobody touched.
func TestColumnType_ADecimalMakesTheColumnReal(t *testing.T) {
	t.Parallel()

	integers := []string{"5", "7", "9", "11", "13", "15", "17", "19", "21", "23"}

	tests := []struct {
		name     string
		values   []string
		wantType string
	}{
		{name: "integers alone", values: integers, wantType: "INTEGER"},
		{name: "one integral decimal", values: append(append([]string{}, integers...), "4.0"), wantType: "REAL"},
		{name: "two integral decimals", values: append(append([]string{}, integers...), "4.0", "6.0"), wantType: "REAL"},
		{name: "one fractional decimal", values: append(append([]string{}, integers...), "2.5"), wantType: "REAL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			declared, _ := loadColumnForTypeTest(t, tt.values, DefaultChunkSize)
			assert.Equal(t, tt.wantType, declared)
		})
	}

	t.Run("the arithmetic of a decimal column is not integer division", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		body := "v\n" + strings.Join(append(append([]string{}, integers...), "4.0"), "\n") + "\n"
		validated, err := NewBuilder().
			AddReader(strings.NewReader(body), "t", FileTypeCSV).
			Build(ctx)
		require.NoError(t, err)
		db, err := validated.Open(ctx)
		require.NoError(t, err)
		defer db.Close()

		var half float64
		require.NoError(t, db.QueryRowContext(ctx, `SELECT v / 2 FROM t WHERE rowid = 1`).Scan(&half))
		assert.InDelta(t, 2.5, half, 0.0001, "5 / 2 in a column that holds decimals")
	})
}

// TestColumnType_DeclaredTypeAgreesWithStoredType pins the invariant behind both
// of the above: SQLite stores a value the declared type cannot hold under its
// own storage class, so a schema that disagrees with typeof() is a column whose
// type was inferred from less than the whole column.
func TestColumnType_DeclaredTypeAgreesWithStoredType(t *testing.T) {
	t.Parallel()

	bodies := map[string]string{
		"decimals among integers": "v\n10\n20\n30\n40\n50\n60\n70\n80\n90\n100\n2.5\n",
		"text among integers":     "v\n10\n20\n30\n40\n50\nabc\n",
		"integers among decimals": "v\n1.5\n2.5\n3\n4\n5\n",
	}
	storageOf := map[string]string{"INTEGER": "integer", "REAL": "real", "TEXT": "text"}

	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			validated, err := NewBuilder().
				AddReader(strings.NewReader(body), "t", FileTypeCSV).
				Build(ctx)
			require.NoError(t, err)
			db, err := validated.Open(ctx)
			require.NoError(t, err)
			defer db.Close()

			var declared string
			require.NoError(t, db.QueryRowContext(ctx,
				`SELECT type FROM pragma_table_info('t') WHERE name = 'v'`).Scan(&declared))
			storage, ok := storageOf[declared]
			require.True(t, ok, "unexpected declared type %q", declared)

			var disagreeing int
			require.NoError(t, db.QueryRowContext(ctx,
				`SELECT count(*) FROM t WHERE v IS NOT NULL AND typeof(v) != ?`, storage).Scan(&disagreeing))
			assert.Zero(t, disagreeing, "column declared %s holds values SQLite stored otherwise", declared)
		})
	}
}

// TestColumnType_FrameAndTheLoaderAgree pins the two inferences to each other.
// The README says frame applies the same rules to its own values, and it holds
// only while both answer the same question the same way — they are separate
// implementations, so the agreement has to be tested rather than assumed.
func TestColumnType_FrameAndTheLoaderAgree(t *testing.T) {
	t.Parallel()

	bodies := map[string]string{
		"integers":                  "v\n1\n2\n3\n",
		"decimals":                  "v\n1.5\n2.5\n3\n",
		"one decimal among many":    "v\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n11.5\n",
		"one text among many":       "v\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\nabc\n",
		"a zero padded code":        "v\n1\n2\n007\n",
		"an int64 overflow":         "v\n1\n2\n11040320260000000000\n",
		"a literal only Go parses":  "v\n1\n2\n1_000\n",
		"a padded number":           "v\n1\n2\n  42\n",
		"a datetime among integers": "v\n1\n2\n3\n4\n5\n6\n7\n8\n9\n2026-08-20\n",
		"datetimes only":            "v\n2026-08-20\n2026-08-21\n",
		"empty cells":               "v\n\n\n5\n",
	}

	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			validated, err := NewBuilder().
				AddReader(strings.NewReader(body), "t", FileTypeCSV).
				Build(ctx)
			require.NoError(t, err)
			db, err := validated.Open(ctx)
			require.NoError(t, err)
			defer db.Close()

			rows, err := db.QueryContext(ctx, `SELECT v FROM t ORDER BY rowid`)
			require.NoError(t, err)
			defer rows.Close()
			var loaded []string
			for rows.Next() {
				var v any
				require.NoError(t, rows.Scan(&v))
				loaded = append(loaded, fmt.Sprintf("%T", v))
			}
			require.NoError(t, rows.Err())

			df, err := frame.NewDataFrame(strings.NewReader(body), parser.CSV)
			require.NoError(t, err)
			var framed []string
			for _, row := range df.ToRecords() {
				framed = append(framed, fmt.Sprintf("%T", row["v"]))
			}

			assert.Equal(t, loaded, framed, "the loader and frame typed the same column differently")
		})
	}
}
