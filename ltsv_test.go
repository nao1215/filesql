package filesql

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLTSVColumnOrderIsTheOrderTheLabelsAppear pins that an LTSV file's columns
// come out in the order its labels are written.
//
// They were collected into a map and then read back out of it, and Go randomizes
// map iteration, so the column order was drawn afresh on every load: the same
// file gave "id,name" one run and "name,id" the next. SELECT * answered in a
// different order each time, and a dump of an LTSV table wrote its columns in a
// different order each time, which made the file's own round-trip unstable.
//
// The load is repeated because one pass cannot tell a stable order from a lucky
// draw.
func TestLTSVColumnOrderIsTheOrderTheLabelsAppear(t *testing.T) {
	t.Parallel()

	const attempts = 20

	tests := []struct {
		name    string
		content string
		want    []string
	}{
		{
			name:    "one record",
			content: "id:1\tname:alice\tage:20\n",
			want:    []string{"id", "name", "age"},
		},
		{
			name:    "a label that only appears in a later record goes last",
			content: "id:1\tname:alice\nid:2\tname:bob\tnickname:bo\n",
			want:    []string{"id", "name", "nickname"},
		},
		{
			name:    "a record that lists its labels in another order does not reorder the columns",
			content: "id:1\tname:alice\nname:bob\tid:2\n",
			want:    []string{"id", "name"},
		},
		{
			name:    "labels that sort differently than they appear",
			content: "zebra:1\tapple:2\tmango:3\n",
			want:    []string{"zebra", "apple", "mango"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			for range attempts {
				ctx := t.Context()
				src := filepath.Join(t.TempDir(), "logs.ltsv")
				require.NoError(t, os.WriteFile(src, []byte(tt.content), 0o600))

				db, err := OpenContext(ctx, src)
				require.NoError(t, err)

				rows, err := db.QueryContext(ctx, "SELECT * FROM logs")
				require.NoError(t, err)
				got, err := rows.Columns()
				require.NoError(t, err)
				require.NoError(t, rows.Err())
				require.NoError(t, rows.Close())
				require.NoError(t, db.Close())

				require.Equal(t, tt.want, got)
			}
		})
	}
}

// TestLTSVColumnOrderSurvivesChunking pins the same order for a file large enough
// to be loaded in chunks, which is a second code path with its own copy of the
// header collection.
func TestLTSVColumnOrderSurvivesChunking(t *testing.T) {
	t.Parallel()

	const attempts = 10

	var sb strings.Builder
	for i := range 500 {
		sb.WriteString("id:")
		sb.WriteString(strings.Repeat("0", 1) + string(rune('0'+i%10)))
		sb.WriteString("\tname:alice\tage:20\n")
	}
	content := sb.String()

	for range attempts {
		ctx := t.Context()
		src := filepath.Join(t.TempDir(), "logs.ltsv")
		require.NoError(t, os.WriteFile(src, []byte(content), 0o600))

		validated, err := buildForTest(ctx, NewBuilder().AddPath(src).SetDefaultChunkSize(10))
		require.NoError(t, err)
		db, err := validated.Open(ctx)
		require.NoError(t, err)

		rows, err := db.QueryContext(ctx, "SELECT * FROM logs")
		require.NoError(t, err)
		got, err := rows.Columns()
		require.NoError(t, err)
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())

		var count int
		require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM logs").Scan(&count))
		require.NoError(t, db.Close())

		require.Equal(t, []string{"id", "name", "age"}, got)
		assert.Equal(t, 500, count)
	}
}

// TestLTSVCaseOnlyDuplicateLabelIsRefused pins the gap the header formats had
// closed. LTSV carries its labels on every record, so its duplicate check is its
// own, and that one compared labels exactly: "A:1\ta:2" passed it and failed at
// SQLite instead, which folds ASCII case — a raw CREATE TABLE error three wraps
// deep, with no ErrDuplicateColumn to match.
//
// Only ASCII case is folded, as SQLite folds only that, so "ä" and "Ä" stay two
// labels and a record using both still loads.
func TestLTSVCaseOnlyDuplicateLabelIsRefused(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		refused bool
	}{
		{name: "labels differing only in ASCII case", content: "A:1\ta:2\n", refused: true},
		{name: "labels repeated exactly", content: "a:1\ta:2\n", refused: true},
		{name: "labels differing by surrounding space", content: "a:1\t a:2\n", refused: true},
		{name: "labels differing beyond ASCII case", content: "ä:1\tÄ:2\n", refused: false},
		{name: "labels that are simply different", content: "a:1\tb:2\n", refused: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "dup.ltsv")
			if err := os.WriteFile(path, []byte(tt.content), 0o600); err != nil {
				t.Fatal(err)
			}

			db, err := OpenContext(context.Background(), path)
			if db != nil {
				defer func() { _ = db.Close() }()
			}

			if !tt.refused {
				if err != nil {
					t.Fatalf("OpenContext refused a record it should load: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("OpenContext accepted a record whose labels are one column")
			}
			if !errors.Is(err, ErrDuplicateColumn) {
				t.Errorf("error = %v, want it to match ErrDuplicateColumn", err)
			}
		})
	}
}

// TestLTSVCaseOnlyLabelAcrossRecordsIsOneColumn pins that two records writing
// the same label in different cases fill one column rather than two.
//
// They did not: the label was tracked as written, so "id" on one line and "ID"
// on the next were two columns, each row filling only its own — and the table
// SQLite was then asked to create was refused with "duplicate column name: ID",
// an error naming neither the file nor the rule, because SQLite compares column
// names the way this package already compares labels within one record.
func TestLTSVCaseOnlyLabelAcrossRecordsIsOneColumn(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "labels.ltsv")
	content := "id:1\tv:a\nID:2\tV:b\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	db, err := OpenContext(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenContext: %v", err)
	}
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(context.Background(), "SELECT id, v FROM labels ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var got [][2]string
	for rows.Next() {
		var id, v string
		if err := rows.Scan(&id, &v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, [2]string{id, v})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}

	want := [][2]string{{"1", "a"}, {"2", "b"}}
	if len(got) != len(want) {
		t.Fatalf("rows = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("row %d = %v, want %v", i, got[i], want[i])
		}
	}
}

// TestLTSVValueKeepsSurroundingWhitespace pins that an LTSV value is read as the
// bytes between the colon and the field's end.
//
// The reader trimmed the value, so a cell with meaningful spaces around it came
// back without them, while CSV and TSV kept theirs and the LTSV writer wrote
// them: a dump and reload through LTSV lost them, and the same data loaded from
// two formats disagreed. LTSV defines a value as everything up to the next tab or
// newline and says nothing about trimming.
//
// The label is still trimmed. LTSV restricts a label to letters, digits,
// underscore, dot, and hyphen, so a space around one is already malformed, and
// tolerating it costs nothing.
func TestLTSVValueKeepsSurroundingWhitespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		want    string
	}{
		{name: "a space after the colon belongs to the value", content: "v: padded\n", want: " padded"},
		{name: "a trailing space belongs to the value", content: "v:padded \n", want: "padded "},
		{name: "spaces on both sides belong to the value", content: "v:  padded  \n", want: "  padded  "},
		{name: "a value of only spaces is not an empty value", content: "v:   \n", want: "   "},
		{name: "a value with no whitespace is unchanged", content: "v:padded\n", want: "padded"},
		{name: "an inner space was never at risk", content: "v:two words\n", want: "two words"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			src := filepath.Join(t.TempDir(), "t.ltsv")
			require.NoError(t, os.WriteFile(src, []byte(tt.content), 0o600))

			db, err := OpenContext(ctx, src)
			require.NoError(t, err)
			defer db.Close()

			var got string
			require.NoError(t, db.QueryRowContext(ctx, "SELECT v FROM t").Scan(&got))
			assert.Equal(t, tt.want, got)
		})
	}

	t.Run("a label keeps being trimmed, since a space in one is malformed", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		src := filepath.Join(t.TempDir(), "t.ltsv")
		require.NoError(t, os.WriteFile(src, []byte(" v :x\n"), 0o600))

		db, err := OpenContext(ctx, src)
		require.NoError(t, err)
		defer db.Close()

		rows, err := db.QueryContext(ctx, "SELECT * FROM t")
		require.NoError(t, err)
		defer rows.Close()
		cols, err := rows.Columns()
		require.NoError(t, err)
		require.NoError(t, rows.Err())
		assert.Equal(t, []string{"v"}, cols)
	})

	t.Run("a dump and reload through LTSV keeps the whitespace", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t, "CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('  padded  ')")

		outDir := t.TempDir()
		require.NoError(t, DumpDatabase(db, outDir, NewDumpOptions().WithFormat(OutputFormatLTSV)))

		reloaded, err := OpenContext(t.Context(), filepath.Join(outDir, "t.ltsv"))
		require.NoError(t, err)
		defer reloaded.Close()

		var got string
		require.NoError(t, reloaded.QueryRowContext(t.Context(), "SELECT v FROM t").Scan(&got))
		assert.Equal(t, "  padded  ", got)
	})

	t.Run("LTSV and CSV agree on the same value", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		dir := t.TempDir()

		ltsvPath := filepath.Join(dir, "a.ltsv")
		require.NoError(t, os.WriteFile(ltsvPath, []byte("v:  padded  \n"), 0o600))
		csvPath := filepath.Join(dir, "b.csv")
		require.NoError(t, os.WriteFile(csvPath, []byte("v\n\"  padded  \"\n"), 0o600))

		db, err := OpenContext(ctx, ltsvPath, csvPath)
		require.NoError(t, err)
		defer db.Close()

		var fromLTSV, fromCSV string
		require.NoError(t, db.QueryRowContext(ctx, "SELECT v FROM a").Scan(&fromLTSV))
		require.NoError(t, db.QueryRowContext(ctx, "SELECT v FROM b").Scan(&fromCSV))
		assert.Equal(t, fromCSV, fromLTSV)
	})
}

// TestLTSVLabelIsTrimmed pins that a label is the name without the whitespace
// around it, which is what makes the writer refuse a column name carrying any:
// the file would name a column this reader does not return.
//
// LTSV restricts a label to letters, digits, underscore, dot and hyphen, so a
// space around one is malformed however it got there, and a file that has one
// is read rather than refused.
func TestLTSVLabelIsTrimmed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		want    string
	}{
		{name: "a leading space", content: " v:1\n", want: "v"},
		{name: "a trailing space", content: "v :1\n", want: "v"},
		{name: "both sides", content: "  v  :1\n", want: "v"},
		{name: "an ideographic space", content: "\u3000v:1\n", want: "v"},
		{name: "no whitespace at all", content: "v:1\n", want: "v"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			src := filepath.Join(t.TempDir(), "t.ltsv")
			require.NoError(t, os.WriteFile(src, []byte(tt.content), 0o600))

			db, err := OpenContext(ctx, src)
			require.NoError(t, err)
			defer db.Close()

			var got string
			require.NoError(t, db.QueryRowContext(ctx,
				`SELECT name FROM pragma_table_info('t')`).Scan(&got))
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestLTSVLabelRulesDifferFromAHeaderCells pins the two places where a label
// and a header cell holding the same characters name different columns, and
// pins them side by side, since either one read alone looks like a mistake.
//
// A label is trimmed as it is read, so the trimming rule has already run when
// the case fold runs and the two are not the separate rules a header gets: " A"
// beside "a" is two columns of a CSV header and one label typed twice. And a
// label that is empty names the empty string rather than taking the name of its
// position, because a position names a column of a header and an LTSV record
// does not have to carry the labels the record before it carried.
func TestLTSVLabelRulesDifferFromAHeaderCells(t *testing.T) {
	t.Parallel()

	columns := func(t *testing.T, name, content string) ([]string, error) {
		t.Helper()

		src := filepath.Join(t.TempDir(), name)
		require.NoError(t, os.WriteFile(src, []byte(content), 0o600))

		db, err := OpenContext(t.Context(), src)
		if err != nil {
			return nil, err
		}
		defer db.Close()
		rows, err := db.QueryContext(t.Context(), `SELECT * FROM t LIMIT 0`)
		require.NoError(t, err)
		defer rows.Close()
		names, err := rows.Columns()
		require.NoError(t, err)
		return names, rows.Err()
	}

	t.Run("trimming and folding are one rule for a label and two for a header", func(t *testing.T) {
		t.Parallel()

		header, err := columns(t, "t.csv", " A,a\n1,2\n")
		require.NoError(t, err, "a header keeps the two apart")
		assert.Equal(t, []string{" A", "a"}, header)

		_, err = columns(t, "t.ltsv", " A:1\ta:2\n")
		require.Error(t, err, "the label is trimmed first, so the fold makes them one")
		assert.ErrorIs(t, err, ErrDuplicateColumn)
	})

	t.Run("an empty label names the empty string and an empty header cell its position", func(t *testing.T) {
		t.Parallel()

		header, err := columns(t, "t.csv", ",b\n1,2\n")
		require.NoError(t, err)
		assert.Equal(t, []string{"column_1", "b"}, header)

		label, err := columns(t, "t.ltsv", ":1\tb:2\n")
		require.NoError(t, err)
		assert.Equal(t, []string{"", "b"}, label)
	})

	t.Run("a space inside a name is kept by both", func(t *testing.T) {
		t.Parallel()

		header, err := columns(t, "t.csv", "a b,c\n1,2\n")
		require.NoError(t, err)
		assert.Equal(t, []string{"a b", "c"}, header)

		label, err := columns(t, "t.ltsv", "a b:1\tc:2\n")
		require.NoError(t, err)
		assert.Equal(t, []string{"a b", "c"}, label)
	})
}

// TestLTSVFieldThatNamesNoLabelFollowsTheMalformedRowPolicy pins that a field
// with no label is handled by the policy the caller chose rather than dropped.
//
// It was dropped: a line holding no pair at all vanished from the table with no
// error, no count and no way to tell the result apart from a file that really
// held one row fewer, and the default policy is the one that exists to say a
// file looks misaligned. A field with no label inside a line that has pairs went
// the same silent way.
func TestLTSVFieldThatNamesNoLabelFollowsTheMalformedRowPolicy(t *testing.T) {
	t.Parallel()

	const oneLineIsGarbage = "a:1\tb:2\nGARBAGE\na:3\tb:4\n"
	const oneFieldIsGarbage = "a:1\tJUNK\tb:2\n"

	tests := []struct {
		name    string
		content string
		policy  MalformedRowPolicy
		rows    int
		skipped int
		wantErr []string
	}{
		{
			name:    "stop refuses a line that names no label",
			content: oneLineIsGarbage,
			policy:  MalformedRowStop,
			wantErr: []string{"row 2", "GARBAGE"},
		},
		{
			name:    "skip drops that line and counts it",
			content: oneLineIsGarbage,
			policy:  MalformedRowSkip,
			rows:    2,
			skipped: 1,
		},
		{
			name:    "fill refuses it, because filling would discard the field",
			content: oneLineIsGarbage,
			policy:  MalformedRowFill,
			wantErr: []string{"row 2", "GARBAGE"},
		},
		{
			name:    "stop refuses one unlabeled field among pairs",
			content: oneFieldIsGarbage,
			policy:  MalformedRowStop,
			wantErr: []string{"row 1", "JUNK"},
		},
		{
			name:    "skip drops the record that field belonged to",
			content: oneFieldIsGarbage,
			policy:  MalformedRowSkip,
			rows:    0,
			skipped: 1,
		},
		{
			name:    "the refusal quotes the first fields and counts the rest",
			content: "a:1\tb:2\nw\tx\ty\tz\n",
			policy:  MalformedRowStop,
			wantErr: []string{"row 2", `"w", "x", "y" and 1 more`},
		},
		{
			name:    "the refusal quotes a field that is only spaces",
			content: "a:1\tb:2\n   \n",
			policy:  MalformedRowStop,
			wantErr: []string{"row 2", `"   "`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rows, skipped, err := loadLTSVWithPolicy(t, tt.content, tt.policy)
			if len(tt.wantErr) > 0 {
				if err == nil {
					t.Fatalf("the load accepted a field that names no label: rows=%d", rows)
				}
				if !errors.Is(err, ErrColumnMismatch) {
					t.Errorf("error = %v, want it to match ErrColumnMismatch", err)
				}
				for _, want := range tt.wantErr {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("error = %v, want it to name %q", err, want)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("the load refused a record the policy keeps: %v", err)
			}
			if rows != tt.rows {
				t.Errorf("rows = %d, want %d", rows, tt.rows)
			}
			if skipped != tt.skipped {
				t.Errorf("skipped = %d, want %d", skipped, tt.skipped)
			}
		})
	}
}

// TestLTSVRecordsEveryPolicyKeeps pins the lines the policy has nothing to say
// about, which is what a refusal of an unlabeled field must not reach: a line
// that names some of the columns is padded, and a line that is not a record at
// all is skipped in silence.
func TestLTSVRecordsEveryPolicyKeeps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		rows    int
	}{
		{name: "a line naming only some labels is padded", content: "a:1\tb:2\na:3\n", rows: 2},
		{name: "a blank line between records", content: "a:1\tb:2\n\na:3\tb:4\n", rows: 2},
		{name: "a line of only tabs", content: "a:1\tb:2\n\t\t\na:3\tb:4\n", rows: 2},
		{name: "a record ending in a tab", content: "a:1\tb:2\t\na:3\tb:4\n", rows: 2},
		{name: "no terminator on the last record", content: "a:1\tb:2\na:3\tb:4", rows: 2},
		{name: "a value holding a colon", content: "a:http://x\tb:2\n", rows: 1},
		{name: "an empty value", content: "a:\tb:2\n", rows: 1},
	}

	for _, tt := range tests {
		for _, policy := range []MalformedRowPolicy{MalformedRowStop, MalformedRowSkip, MalformedRowFill} {
			t.Run(tt.name+" under "+policy.String(), func(t *testing.T) {
				t.Parallel()

				rows, skipped, err := loadLTSVWithPolicy(t, tt.content, policy)
				if err != nil {
					t.Fatalf("the load refused a record every policy keeps: %v", err)
				}
				if rows != tt.rows {
					t.Errorf("rows = %d, want %d", rows, tt.rows)
				}
				if skipped != 0 {
					t.Errorf("skipped = %d, want 0", skipped)
				}
			})
		}
	}
}

// loadLTSVWithPolicy loads content as an LTSV file under policy and reports how
// many rows the table holds and how many records the load says it dropped.
func loadLTSVWithPolicy(t *testing.T, content string, policy MalformedRowPolicy) (rows, skipped int, err error) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "policy.ltsv")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	builder, err := buildForTest(ctx, NewBuilder().AddPath(path).WithMalformedRowPolicy(policy))
	if err != nil {
		return 0, 0, err
	}
	db, err := builder.Open(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = db.Close() }()

	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM policy").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	for _, s := range builder.SkippedRows() {
		skipped += s.Count
	}
	return rows, skipped, nil
}
