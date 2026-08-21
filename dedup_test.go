package filesql

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Deduplication exists for one case: the same dataset offered both plain and
// compressed, where reading both would build one table from one place twice.
// It used to be keyed on the table name instead of the place, which made
// "a/users.csv" and "b/users.csv" look like the same input. One of them was
// dropped, nothing was said about it, and which one survived came out of a Go
// map — so the same command could lose a different file on different runs.
//
// These tests pin both halves of the repair: what counts as one source, and
// that the surviving order is the order the caller gave.

// TestSourceIdentity covers the rule the deduplication is built on, directly.
func TestSourceIdentity(t *testing.T) {
	t.Parallel()

	t.Run("a codec suffix does not change which source a path names", func(t *testing.T) {
		t.Parallel()
		for _, compressed := range []string{
			"dir/users.csv.gz", "dir/users.csv.bz2", "dir/users.csv.xz",
			"dir/users.csv.zst", "dir/users.csv.z", "dir/users.csv.snappy",
			"dir/users.csv.s2", "dir/users.csv.lz4",
		} {
			if got, want := sourceIdentity(compressed), sourceIdentity("dir/users.csv"); got != want {
				t.Errorf("sourceIdentity(%q) = %q, want %q", compressed, got, want)
			}
		}
	})

	t.Run("the same file written two ways is one source", func(t *testing.T) {
		t.Parallel()
		if got, want := sourceIdentity("./dir/users.csv"), sourceIdentity("dir/users.csv"); got != want {
			t.Errorf("sourceIdentity of an equivalent path = %q, want %q", got, want)
		}
		if got, want := sourceIdentity("dir/../dir/users.csv"), sourceIdentity("dir/users.csv"); got != want {
			t.Errorf("sourceIdentity of an equivalent path = %q, want %q", got, want)
		}
	})

	t.Run("files in different directories are different sources", func(t *testing.T) {
		t.Parallel()
		pairs := [][2]string{
			{"a/users.csv", "b/users.csv"},
			{"a/users.csv", "b/users.csv.gz"},
			{"a/book.xlsx.gz", "b/book.xlsx.gz"},
			{"users.csv", "nested/users.csv"},
		}
		for _, pair := range pairs {
			if sourceIdentity(pair[0]) == sourceIdentity(pair[1]) {
				t.Errorf("%q and %q share a source identity; they are different files", pair[0], pair[1])
			}
		}
	})

	t.Run("different files in one directory are different sources", func(t *testing.T) {
		t.Parallel()
		// These sanitize to the same table name. That is a collision for whoever
		// creates the tables to report, and no business of deduplication's.
		if sourceIdentity("dir/user-data.csv") == sourceIdentity("dir/user data.csv") {
			t.Error("two differently named files share a source identity")
		}
		if sourceIdentity("dir/products.tsv") == sourceIdentity("dir/products.parquet") {
			t.Error("two formats of different data share a source identity")
		}
	})
}

// TestDeduplicateCompressedFiles asserts the whole result slice, in order,
// rather than membership. Order is the half of the contract a set comparison
// cannot see, and it is the half everything downstream reads: which input a
// last-wins load leaves in place, which malformed file a failing load names,
// and what order the collision check works through.
func TestDeduplicateCompressedFiles(t *testing.T) {
	t.Parallel()

	fp := newFileProcessor()

	tests := []struct {
		name  string
		files []string
		want  []string
	}{
		{
			name:  "a plain file wins over its own compressed twin",
			files: []string{"dir/users.csv", "dir/users.csv.gz"},
			want:  []string{"dir/users.csv"},
		},
		{
			name: "and wins from behind, too",
			// The plain file is second here. Preferring it must not depend on it
			// being seen first.
			files: []string{"dir/users.csv.gz", "dir/users.csv"},
			want:  []string{"dir/users.csv"},
		},
		{
			name:  "the same name in two directories is two sources",
			files: []string{"a/users.csv", "b/users.csv"},
			want:  []string{"a/users.csv", "b/users.csv"},
		},
		{
			name: "a plain file does not displace a compressed file somewhere else",
			// The bug this replaces: both map to table "users", so one vanished.
			files: []string{"a/users.csv", "b/users.csv.gz"},
			want:  []string{"a/users.csv", "b/users.csv.gz"},
		},
		{
			name:  "two compressed workbooks in different directories both survive",
			files: []string{"a/book.xlsx.gz", "b/book.xlsx.gz"},
			want:  []string{"a/book.xlsx.gz", "b/book.xlsx.gz"},
		},
		{
			name: "files that only share a sanitized name both survive",
			// "user-data" and "user data" both sanitize to user_data, and
			// products.tsv and products.parquet both to products. Whether that
			// can be loaded is the table layer's decision, made out loud.
			files: []string{"dir/user-data.csv", "dir/user data.csv", "dir/products.tsv", "dir/products.parquet"},
			want:  []string{"dir/user-data.csv", "dir/user data.csv", "dir/products.tsv", "dir/products.parquet"},
		},
		{
			name: "order survives a longer list",
			files: []string{
				"z/last.csv", "a/first.csv", "m/middle.csv.gz",
				"a/first.csv.gz", "q/other.tsv",
			},
			want: []string{"z/last.csv", "a/first.csv", "m/middle.csv.gz", "q/other.tsv"},
		},
		{
			name:  "the same source named twice is read once",
			files: []string{"./dir/users.csv", "dir/users.csv"},
			want:  []string{"./dir/users.csv"},
		},
		{
			name:  "an exact repeat is read once",
			files: []string{"dir/users.csv", "dir/users.csv"},
			want:  []string{"dir/users.csv"},
		},
		{
			name:  "a lone compressed file is kept",
			files: []string{"dir/users.csv.gz"},
			want:  []string{"dir/users.csv.gz"},
		},
		{
			name:  "nothing in, nothing out",
			files: []string{},
			want:  []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := fp.deduplicateCompressedFiles(tt.files)
			if strings.Join(got, "|") != strings.Join(tt.want, "|") {
				t.Errorf("deduplicateCompressedFiles(%v)\n got %v\nwant %v", tt.files, got, tt.want)
			}
		})
	}
}

// TestDeduplicateCompressedFilesIsStable checks the result is a function of the
// input and nothing else. The old implementation built its result by ranging
// over a map, so the answer was free to change between calls in one process —
// which is exactly the kind of failure that reproduces once and then hides.
func TestDeduplicateCompressedFilesIsStable(t *testing.T) {
	t.Parallel()

	fp := newFileProcessor()
	files := []string{
		"a/users.csv", "b/users.csv.gz", "c/users.csv", "a/users.csv.gz",
		"d/orders.tsv", "e/orders.tsv", "f/logs.ltsv.xz",
	}
	want := strings.Join(fp.deduplicateCompressedFiles(files), "|")

	for i := range 50 {
		got := strings.Join(fp.deduplicateCompressedFiles(files), "|")
		if got != want {
			t.Fatalf("call %d returned %q, want %q; the result depends on something other than the input", i, got, want)
		}
	}
	// And the answer itself, so a stable-but-wrong result is not mistaken for
	// success.
	if expected := "a/users.csv|b/users.csv.gz|c/users.csv|d/orders.tsv|e/orders.tsv|f/logs.ltsv.xz"; want != expected {
		t.Errorf("result = %q, want %q", want, expected)
	}
}

// writeCSVFile writes a one-column CSV whose single row carries value.
func writeCSVFile(t *testing.T, path, value string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("v\n"+value+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// sameNamePair writes a/book.csv and b/book.csv, each carrying its own value.
// Both want the table "book", so what each API does with them is the question
// these tests ask.
func sameNamePair(t *testing.T) (first, second string) {
	t.Helper()
	dir := t.TempDir()
	return writeCSVFile(t, filepath.Join(dir, "a", "book.csv"), "from-a"),
		writeCSVFile(t, filepath.Join(dir, "b", "book.csv"), "from-b")
}

// TestOpenContextRefusesTwoSourcesWantingOneTable pins Open's side of the
// contract. Open builds a fresh database, so two files asking for one table
// cannot both be honored, and the answer is a refusal that names the file —
// deterministically, on every run. What it must never be is one of the two
// loading and the other disappearing.
func TestOpenContextRefusesTwoSourcesWantingOneTable(t *testing.T) {
	t.Parallel()
	first, second := sameNamePair(t)

	db, err := OpenContext(context.Background(), first, second)
	if err == nil {
		_ = db.Close()
		t.Fatal("OpenContext succeeded on two files that both want the table 'book'")
	}
	if !errors.Is(err, ErrDuplicateTable) {
		t.Errorf("error = %v, want ErrDuplicateTable", err)
	}
	// The second file is the one that found the name taken, so it is the one
	// named. The path is compared as the platform writes it — normalizing it
	// here would have passed on Windows whatever the message said.
	if !strings.Contains(err.Error(), second) {
		t.Errorf("error %q should name the file that collided, %q", err, second)
	}

	reversed, err := OpenContext(context.Background(), second, first)
	if err == nil {
		_ = reversed.Close()
		t.Fatal("OpenContext succeeded with the arguments reversed")
	}
	if !strings.Contains(err.Error(), first) {
		t.Errorf("with the arguments reversed the error %q should name %q", err, first)
	}
}

// TestLoadIntoKeepsItsLastWinsContract pins the other API's side, and pins that
// this change did not move it. LoadInto loads into a database the caller owns,
// where replacing a same-named table is the useful behavior and has been the
// documented one; what changed is only that both inputs are now read at all.
// Before, one of them was discarded before any of this was reached.
func TestLoadIntoKeepsItsLastWinsContract(t *testing.T) {
	t.Parallel()
	first, second := sameNamePair(t)

	for _, tt := range []struct {
		name  string
		paths []string
		want  string
	}{
		{name: "the later input wins", paths: []string{first, second}, want: "from-b"},
		{name: "and reversing the order reverses the winner", paths: []string{second, first}, want: "from-a"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := newMemoryDB(t)
			ctx := context.Background()
			if err := LoadInto(ctx, db, tt.paths...); err != nil {
				t.Fatalf("LoadInto: %v", err)
			}
			var got string
			if err := db.QueryRowContext(ctx, "SELECT v FROM book").Scan(&got); err != nil {
				t.Fatalf("query book: %v", err)
			}
			if got != tt.want {
				t.Errorf("book holds %q, want %q; input order decides last-wins", got, tt.want)
			}
		})
	}
}

// TestLoadIntoTxKeepsInputOrder is the same question for the transactional
// entry point, where the caller owns the transaction as well.
func TestLoadIntoTxKeepsInputOrder(t *testing.T) {
	t.Parallel()
	first, second := sameNamePair(t)

	for _, tt := range []struct {
		name  string
		paths []string
		want  string
	}{
		{name: "the later input wins", paths: []string{first, second}, want: "from-b"},
		{name: "and reversing the order reverses the winner", paths: []string{second, first}, want: "from-a"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := newMemoryDB(t)
			ctx := context.Background()

			builder, err := NewBuilder().AddPaths(tt.paths...).Build(ctx)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("BeginTx: %v", err)
			}
			if err := builder.LoadIntoTx(ctx, tx); err != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					t.Errorf("rollback after a failed load: %v", rollbackErr)
				}
				t.Fatalf("LoadIntoTx: %v", err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("Commit: %v", err)
			}

			var got string
			if err := db.QueryRowContext(ctx, "SELECT v FROM book").Scan(&got); err != nil {
				t.Fatalf("query book: %v", err)
			}
			if got != tt.want {
				t.Errorf("book holds %q, want %q; input order decides last-wins", got, tt.want)
			}
		})
	}
}

// TestLoadReadsEveryDistinctSource is the plainest statement of what the repair
// bought: files that merely share a base name all arrive.
func TestLoadReadsEveryDistinctSource(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	paths := []string{
		writeCSVFile(t, filepath.Join(dir, "a", "users.csv"), "a"),
		writeCSVFile(t, filepath.Join(dir, "b", "orders.csv"), "b"),
		writeCSVFile(t, filepath.Join(dir, "c", "logs.csv"), "c"),
	}

	db := newMemoryDB(t)
	ctx := context.Background()
	if err := LoadInto(ctx, db, paths...); err != nil {
		t.Fatalf("LoadInto: %v", err)
	}
	for table, want := range map[string]string{"users": "a", "orders": "b", "logs": "c"} {
		var got string
		if err := db.QueryRowContext(ctx, "SELECT v FROM "+table).Scan(&got); err != nil {
			t.Fatalf("query %s: %v", table, err)
		}
		if got != want {
			t.Errorf("%s holds %q, want %q", table, got, want)
		}
	}
}

// TestOpenDirectoryWithCollidingBasenamesFails is the test the repointed
// fixtures above refer to. A directory tree holding two files of the same base
// name in different subdirectories cannot become one database, and saying so is
// the whole point: the previous behavior was to load one of them and leave the
// other out, without a word and without deciding which.
func TestOpenDirectoryWithCollidingBasenamesFails(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeCSVFile(t, filepath.Join(dir, "a", "book.csv"), "from-a")
	writeCSVFile(t, filepath.Join(dir, "b", "book.csv"), "from-b")

	db, err := Open(dir)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open succeeded on a tree with two files that both want the table 'book'")
	}
	if !errors.Is(err, ErrDuplicateTable) {
		t.Errorf("error = %v, want ErrDuplicateTable", err)
	}
}

// TestOpenDirectoryStillPrefersThePlainSibling keeps the case deduplication is
// actually for: one dataset, offered twice in one place.
func TestOpenDirectoryStillPrefersThePlainSibling(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeCSVFile(t, filepath.Join(dir, "users.csv"), "plain")
	gzipFile(t, filepath.Join(dir, "users.csv"), filepath.Join(dir, "users.csv.gz"))

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	var rows int
	if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM users").Scan(&rows); err != nil {
		t.Fatalf("query users: %v", err)
	}
	if rows != 1 {
		t.Errorf("users holds %d rows, want 1; the compressed twin was loaded as well", rows)
	}
}

// newMemoryDB opens a caller-owned in-memory database, pinned to one connection
// so every query reaches the same one.
func newMemoryDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestOpenRefusesTwoSourcesWhoseNamesDifferOnlyInCase is the same question with
// the names spelled differently. SQLite folds ASCII case when it compares
// identifiers, so "Users" and "users" are one table there whatever the file
// names were; the check that finds the clash has to fold the same way, or the
// second file's rows land in the first file's table by position and the columns
// they were written under are gone.
func TestOpenRefusesTwoSourcesWhoseNamesDifferOnlyInCase(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "Users.csv"), []byte("a,b\n1,2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "users.csv"), []byte("c,d\n3,4\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	db, err := Open(dir)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open succeeded on two files that both want one table, spelled in two cases")
	}
	if !errors.Is(err, ErrDuplicateTable) {
		t.Errorf("error = %v, want ErrDuplicateTable", err)
	}
}

// TestBuilderRefusesTwoReadersWhoseNamesDifferOnlyInCase asks it of the API
// where the caller picks the names outright, which no filesystem constrains.
func TestBuilderRefusesTwoReadersWhoseNamesDifferOnlyInCase(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	validated, err := NewBuilder().
		AddReader(strings.NewReader("a,b\n1,2\n"), "Users", FileTypeCSV).
		AddReader(strings.NewReader("c,d\n3,4\n"), "users", FileTypeCSV).
		Build(ctx)
	if err != nil {
		if !errors.Is(err, ErrDuplicateTable) {
			t.Errorf("Build error = %v, want ErrDuplicateTable", err)
		}
		return
	}

	db, err := validated.Open(ctx)
	if err == nil {
		_ = db.Close()
		t.Fatal("two readers named Users and users both loaded")
	}
	if !errors.Is(err, ErrDuplicateTable) {
		t.Errorf("error = %v, want ErrDuplicateTable", err)
	}
}

// TestReplacingKeepsTheLaterSpellingsColumns pins what the clash looks like when
// replacing is asked for: one table, holding the second file's columns and rows
// rather than the second file's rows under the first file's headers.
func TestReplacingKeepsTheLaterSpellingsColumns(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	first := filepath.Join(dir, "Users.csv")
	second := filepath.Join(dir, "users.csv")
	if err := os.WriteFile(first, []byte("a,b\n1,2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(second, []byte("c,d\n3,4\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	db := newMemoryDB(t)
	if err := LoadInto(ctx, db, first, second); err != nil {
		t.Fatalf("LoadInto: %v", err)
	}

	var tables int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE '\_filesql\_%' ESCAPE '\'`,
	).Scan(&tables); err != nil {
		t.Fatalf("count tables: %v", err)
	}
	if tables != 1 {
		t.Errorf("database holds %d tables, want 1", tables)
	}

	var columns string
	if err := db.QueryRowContext(ctx,
		`SELECT group_concat(name) FROM pragma_table_info('users')`,
	).Scan(&columns); err != nil {
		t.Fatalf("read columns: %v", err)
	}
	if columns != "c,d" {
		t.Errorf("the surviving table has columns %q, want %q; the later file's rows are stored under the earlier file's headers", columns, "c,d")
	}
}
