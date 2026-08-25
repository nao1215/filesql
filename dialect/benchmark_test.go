//go:build benchmark

package dialect

import (
	"context"
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"
)

// benchmarkQueries are one query per dialect, each written the way a caller
// would write it: several calls whose names SQLite shares and means something
// else by, so the rewrite passes have work to do.
var benchmarkQueries = map[Dialect]string{
	MySQL:      `SELECT LEFT(name, 3), FORMAT(amount, 2), LOG(amount), REGEXP_REPLACE(note, 'a+', 'A'), CAST(code AS SIGNED) FROM t WHERE DATE_FORMAT(created, '%Y-%m') = '2026-08' GROUP BY LEFT(name, 3)`,
	PostgreSQL: `SELECT left(name, 3), to_hex(code), regexp_replace(note, 'a+', 'A'), amount::int FROM t WHERE created::date > '2026-01-01' GROUP BY left(name, 3)`,
	GoogleSQL:  `SELECT LEFT(name, 3), FORMAT('%t', name), LOG(amount, 2), CAST(code AS INT64) FROM t WHERE DATE_DIFF(created, DATE '2026-01-01', DAY) > 0 GROUP BY LEFT(name, 3)`,
}

// BenchmarkTranslate measures the rewrite of one query, which a caller pays once
// per prepared statement.
func BenchmarkTranslate(b *testing.B) {
	for _, d := range []Dialect{MySQL, PostgreSQL, GoogleSQL} {
		query := benchmarkQueries[d]
		b.Run(string(d), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := Translate(d, query); err != nil {
					b.Fatalf("Translate(%s): %v", d, err)
				}
			}
		})
	}
}

// BenchmarkDialectFunctions measures the helpers a translated query calls once
// per row.
func BenchmarkDialectFunctions(b *testing.B) {
	if err := RegisterFunctions(); err != nil {
		b.Fatalf("RegisterFunctions(): %v", err)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })

	cases := []struct {
		name    string
		dialect Dialect
		query   string
	}{
		{"mysql_left", MySQL, `SELECT LEFT('abcdef', 3)`},
		{"mysql_format", MySQL, `SELECT FORMAT(1234.5678, 2)`},
		{"mysql_regexp_replace", MySQL, `SELECT REGEXP_REPLACE('aaa', 'a', 'X', 2)`},
		{"postgresql_to_hex", PostgreSQL, `SELECT to_hex(255)`},
		{"postgresql_regexp_replace", PostgreSQL, `SELECT regexp_replace('aaa', 'a', 'X')`},
		{"googlesql_format", GoogleSQL, `SELECT FORMAT('%s=%t', 'a', 'b')`},
		{"googlesql_left", GoogleSQL, `SELECT LEFT('abcdef', 3)`},
	}
	for _, c := range cases {
		translated, err := Translate(c.dialect, c.query)
		if err != nil {
			b.Fatalf("Translate(%s, %s): %v", c.dialect, c.query, err)
		}
		stmt, err := db.PrepareContext(context.Background(), translated)
		if err != nil {
			b.Fatalf("prepare %s: %v", translated, err)
		}
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var got sql.NullString
				if err := stmt.QueryRowContext(context.Background()).Scan(&got); err != nil {
					b.Fatalf("%s: %v", translated, err)
				}
			}
		})
		_ = stmt.Close()
	}
}
