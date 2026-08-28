package runtime

import (
	"database/sql/driver"

	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/nao1215/filesql/dialect/internal/dialects"
	"github.com/nao1215/filesql/dialect/internal/sqlerr"
)

// TestOperatorSemantics locks in the operators whose meaning changes on the way
// to SQLite: division that truncates, a LIKE that folds case, and the MySQL
// spellings of OR and null-safe equality.
func TestOperatorSemantics(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		name    string
		dialect dialects.Dialect
		query   string
		want    string
		null    bool
	}{
		// Division is floating point in MySQL and GoogleSQL, integer division
		// in SQLite when both operands are integers.
		{"mysql divides as float", dialects.MySQL, `SELECT 5/2`, "2.5", false},
		{"googlesql divides as float", dialects.GoogleSQL, `SELECT 5/2`, "2.5", false},
		{"mysql divides exactly", dialects.MySQL, `SELECT 4/2`, "2", false},
		{"mysql divides by zero to null", dialects.MySQL, `SELECT 1/0`, "", true},
		{"mysql chains division left to right", dialects.MySQL, `SELECT 100/4/5`, "5", false},
		{"mysql divides a call result", dialects.MySQL, `SELECT ABS(-5)/2`, "2.5", false},
		{"mysql divides a parenthesized operand", dialects.MySQL, `SELECT (3+7)/4`, "2.5", false},
		{"mysql divides by a negative literal", dialects.MySQL, `SELECT 5/-2`, "-2.5", false},
		{"mysql mixes multiplication and division", dialects.MySQL, `SELECT 2*3/4`, "1.5", false},
		{"mysql DIV still truncates", dialects.MySQL, `SELECT 7 DIV 2`, "3", false},
		{"mysql DIV truncates toward zero", dialects.MySQL, `SELECT -7 DIV 2`, "-3", false},
		{"postgresql keeps integer division", dialects.PostgreSQL, `SELECT 5/2`, "2", false},

		// MOD is DIV's sibling and the same operation as SQLite's "%", including
		// the sign rule: the result takes the sign of the dividend.
		{"mysql MOD is the remainder", dialects.MySQL, `SELECT 7 MOD 2`, "1", false},
		{"mysql MOD takes the dividend's sign", dialects.MySQL, `SELECT -7 MOD 2`, "-1", false},
		{"mysql MOD by a negative divisor", dialects.MySQL, `SELECT 7 MOD -2`, "1", false},
		{"mysql MOD binds like multiplication", dialects.MySQL, `SELECT 1 + 7 MOD 2`, "2", false},
		{"mysql MOD chains left to right", dialects.MySQL, `SELECT 7 MOD 2 * 3`, "3", false},
		{"mysql MOD as a function is unchanged", dialects.MySQL, `SELECT MOD(7, 2)`, "1", false},

		// DIV, MOD, "*", "/" and "%" are one precedence level in MySQL and
		// associate left to right, so the left operand of a DIV is the whole
		// chain to its left rather than the value beside it.
		{"mysql DIV divides a product", dialects.MySQL, `SELECT 8 * 5 DIV 2`, "20", false},
		{"mysql DIV divides a quotient", dialects.MySQL, `SELECT 100 / 5 DIV 2`, "10", false},
		{"mysql DIV divides a remainder", dialects.MySQL, `SELECT 8 % 5 DIV 2`, "1", false},
		{"mysql DIV divides a MOD result", dialects.MySQL, `SELECT 8 MOD 5 DIV 2`, "1", false},
		{"mysql MOD takes a product as its left operand", dialects.MySQL, `SELECT 4 * 5 MOD 7`, "6", false},
		{"mysql DIV chains left to right", dialects.MySQL, `SELECT 8 DIV 2 DIV 2`, "2", false},
		// An operator that binds less tightly stays outside the operand.
		{"mysql DIV binds tighter than addition", dialects.MySQL, `SELECT 2 + 8 DIV 2`, "6", false},
		{"mysql DIV binds tighter than subtraction", dialects.MySQL, `SELECT 2 - 8 DIV 2`, "-2", false},
		{"mysql DIV of a call result", dialects.MySQL, `SELECT ABS(-8) DIV 2`, "4", false},
		{"mysql DIV of a parenthesized sum", dialects.MySQL, `SELECT (2 + 8) DIV 3`, "3", false},
		{"mysql DIV inside a call argument", dialects.MySQL, `SELECT MAX(8 * 5 DIV 2)`, "20", false},

		// "/" takes its left operand the same way DIV does, and a remainder in
		// that chain is the case where regrouping changes the value.
		{"mysql divides a remainder", dialects.MySQL, `SELECT 7 % 4 / 2`, "1.5", false},
		{"mysql divides a MOD result", dialects.MySQL, `SELECT 7 MOD 4 / 2`, "1.5", false},
		{"mysql divides a longer chain", dialects.MySQL, `SELECT 9 % 4 * 2 / 4`, "0.5", false},
		{"mysql division binds tighter than addition", dialects.MySQL, `SELECT 2 + 8 / 2`, "6", false},
		{"mysql division binds tighter than subtraction", dialects.MySQL, `SELECT 2 - 8 / 2`, "-2", false},
		// The other callers of the same pass must keep their own precedence: a
		// bitwise XOR binds tighter than "*" in MySQL, and PostgreSQL's "^" is
		// exponentiation, which it reads left to right.
		{"mysql xor binds tighter than multiplication", dialects.MySQL, `SELECT 2 * 3 ^ 1`, "4", false},
		{"postgresql power binds tighter than multiplication", dialects.PostgreSQL, `SELECT 2 * 3 ^ 2`, "18", false},
		{"postgresql power associates left to right", dialects.PostgreSQL, `SELECT 2 ^ 3 ^ 2`, "64", false},

		// The operators MySQL spells with punctuation, executed rather than only
		// rewritten: the rewrite is only right if the answer is.
		{"mysql && is AND", dialects.MySQL, `SELECT 1 && 0`, "0", false},
		{"mysql && is true when both are", dialects.MySQL, `SELECT 1 && 2`, "1", false},
		{"mysql ! negates", dialects.MySQL, `SELECT !0`, "1", false},
		{"mysql ! negates a nonzero", dialects.MySQL, `SELECT !5`, "0", false},
		// The case that tells the two readings apart: "(!1) = 2" is 0, while the
		// bare "NOT 1 = 2" SQLite would have parsed is 1.
		{"mysql ! binds tighter than a comparison", dialects.MySQL, `SELECT !1 = 2`, "0", false},
		{"mysql ! twice is the value's truth", dialects.MySQL, `SELECT !!5`, "1", false},
		{"mysql ^ is a bitwise xor", dialects.MySQL, `SELECT 5 ^ 3`, "6", false},
		{"mysql ^ of equal operands is zero", dialects.MySQL, `SELECT 7 ^ 7`, "0", false},
		// The bits are what the operator works on, and SQLite's only integer is
		// signed, so a result with the high bit set reads as a negative number
		// where MySQL prints it unsigned.
		{"mysql ^ works on the high bit", dialects.MySQL, `SELECT -1 ^ 0`, "-1", false},
		// A text operand past int64 is read as the unsigned number it spells. As a
		// literal it never reaches the helper unchanged: SQLite's own parser turns
		// an integer past int64 into a float first.
		{"mysql ^ of a high-bit text operand", dialects.MySQL, `SELECT '18446744073709551615' ^ 0`, "-1", false},
		{"mysql ^ propagates null", dialects.MySQL, `SELECT 1 ^ NULL`, "", true},

		// LIKE is case-sensitive outside MySQL; SQLite's folds ASCII case.
		{"postgresql LIKE is case-sensitive", dialects.PostgreSQL, `SELECT 'ABC' LIKE 'abc'`, "0", false},
		{"postgresql LIKE matches exactly", dialects.PostgreSQL, `SELECT 'abc' LIKE 'a%'`, "1", false},
		{"postgresql NOT LIKE", dialects.PostgreSQL, `SELECT 'ABC' NOT LIKE 'abc'`, "1", false},
		{"postgresql ILIKE folds case", dialects.PostgreSQL, `SELECT 'ABC' ILIKE 'abc'`, "1", false},
		{"postgresql ILIKE folds beyond ascii", dialects.PostgreSQL, `SELECT 'ÄBC' ILIKE 'äbc'`, "1", false},
		{"postgresql NOT ILIKE", dialects.PostgreSQL, `SELECT 'ABC' NOT ILIKE 'abc'`, "0", false},
		{"postgresql underscore wildcard", dialects.PostgreSQL, `SELECT 'abc' LIKE 'a_c'`, "1", false},
		{"postgresql trailing percent", dialects.PostgreSQL, `SELECT 'abc' LIKE '%c'`, "1", false},
		{"postgresql anchored percent", dialects.PostgreSQL, `SELECT 'abc' LIKE '%b%'`, "1", false},
		{"postgresql empty pattern", dialects.PostgreSQL, `SELECT '' LIKE ''`, "1", false},
		{"postgresql percent matches empty", dialects.PostgreSQL, `SELECT '' LIKE '%'`, "1", false},
		{"googlesql LIKE is case-sensitive", dialects.GoogleSQL, `SELECT 'ABC' LIKE 'abc'`, "0", false},
		{"mysql LIKE stays case-insensitive", dialects.MySQL, `SELECT 'ABC' LIKE 'abc'`, "1", false},
		// MySQL's default collation folds case beyond ASCII, where SQLite's LIKE
		// stops at it, so this pair used to answer 0.
		{"mysql LIKE folds beyond ascii", dialects.MySQL, `SELECT 'É' LIKE 'é'`, "1", false},
		// A pattern ending in the escape character reads that character as
		// itself. SQLite's native LIKE ... ESCAPE matches nothing for it, so a
		// row holding exactly that text was dropped.
		{"mysql LIKE with a trailing escape", dialects.MySQL, `SELECT 'A\\' LIKE 'A\\'`, "1", false},
		{"mysql LIKE still escapes a wildcard", dialects.MySQL, `SELECT 'a%b' LIKE 'a\%b'`, "1", false},
		{"mysql LIKE escape excludes the wildcard match", dialects.MySQL, `SELECT 'axb' LIKE 'a\%b'`, "0", false},
		{"mysql NOT LIKE", dialects.MySQL, `SELECT 'abc' NOT LIKE 'x%'`, "1", false},
		{"mysql LIKE keeps a named escape", dialects.MySQL, `SELECT 'a%b' LIKE 'a!%b' ESCAPE '!'`, "1", false},

		// PostgreSQL "^" is exponentiation, not a SQLite operator at all.
		{"postgresql power", dialects.PostgreSQL, `SELECT 2 ^ 3`, "8", false},

		// MySQL reads "||" as OR and "<=>" as null-safe equality.
		{"mysql pipes are OR", dialects.MySQL, `SELECT 1 || 0`, "1", false},
		{"mysql pipes are OR when false", dialects.MySQL, `SELECT 0 || 0`, "0", false},
		{"mysql null-safe equality", dialects.MySQL, `SELECT NULL <=> NULL`, "1", false},
		{"mysql null-safe inequality", dialects.MySQL, `SELECT 1 <=> NULL`, "0", false},

		// MySQL HEX takes the value of a number, not the bytes of its digits.
		{"mysql hex of a number", dialects.MySQL, `SELECT HEX(255)`, "FF", false},
		{"mysql hex of a string", dialects.MySQL, `SELECT HEX('ab')`, "6162", false},
		{"mysql unhex", dialects.MySQL, `SELECT UNHEX('6162')`, "ab", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runDialect(t, db, tt.dialect, tt.query)
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			if tt.null {
				if got.Valid {
					t.Fatalf("%s = %q, want NULL", tt.query, got.String)
				}
				return
			}
			if !got.Valid || got.String != tt.want {
				t.Fatalf("%s = %v, want %q", tt.query, got, tt.want)
			}
		})
	}
}

// TestGoogleSQLDivideByZeroRaises keeps the one place the two float-division
// dialects disagree: GoogleSQL raises where MySQL answers NULL, and offers
// SAFE_DIVIDE to callers who want the NULL.
func TestGoogleSQLDivideByZeroRaises(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	if _, err := runDialect(t, db, dialects.GoogleSQL, `SELECT 1/0`); err == nil {
		t.Fatal("GoogleSQL division by zero should fail")
	}
	got, err := runDialect(t, db, dialects.GoogleSQL, `SELECT SAFE_DIVIDE(1, 0)`)
	if err != nil {
		t.Fatalf("SAFE_DIVIDE: %v", err)
	}
	if got.Valid {
		t.Fatalf("SAFE_DIVIDE(1, 0) = %q, want NULL", got.String)
	}
}

// TestZeroDivisorFollowsTheDialect pins what each dialect does with a zero
// divisor, which is the one place the three engines do not agree: two of them
// stop the query and one answers NULL, while SQLite always answers NULL.
// postgres:17-alpine raises "division by zero" for all four spellings, BigQuery
// raises for "/" and MOD() and has no "%" of its own, and MySQL answers NULL.
// So a PostgreSQL query the engine would have stopped came back with rows in
// it, and a NULL in a numeric column reads as missing data rather than as
// arithmetic the engine refused.
func TestZeroDivisorFollowsTheDialect(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	raising := []struct {
		dialect dialects.Dialect
		queries []string
	}{
		{dialects.PostgreSQL, []string{`SELECT 7/0`, `SELECT 7.0/0.0`, `SELECT 7 % 0`, `SELECT mod(7, 0)`}},
		{dialects.GoogleSQL, []string{`SELECT 7/0`, `SELECT 7 % 0`, `SELECT MOD(7, 0)`}},
	}
	for _, tt := range raising {
		for _, query := range tt.queries {
			_, err := runDialect(t, db, tt.dialect, query)
			if err == nil {
				t.Errorf("%v: %s answered a value; the engine raises", tt.dialect, query)
				continue
			}
			// The driver renders a scalar function's error as text on the way
			// out, so the sentinel does not survive to be matched with
			// errors.Is; the message it carries is what a caller sees.
			if !strings.Contains(err.Error(), "dialect: division by zero") {
				t.Errorf("%v: %s = %v, want a division-by-zero failure", tt.dialect, query, err)
			}
			// The words matter as much as the failure: the message used to
			// read "invalid cast: division by zero", naming a conversion the
			// query does not contain.
			if strings.Contains(err.Error(), "invalid cast") {
				t.Errorf("%v: %s = %v, want no mention of a cast", tt.dialect, query, err)
			}
		}
	}

	// A NULL operand is nothing to divide by, so it stays NULL rather than
	// raising: the query has no zero in it.
	for _, query := range []string{`SELECT 7 / NULL`, `SELECT 7 % NULL`, `SELECT NULL % 2`} {
		got, err := runDialect(t, db, dialects.PostgreSQL, query)
		if err != nil {
			t.Errorf("PostgreSQL: %s = %v, want NULL", query, err)
			continue
		}
		if got.Valid {
			t.Errorf("PostgreSQL: %s = %q, want NULL", query, got.String)
		}
	}

	// MySQL's NULL is correct and must not move.
	for _, query := range []string{`SELECT 7/0`, `SELECT 7 % 0`, `SELECT MOD(7, 0)`} {
		got, err := runDialect(t, db, dialects.MySQL, query)
		if err != nil {
			t.Errorf("MySQL: %s = %v, want NULL", query, err)
			continue
		}
		if got.Valid {
			t.Errorf("MySQL: %s = %q, want NULL", query, got.String)
		}
	}
}

// TestZeroDivisorLeavesTheArithmeticAlone is the other half: routing the
// operators through a helper must not change what a division or a remainder
// answers for any operand a query can carry. PostgreSQL divides two integers as
// integers, the way SQLite does, while MySQL and GoogleSQL answer a real, and
// text, floats and out-of-range values are read the way SQLite reads them.
func TestZeroDivisorLeavesTheArithmeticAlone(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		dialect dialects.Dialect
		query   string
		want    string
	}{
		{dialects.PostgreSQL, `SELECT 7/2`, "3"},
		{dialects.PostgreSQL, `SELECT -7/2`, "-3"},
		{dialects.PostgreSQL, `SELECT 1/2`, "0"},
		{dialects.PostgreSQL, `SELECT 7/2.0`, "3.5"},
		{dialects.PostgreSQL, `SELECT 7.0/2`, "3.5"},
		{dialects.PostgreSQL, `SELECT 7 % 2`, "1"},
		{dialects.PostgreSQL, `SELECT -7 % 2`, "-1"},
		{dialects.PostgreSQL, `SELECT mod(7, 2)`, "1"},
		{dialects.GoogleSQL, `SELECT 7/2`, "3.5"},
		{dialects.GoogleSQL, `SELECT 7 % 2`, "1"},
		{dialects.GoogleSQL, `SELECT MOD(7, 2)`, "1"},
		{dialects.MySQL, `SELECT 7/2`, "3.5"},
		{dialects.MySQL, `SELECT 7 % 2`, "1"},

		// The remainder is taken on the operands as written rather than on the
		// integers SQLite truncates them to, which is what every dialect here
		// answers: 7.5 % 2 is 1.5 and not the 1 SQLite's own "%" gives. Text
		// still takes the number it spells, and text that spells none is zero.
		{dialects.PostgreSQL, `SELECT 7.5 % 2`, "1.5"},
		{dialects.PostgreSQL, `SELECT -7.5 % 2`, "-1.5"},
		{dialects.PostgreSQL, `SELECT 7 % 2.5`, "2"},
		{dialects.PostgreSQL, `SELECT typeof(7 % 2.5)`, "real"},
		{dialects.MySQL, `SELECT 7.5 % 2`, "1.5"},
		{dialects.MySQL, `SELECT 7 % 2.5`, "2"},
		{dialects.MySQL, `SELECT 7 MOD 2.5`, "2"},
		{dialects.MySQL, `SELECT MOD(7, 2.5)`, "2"},
		{dialects.GoogleSQL, `SELECT MOD(7, 2.5)`, "2"},
		{dialects.PostgreSQL, `SELECT '7' % 2`, "1"},
		{dialects.PostgreSQL, `SELECT 'abc' % 2`, "0"},
		{dialects.PostgreSQL, `SELECT '7' / 2`, "3"},
		{dialects.PostgreSQL, `SELECT 'abc' / 2`, "0"},
		{dialects.GoogleSQL, `SELECT 'abc' % 2`, "0"},

		// A divisor is zero as a value rather than as the integer it truncates
		// to, so a fractional divisor that divides evenly answers zero instead
		// of being refused.
		{dialects.PostgreSQL, `SELECT 7 % 0.5`, "0"},
		{dialects.MySQL, `SELECT 7 % 0.5`, "0"},

		// A magnitude past what a float64 holds exactly is the ceiling this
		// package has and PostgreSQL does not: its numeric is arbitrary
		// precision, so it answers 0 for the second of these where the nearest
		// float64 to that integer is 2^63 and leaves 1. The first agrees.
		{dialects.PostgreSQL, `SELECT 1e300 % 7`, "1"},
		{dialects.PostgreSQL, `SELECT 9223372036854775807.0 % 7`, "1"},
		{dialects.PostgreSQL, `SELECT typeof(7.5 % 2)`, "real"},
		{dialects.PostgreSQL, `SELECT typeof(7 % 2)`, "integer"},
		{dialects.PostgreSQL, `SELECT typeof('7' / 2)`, "integer"},
	}
	for _, tt := range tests {
		got, err := runDialect(t, db, tt.dialect, tt.query)
		if err != nil {
			t.Errorf("%v: %s: %v", tt.dialect, tt.query, err)
			continue
		}
		if !got.Valid || got.String != tt.want {
			t.Errorf("%v: %s = %v, want %q", tt.dialect, tt.query, got, tt.want)
		}
	}
}

// TestArithmeticFunctionsAreTranslated pins the five functions both engines
// define that SQLite has no form of, so a query written for either dialect is
// computed rather than refused with a SQLite message naming a function the
// caller did not write. Every value was read from postgres:17-alpine, except
// GoogleSQL's, which came from the BigQuery emulator; its TRUNC takes one
// argument only, so the two-argument answer follows PostgreSQL's, which is what
// BigQuery documents.
func TestArithmeticFunctionsAreTranslated(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		dialect dialects.Dialect
		query   string
		want    string
	}{
		// An integer division truncates toward zero rather than flooring, so a
		// negative operand answers -3 rather than -4.
		{dialects.PostgreSQL, `SELECT div(7, 2)`, "3"},
		{dialects.PostgreSQL, `SELECT div(-7, 2)`, "-3"},
		{dialects.PostgreSQL, `SELECT div(7, -2)`, "-3"},
		{dialects.GoogleSQL, `SELECT DIV(7, 2)`, "3"},
		{dialects.GoogleSQL, `SELECT DIV(-7, 2)`, "-3"},
		// The quotient is what truncates, not the operands: 7 / 2.5 is 2.8 and
		// truncates to 2, where truncating the divisor to 2 first answered 3.
		{dialects.PostgreSQL, `SELECT div(7, 2.5)`, "2"},
		{dialects.PostgreSQL, `SELECT div(-7, 2.5)`, "-2"},
		{dialects.PostgreSQL, `SELECT div(7.5, 2)`, "3"},
		{dialects.PostgreSQL, `SELECT div(-7.5, 2.5)`, "-3"},
		{dialects.GoogleSQL, `SELECT DIV(7, 2.5)`, "2"},
		// A divisor that is zero only after truncation still divides.
		{dialects.PostgreSQL, `SELECT div(7, 0.5)`, "14"},

		// A truncation at a scale cuts toward zero, and a negative scale
		// truncates to a power of ten.
		{dialects.PostgreSQL, `SELECT trunc(12.345, 2)`, "12.34"},
		{dialects.PostgreSQL, `SELECT trunc(-12.345, 2)`, "-12.34"},
		{dialects.PostgreSQL, `SELECT trunc(12.345, 0)`, "12"},
		{dialects.PostgreSQL, `SELECT trunc(12.345, -1)`, "10"},
		{dialects.PostgreSQL, `SELECT trunc(-12.345, -1)`, "-10"},
		{dialects.PostgreSQL, `SELECT trunc(12345.6, -2)`, "12300"},
		{dialects.PostgreSQL, `SELECT trunc(12.345, -3)`, "0"},
		// A scale whose power of ten leaves the float64 range at either end:
		// PostgreSQL truncates everything away at one and keeps the value at
		// the other.
		{dialects.PostgreSQL, `SELECT trunc(12.345, -400)`, "0"},
		{dialects.PostgreSQL, `SELECT trunc(12.345, 400)`, "12.345"},
		{dialects.GoogleSQL, `SELECT TRUNC(12.345, 2)`, "12.34"},

		// The one-argument form is SQLite's own and must keep working.
		{dialects.PostgreSQL, `SELECT trunc(12.345)`, "12"},
		{dialects.GoogleSQL, `SELECT TRUNC(12.345)`, "12"},

		// The buckets are numbered from 1, with 0 below the range and count+1
		// above it, and the bounds may be given in either order.
		{dialects.PostgreSQL, `SELECT width_bucket(5.35, 0.024, 10.06, 5)`, "3"},
		{dialects.PostgreSQL, `SELECT width_bucket(0.0, 0.024, 10.06, 5)`, "0"},
		{dialects.PostgreSQL, `SELECT width_bucket(20.0, 0.024, 10.06, 5)`, "6"},
		{dialects.PostgreSQL, `SELECT width_bucket(5.0, 10.0, 0.0, 5)`, "3"},
	}
	for _, tt := range tests {
		got, err := runDialect(t, db, tt.dialect, tt.query)
		if err != nil {
			t.Errorf("%v: %s: %v", tt.dialect, tt.query, err)
			continue
		}
		if !got.Valid || got.String != tt.want {
			t.Errorf("%v: %s = %v, want %q", tt.dialect, tt.query, got, tt.want)
		}
	}

	// A zero divisor raises for the function the way it does for the operator,
	// and a range of no width is refused the way PostgreSQL refuses it.
	for _, tt := range []struct {
		dialect dialects.Dialect
		query   string
	}{
		{dialects.PostgreSQL, `SELECT div(7, 0)`},
		{dialects.GoogleSQL, `SELECT DIV(7, 0)`},
		{dialects.PostgreSQL, `SELECT width_bucket(5.0, 3.0, 3.0, 5)`},
		{dialects.PostgreSQL, `SELECT width_bucket(5.0, 0.0, 10.0, 0)`},
	} {
		if _, err := runDialect(t, db, tt.dialect, tt.query); err == nil {
			t.Errorf("%v: %s answered a value; the engine refuses it", tt.dialect, tt.query)
		}
	}
}

// TestLikeEscapeClauseIsLeftToSQLite keeps a pattern with a custom escape
// character on SQLite's own LIKE, which the helpers do not model.
func TestLikeEscapeClauseIsLeftToSQLite(t *testing.T) {
	t.Parallel()

	got, err := Translate(dialects.PostgreSQL, `SELECT * FROM t WHERE a LIKE 'x!%' ESCAPE '!'`)
	if err != nil {
		t.Fatalf("Translate: %v", err)
	}
	if !strings.Contains(got, "LIKE") || strings.Contains(got, "like_sensitive") {
		t.Fatalf("Translate = %q, want the native LIKE kept", got)
	}
}

func TestLikeMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pattern, subject string
		want             bool
	}{
		{"", "", true},
		{"", "a", false},
		{"%", "", true},
		{"%", "anything", true},
		{"a%", "abc", true},
		{"a%", "bbc", false},
		{"%c", "abc", true},
		{"a_c", "abc", true},
		{"a_c", "ac", false},
		{"a%c", "abbbc", true},
		{"a%b%c", "axxbyyc", true},
		{"a%b%c", "axxbyy", false},
		{"%%%%%%b", "aaaaaaaaab", true},
		{"%%%%%%b", "aaaaaaaaac", false},
	}
	for _, tt := range tests {
		if got := likeMatch([]rune(tt.pattern), []rune(tt.subject)); got != tt.want {
			t.Fatalf("likeMatch(%q, %q) = %v, want %v", tt.pattern, tt.subject, got, tt.want)
		}
	}
}

// TestOperatorNullHandling verifies that a NULL operand propagates rather than
// being coerced by the helpers.
func TestOperatorNullHandling(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	for _, q := range []string{
		`SELECT like_sensitive(NULL, 'a')`,
		`SELECT like_insensitive('a', NULL)`,
		`SELECT mysql_divide(NULL, 1)`,
		`SELECT mysql_divide(1, NULL)`,
		`SELECT mysql_hex(NULL)`,
		`SELECT mysql_unhex(NULL)`,
		`SELECT mysql_unhex('not hex')`,
	} {
		var got sql.NullString
		if err := db.QueryRowContext(context.Background(), q).Scan(&got); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if got.Valid {
			t.Fatalf("%s = %q, want NULL", q, got.String)
		}
	}
}

// TestOperandMustBePrimary reports a missing operand instead of emitting SQL
// that would mean something else.
func TestOperandMustBePrimary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		dialect dialects.Dialect
		query   string
	}{
		{dialects.MySQL, "SELECT 2 /"},
		{dialects.PostgreSQL, "SELECT a LIKE"},
		{dialects.PostgreSQL, "SELECT 2 ^"},
	}
	for _, tt := range tests {
		if _, err := Translate(tt.dialect, tt.query); err == nil {
			t.Fatalf("Translate(%s, %q) should fail", tt.dialect, tt.query)
		}
	}
}

// TestWindowFunctionOperands keeps a window or filter clause attached to the
// aggregate it modifies. The operand scanners walk back from an operator to the
// primary expression beside it, and a bare "SUM(x) OVER (...)" ends in the
// clause's own parentheses, which is easy to mistake for the whole operand.
func TestWindowFunctionOperands(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	if _, err := db.ExecContext(context.Background(), `CREATE TABLE w (id INTEGER, score INTEGER)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `INSERT INTO w VALUES (1, 10), (2, 20), (3, 30)`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	tests := []struct {
		name    string
		dialect dialects.Dialect
		query   string
		want    string
	}{
		{"mysql divides a windowed sum", dialects.MySQL, `SELECT SUM(score) OVER (ORDER BY id) / 2 FROM w ORDER BY id LIMIT 1`, "5"},
		{"mysql divides by a windowed count", dialects.MySQL, `SELECT score / COUNT(*) OVER () FROM w ORDER BY id LIMIT 1`, "3.3333333333333335"},
		{"googlesql divides a windowed sum", dialects.GoogleSQL, `SELECT SUM(score) OVER (ORDER BY id) / 2 FROM w ORDER BY id LIMIT 1`, "5"},
		{"mysql divides a named window", dialects.MySQL, `SELECT SUM(score) OVER win / 2 FROM w WINDOW win AS (ORDER BY id) ORDER BY id LIMIT 1`, "5"},
		{"mysql divides a filtered aggregate", dialects.MySQL, `SELECT COUNT(*) FILTER (WHERE id > 1) / 2 FROM w`, "1"},
		{"postgresql matches a windowed value", dialects.PostgreSQL, `SELECT CAST(SUM(score) OVER (ORDER BY id) AS TEXT) LIKE '1%' FROM w ORDER BY id LIMIT 1`, "1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runDialect(t, db, tt.dialect, tt.query)
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			if !got.Valid || got.String != tt.want {
				t.Fatalf("%s = %v, want %q", tt.query, got, tt.want)
			}
		})
	}
}

// TestStrictConcatSemantics runs CONCAT through the real driver for every
// dialect. MySQL and GoogleSQL propagate a NULL argument to a NULL result;
// PostgreSQL's concat() ignores NULLs, and SQLite's does too, so only the first
// two are routed to the helper. Passing all of them through to SQLite answered a
// plausible non-NULL string where MySQL and BigQuery answer NULL, with nothing
// to notice.
func TestStrictConcatSemantics(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		name    string
		dialect dialects.Dialect
		query   string
		want    string
		null    bool
	}{
		{name: "mysql concat joins", dialect: dialects.MySQL, query: `SELECT CONCAT('a', 'b')`, want: "ab"},
		{name: "mysql concat with null is null", dialect: dialects.MySQL, query: `SELECT CONCAT('a', NULL)`, null: true},
		{name: "mysql concat null first", dialect: dialects.MySQL, query: `SELECT CONCAT(NULL, 'b')`, null: true},
		{name: "mysql concat single argument", dialect: dialects.MySQL, query: `SELECT CONCAT('a')`, want: "a"},
		{name: "mysql concat numbers", dialect: dialects.MySQL, query: `SELECT CONCAT(1, 2)`, want: "12"},
		{name: "mysql concat nested null", dialect: dialects.MySQL, query: `SELECT CONCAT('a', CONCAT('b', NULL))`, null: true},
		{name: "mysql concat_ws still skips null", dialect: dialects.MySQL, query: `SELECT CONCAT_WS(',', 'a', NULL, 'b')`, want: "a,b"},

		{name: "googlesql concat joins", dialect: dialects.GoogleSQL, query: `SELECT CONCAT('a', 'b')`, want: "ab"},
		{name: "googlesql concat with null is null", dialect: dialects.GoogleSQL, query: `SELECT CONCAT('a', NULL)`, null: true},

		// PostgreSQL's concat() ignores NULLs, so it must NOT be rewritten.
		{name: "postgresql concat ignores null", dialect: dialects.PostgreSQL, query: `SELECT CONCAT('a', NULL)`, want: "a"},
		{name: "sqlite concat ignores null", dialect: dialects.SQLite, query: `SELECT CONCAT('a', NULL)`, want: "a"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runDialect(t, db, tt.dialect, tt.query)
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			if tt.null {
				if got.Valid {
					t.Fatalf("%s = %q, want NULL", tt.query, got.String)
				}
				return
			}
			if !got.Valid || got.String != tt.want {
				t.Fatalf("%s = %v, want %q", tt.query, got, tt.want)
			}
		})
	}
}

// TestSimilarToRegexpEscapes covers the characters the SIMILAR TO translation
// has to protect from the regular expression it produces, and the escape a
// pattern can use to make % or _ mean itself.
func TestSimilarToRegexpEscapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
		want    string
	}{
		{name: "an anchor is escaped", pattern: "^a$", want: `^\^a\$$`},
		{name: "an escape keeps the character after it", pattern: `a\%b`, want: `^a\%b$`},
		{name: "a trailing escape escapes itself", pattern: `a\`, want: `^a\\$`},
		{name: "the regex parts pass through", pattern: "(a|b)+[0-9]{2}", want: "^(a|b)+[0-9]{2}$"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := similarToRegexp(tt.pattern); got != tt.want {
				t.Fatalf("similarToRegexp(%q) = %q, want %q", tt.pattern, got, tt.want)
			}
		})
	}
}

// TestPredicatesAndOperatorsWithoutASQLiteSpelling pins the constructs that
// reached SQLite unchanged and failed there naming something the caller had not
// written: the truth-value predicate, the quantified comparisons, PostgreSQL's
// LIKE operator aliases and its two remaining bit operators, and the COLLATE
// clause. Every expected value was read from mysql:8.4 or postgres:17-alpine.
func TestPredicatesAndOperatorsWithoutASQLiteSpelling(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		dialect dialects.Dialect
		query   string
		want    string
	}{
		// IS UNKNOWN is IS NULL; IS TRUE and IS FALSE SQLite already takes.
		{dialects.PostgreSQL, `SELECT NULL IS UNKNOWN`, "1"},
		{dialects.PostgreSQL, `SELECT 1 IS NOT UNKNOWN`, "1"},
		{dialects.MySQL, `SELECT NULL IS UNKNOWN`, "1"},
		{dialects.MySQL, `SELECT 1 IS TRUE`, "1"},
		{dialects.MySQL, `SELECT 0 IS FALSE`, "1"},

		// "= ANY" and "= SOME" are IN, and "<> ALL" is NOT IN.
		{dialects.PostgreSQL, `SELECT 1 = ANY (SELECT 1)`, "1"},
		{dialects.PostgreSQL, `SELECT 1 = SOME (SELECT 1)`, "1"},
		{dialects.PostgreSQL, `SELECT 1 <> ALL (SELECT 2)`, "1"},
		{dialects.MySQL, `SELECT 1 = ANY (SELECT 1)`, "1"},

		// The LIKE operator aliases are LIKE, not the regex operators they
		// start with: the pattern is written with LIKE wildcards.
		{dialects.PostgreSQL, `SELECT 'abc' ~~ 'a%'`, "1"},
		{dialects.PostgreSQL, `SELECT 'abc' !~~ 'a%'`, "0"},
		{dialects.PostgreSQL, `SELECT 'abc' ~~* 'A%'`, "1"},
		{dialects.PostgreSQL, `SELECT 'abc' !~~* 'A%'`, "0"},
		{dialects.PostgreSQL, `SELECT 'abc' ~ 'a.c'`, "1"},
		{dialects.PostgreSQL, `SELECT 'ABC' ~* 'a.c'`, "1"},
		{dialects.PostgreSQL, `SELECT 'abc' !~ '^z'`, "1"},

		// "#" is the bitwise XOR and a leading "~" the bitwise NOT.
		{dialects.PostgreSQL, `SELECT 5 # 3`, "6"},
		{dialects.PostgreSQL, `SELECT 1 # 2 # 3`, "0"},
		{dialects.PostgreSQL, `SELECT ~5`, "-6"},
		{dialects.PostgreSQL, `SELECT ~0`, "-1"},
		// The JSON operators that start with the same characters are one token
		// each, so the XOR pass does not take the "#" of "#>" and leave the ">".
		{dialects.PostgreSQL, `SELECT '{"a":1}'::json -> 'a'`, "1"},

		// A COLLATE clause reaches the SQLite collation that means the same.
		{dialects.PostgreSQL, `SELECT ('a' COLLATE "C" < 'B')`, "0"},
		{dialects.MySQL, `SELECT ('a' COLLATE utf8mb4_bin < 'B')`, "0"},
		{dialects.MySQL, `SELECT ('a' COLLATE utf8mb4_general_ci = 'A')`, "1"},
	}
	for _, tt := range tests {
		got, err := runDialect(t, db, tt.dialect, tt.query)
		if err != nil {
			t.Errorf("%v: %s: %v", tt.dialect, tt.query, err)
			continue
		}
		if !got.Valid || got.String != tt.want {
			t.Errorf("%v: %s = %v, want %q", tt.dialect, tt.query, got, tt.want)
		}
	}

	// A quantified comparison with no short SQLite form, and a collation whose
	// order this package cannot reproduce, are refused by name rather than
	// left to SQLite's parser.
	refused := []struct {
		dialect dialects.Dialect
		query   string
	}{
		{dialects.PostgreSQL, `SELECT 1 = ALL (SELECT 1)`},
		{dialects.PostgreSQL, `SELECT 1 > ALL (SELECT 2)`},
		{dialects.PostgreSQL, `SELECT 'a' COLLATE "en_US"`},
		{dialects.MySQL, `SELECT 'a' COLLATE nosuch`},
	}
	for _, tt := range refused {
		if _, err := Translate(tt.dialect, tt.query); !errors.Is(err, sqlerr.ErrUnsupportedSyntax) {
			t.Errorf("Translate(%v, %q) error = %v, want ErrUnsupportedSyntax", tt.dialect, tt.query, err)
		}
	}

	// "#>" and "@>" are single tokens rather than an operator beside a
	// comparison, so each is refused as the one unsupported operator it is
	// rather than reaching SQLite as a mangled expression.
	for _, query := range []string{
		`SELECT '{"a":{"b":1}}'::jsonb #> '{a,b}'`,
		`SELECT '{"a":1}'::jsonb @> '{"a":1}'`,
	} {
		if _, err := Translate(dialects.PostgreSQL, query); !errors.Is(err, sqlerr.ErrUnsupportedSyntax) {
			t.Errorf("Translate(postgresql, %q) error = %v, want ErrUnsupportedSyntax", query, err)
		}
	}

	// The words these passes look for are not always keywords.
	kept := []struct {
		dialect dialects.Dialect
		query   string
	}{
		{dialects.PostgreSQL, "SELECT ALL a FROM t"},
		{dialects.PostgreSQL, "SELECT COUNT(ALL a) FROM t"},
		{dialects.MySQL, "SELECT unknown FROM t"},
		{dialects.MySQL, "SELECT `any` FROM t"},
		{dialects.MySQL, "SELECT 'IS UNKNOWN' FROM t"},
	}
	for _, tt := range kept {
		if _, err := Translate(tt.dialect, tt.query); err != nil {
			t.Errorf("Translate(%v, %q): %v", tt.dialect, tt.query, err)
		}
	}
}

func TestSimilarToRegexp(t *testing.T) {
	t.Parallel()

	tests := []struct{ in, want string }{
		{"a%", "^a.*$"},
		{"a_c", "^a.c$"},
		{"a.c", `^a\.c$`},
		{"(a|b)", "^(a|b)$"},
		{"a+", "^a+$"},
	}
	for _, tt := range tests {
		if got := similarToRegexp(tt.in); got != tt.want {
			t.Fatalf("similarToRegexp(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// TestOverlayBoundaries covers the OVERLAY offsets that fall outside the target.
func TestOverlayBoundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		args []driver.Value
		want string
	}{
		{[]driver.Value{"abc", "X", int64(1)}, "Xbc"},
		{[]driver.Value{"abc", "X", int64(9)}, "abcX"},
		{[]driver.Value{"abc", "XY", int64(2), int64(0)}, "aXYbc"},
		// PostgreSQL defines the call as substring(s from 1 for start-1) ||
		// replacement || substring(s from start+count), so a negative count
		// makes the tail begin before the overlaid position and part of the
		// string is repeated. Checked against PostgreSQL 17.
		{[]driver.Value{"abc", "XY", int64(2), int64(-1)}, "aXYabc"},
		{[]driver.Value{"abc", "XY", int64(2), int64(99)}, "aXY"},
		// A count near math.MaxInt64 is an ordinary SQLite integer literal and
		// must not wrap into a negative slice bound.
		{[]driver.Value{"ab", "X", int64(2), int64(9223372036854775807)}, "aX"},
		{[]driver.Value{"ab", "X", int64(9223372036854775807)}, "abX"},
	}
	for _, tt := range tests {
		got, err := fnOverlay(tt.args)
		if err != nil {
			t.Fatalf("fnOverlay(%v): %v", tt.args, err)
		}
		if got != tt.want {
			t.Fatalf("fnOverlay(%v) = %v, want %q", tt.args, got, tt.want)
		}
	}
	if _, err := fnOverlay([]driver.Value{"abc", "X"}); err == nil {
		t.Fatal("fnOverlay with 2 arguments should fail")
	}
	// A start below 1 makes the first substring's length negative, which is the
	// error PostgreSQL raises rather than an answer it gives.
	if _, err := fnOverlay([]driver.Value{"abc", "X", int64(0)}); err == nil {
		t.Fatal("fnOverlay at position 0 should fail, as PostgreSQL does")
	}
}
