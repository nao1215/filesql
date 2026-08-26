package dialect

import (
	"fmt"
	"math"
	"testing"
)

// TestMySQLOnlyFunctions pins the functions MySQL has and SQLite does not
// against MySQL 8.4. Every want below was read from a running MySQL 8.4.11
// rather than derived, because these are the functions whose edges are easiest
// to guess wrong: CONV stops at the first digit its base does not have, INSERT
// leaves the string alone for a position outside it, and INET_ATON reads a
// short address by filling the top bytes from the left.
func TestMySQLOnlyFunctions(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		query    string
		want     string
		wantNull bool
		wantErr  bool
	}{
		// spellings of functions this package already has
		{query: `SELECT LCASE('ABC')`, want: "abc"},
		{query: `SELECT LCASE('École')`, want: "école"},
		{query: `SELECT LCASE(NULL)`, wantNull: true},
		{query: `SELECT LCASE(123)`, want: "123"},
		{query: `SELECT UCASE('abc')`, want: "ABC"},
		{query: `SELECT UCASE('école')`, want: "ÉCOLE"},
		{query: `SELECT MID('abcdef', 2, 3)`, want: "bcd"},
		{query: `SELECT MID('abcdef', -2, 2)`, want: "ef"},
		{query: `SELECT MID('abcdef', 0, 3)`, want: ""},
		{query: `SELECT MID('abcdef', 2)`, want: "bcdef"},
		{query: `SELECT MID('日本語', 2, 1)`, want: "本"},
		{query: `SELECT DAYOFMONTH('2024-02-29')`, want: "29"},
		{query: `SELECT DAYOFMONTH('not-a-date')`, wantNull: true},
		{query: `SELECT ISNULL(NULL)`, want: "1"},
		{query: `SELECT ISNULL(1)`, want: "0"},
		{query: `SELECT ISNULL('')`, want: "0"},

		// STRCMP
		{query: `SELECT STRCMP('a', 'b')`, want: "-1"},
		{query: `SELECT STRCMP('b', 'a')`, want: "1"},
		{query: `SELECT STRCMP('a', 'A')`, want: "0"},
		{query: `SELECT STRCMP('a', 'ab')`, want: "-1"},
		{query: `SELECT STRCMP('', '')`, want: "0"},
		{query: `SELECT STRCMP(NULL, 'a')`, wantNull: true},
		{query: `SELECT STRCMP('a', NULL)`, wantNull: true},
		{query: `SELECT STRCMP(1, '1')`, want: "0"},
		{query: `SELECT STRCMP(2, 10)`, want: "1"},
		{query: `SELECT STRCMP('2', '10')`, want: "1"},
		{query: `SELECT STRCMP('日', '本')`, want: "-1"},

		// BIT_LENGTH
		{query: `SELECT BIT_LENGTH('abc')`, want: "24"},
		{query: `SELECT BIT_LENGTH('日本')`, want: "48"},
		{query: `SELECT BIT_LENGTH('')`, want: "0"},
		{query: `SELECT BIT_LENGTH(255)`, want: "24"},
		{query: `SELECT BIT_LENGTH(NULL)`, wantNull: true},

		// CONV, BIN and OCT
		{query: `SELECT CONV(15, 10, 2)`, want: "1111"},
		{query: `SELECT CONV('ff', 16, 10)`, want: "255"},
		{query: `SELECT CONV('ff', 16, 16)`, want: "FF"},
		{query: `SELECT CONV(-15, 10, 16)`, want: "FFFFFFFFFFFFFFF1"},
		{query: `SELECT CONV(15, 10, -2)`, want: "1111"},
		{query: `SELECT CONV(-15, 10, -2)`, want: "-1111"},
		{query: `SELECT CONV('7fffffffffffffff', 16, 10)`, want: "9223372036854775807"},
		{query: `SELECT CONV('ffffffffffffffff', 16, 10)`, want: "18446744073709551615"},
		{query: `SELECT CONV('xyz', 16, 10)`, want: "0"},
		{query: `SELECT CONV('zz', 36, 10)`, want: "1295"},
		{query: `SELECT CONV('zz', 37, 10)`, wantNull: true},
		{query: `SELECT CONV('zz', 36, 37)`, wantNull: true},
		{query: `SELECT CONV('zz', 1, 10)`, wantNull: true},
		{query: `SELECT CONV('', 16, 10)`, wantNull: true},
		{query: `SELECT CONV(NULL, 10, 2)`, wantNull: true},
		{query: `SELECT CONV('0x1f', 16, 10)`, want: "0"},
		{query: `SELECT CONV('12abc', 10, 10)`, want: "12"},
		{query: `SELECT CONV(' 12', 10, 10)`, want: "12"},
		{query: `SELECT CONV('-ff', 16, 10)`, want: "18446744073709551361"},
		{query: `SELECT BIN(12)`, want: "1100"},
		{query: `SELECT BIN(-1)`, want: "1111111111111111111111111111111111111111111111111111111111111111"},
		{query: `SELECT BIN(0)`, want: "0"},
		{query: `SELECT BIN(NULL)`, wantNull: true},
		{query: `SELECT OCT(12)`, want: "14"},
		{query: `SELECT OCT(-1)`, want: "1777777777777777777777"},

		// CRC32
		{query: `SELECT CRC32('MySQL')`, want: "3259397556"},
		{query: `SELECT CRC32('')`, want: "0"},
		{query: `SELECT CRC32(123)`, want: "2286445522"},
		{query: `SELECT CRC32(NULL)`, wantNull: true},

		// base 64
		{query: `SELECT TO_BASE64('abc')`, want: "YWJj"},
		{query: `SELECT TO_BASE64('')`, want: ""},
		{query: `SELECT TO_BASE64(123)`, want: "MTIz"},
		{query: `SELECT TO_BASE64(NULL)`, wantNull: true},
		{query: `SELECT TO_BASE64('abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij')`, want: "YWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXowMTIzNDU2Nzg5QUJDREVGR0hJSktMTU5PUFFSU1RV\nVldYWVphYmNkZWZnaGlq"},
		{query: `SELECT FROM_BASE64('YWJj')`, want: "abc"},
		{query: `SELECT FROM_BASE64('!!')`, wantNull: true},
		{query: `SELECT FROM_BASE64('')`, want: ""},
		{query: `SELECT FROM_BASE64(NULL)`, wantNull: true},
		{query: `SELECT INSERT('Quadratic', 3, 4, 'What')`, want: "QuWhattic"},

		// INSERT
		{query: `SELECT INSERT('Quadratic', -1, 4, 'What')`, want: "Quadratic"},
		{query: `SELECT INSERT('Quadratic', 0, 4, 'What')`, want: "Quadratic"},
		{query: `SELECT INSERT('Quadratic', 3, 100, 'What')`, want: "QuWhat"},
		{query: `SELECT INSERT('abc', 2, -1, 'X')`, want: "aX"},
		{query: `SELECT INSERT('abc', 4, 1, 'X')`, want: "abc"},
		{query: `SELECT INSERT('abc', 5, 1, 'X')`, want: "abc"},
		{query: `SELECT INSERT('日本語', 2, 1, 'X')`, want: "日X語"},
		{query: `SELECT INSERT(NULL, 1, 1, 'X')`, wantNull: true},
		{query: `SELECT INSERT('abc', 1, 1, NULL)`, wantNull: true},
		{query: `SELECT BIT_COUNT(29)`, want: "4"},

		// BIT_COUNT, COT and the two-argument ATAN
		{query: `SELECT BIT_COUNT(-1)`, want: "64"},
		{query: `SELECT BIT_COUNT(0)`, want: "0"},
		{query: `SELECT BIT_COUNT(NULL)`, wantNull: true},
		{query: `SELECT ATAN(1, 1)`, want: "0.7853981633974483"},
		{query: `SELECT ATAN(1, 0)`, want: "1.5707963267948966"},
		{query: `SELECT ATAN(-1, -1)`, want: "-2.356194490192345"},
		{query: `SELECT ATAN(1)`, want: "0.7853981633974483"},
		{query: `SELECT REGEXP_LIKE('abc', '^a')`, want: "1"},

		// the REGEXP family
		{query: `SELECT REGEXP_LIKE('abc', 'B')`, want: "1"},
		{query: `SELECT REGEXP_LIKE('abc', 'B', 'c')`, want: "0"},
		{query: `SELECT REGEXP_LIKE('abc', 'B', 'i')`, want: "1"},
		{query: `SELECT REGEXP_LIKE(NULL, 'a')`, wantNull: true},
		{query: `SELECT REGEXP_SUBSTR('abc def', '[a-z]+')`, want: "abc"},
		{query: `SELECT REGEXP_SUBSTR('abc def', '[a-z]+', 5)`, want: "def"},
		{query: `SELECT REGEXP_SUBSTR('abc def ghi', '[a-z]+', 1, 2)`, want: "def"},
		{query: `SELECT REGEXP_SUBSTR('aAbB', '[ab]+')`, want: "aAbB"},
		{query: `SELECT REGEXP_SUBSTR('abc', 'z')`, wantNull: true},
		{query: `SELECT REGEXP_SUBSTR('日本語です', '[本語]+')`, want: "本語"},
		{query: `SELECT REGEXP_SUBSTR('abc', 'B', 1, 1, 'c')`, wantNull: true},
		{query: `SELECT REGEXP_INSTR('abc def', 'def')`, want: "5"},
		{query: `SELECT REGEXP_INSTR('abc def', 'z')`, want: "0"},
		{query: `SELECT REGEXP_INSTR('abc def ghi', '[a-z]+', 1, 2)`, want: "5"},
		{query: `SELECT REGEXP_INSTR('abc def', 'def', 1, 1, 1)`, want: "8"},
		{query: `SELECT REGEXP_INSTR('日本語です', 'です')`, want: "4"},
		{query: `SELECT REGEXP_INSTR('aAbB', 'B')`, want: "3"},
		{query: `SELECT SHA1('abc')`, want: "a9993e364706816aba3e25717850c26c9cd0d89d"},
		{query: `SELECT SHA1('')`, want: "da39a3ee5e6b4b0d3255bfef95601890afd80709"},
		{query: `SELECT SHA1(NULL)`, wantNull: true},

		// the digests
		{query: `SELECT SHA2('abc', 224)`, want: "23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7"},
		{query: `SELECT SHA2('abc', 256)`, want: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
		{query: `SELECT SHA2('abc', 384)`, want: "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7"},
		{query: `SELECT SHA2('abc', 512)`, want: "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"},
		{query: `SELECT SHA2('abc', 0)`, want: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
		{query: `SELECT SHA2('abc', 123)`, wantNull: true},
		{query: `SELECT SHA2(NULL, 256)`, wantNull: true},
		{query: `SELECT INET_ATON('10.0.0.1')`, want: "167772161"},
		{query: `SELECT INET_ATON('1.2.3')`, want: "16908291"},
		{query: `SELECT INET_ATON('1.2')`, want: "16777218"},

		// addresses
		{query: `SELECT INET_ATON('1')`, want: "1"},
		{query: `SELECT INET_ATON('255.255.255.255')`, want: "4294967295"},
		{query: `SELECT INET_ATON('1.2.3.4.5')`, wantNull: true},
		{query: `SELECT INET_ATON('1.2.3.256')`, wantNull: true},
		{query: `SELECT INET_ATON('')`, wantNull: true},
		{query: `SELECT INET_ATON(NULL)`, wantNull: true},
		{query: `SELECT INET_NTOA(0)`, want: "0.0.0.0"},
		{query: `SELECT INET_NTOA(3232235777)`, want: "192.168.1.1"},
		{query: `SELECT INET_NTOA(4294967295)`, want: "255.255.255.255"},
		{query: `SELECT INET_NTOA(-1)`, wantNull: true},
		{query: `SELECT INET_NTOA(NULL)`, wantNull: true},
		{query: `SELECT IS_IPV4('1.2.3.4')`, want: "1"},
		{query: `SELECT IS_IPV4('1.2.3.256')`, want: "0"},
		{query: `SELECT IS_IPV4('1.2.3')`, want: "0"},
		{query: `SELECT IS_IPV4('01.2.3.4')`, want: "1"},
		{query: `SELECT IS_IPV4(NULL)`, wantNull: true},
		{query: `SELECT IS_IPV6('::1')`, want: "1"},
		{query: `SELECT IS_IPV6('1.2.3.4')`, want: "0"},
		{query: `SELECT IS_IPV6('fe80::1%eth0')`, want: "0"},
		{query: `SELECT IS_IPV6(NULL)`, wantNull: true},

		// The bases and widths these functions refuse.
		{query: `SELECT CONV('zz', 37, 10)`, wantNull: true},
		{query: `SELECT CONV('zz', 36, 37)`, wantNull: true},
		{query: `SELECT SHA2('abc', 225)`, wantNull: true},
		{query: `SELECT REGEXP_INSTR('abc', 'b', 0)`, wantErr: true},
		{query: `SELECT REGEXP_SUBSTR('abc', 'b', 5)`, wantErr: true},
		{query: `SELECT REGEXP_INSTR('abc', 'b', 1, 1, 2)`, wantErr: true},
		{query: `SELECT COT(0)`, wantErr: true},
		{query: `SELECT ISNULL(1, 2)`, wantErr: true},
		{query: `SELECT REGEXP_LIKE('a')`, wantErr: true},
		{query: `SELECT REGEXP_SUBSTR('a')`, wantErr: true},
		{query: `SELECT REGEXP_INSTR('a')`, wantErr: true},
		{query: `SELECT REGEXP_LIKE('a', 'a', 'a', 'a')`, wantErr: true},
		{query: `SELECT REGEXP_LIKE('a', '(')`, wantErr: true},
		{query: `SELECT REGEXP_SUBSTR('a', '(')`, wantErr: true},
		{query: `SELECT REGEXP_LIKE('a', 'a', 'z')`, wantErr: true},

		// NULL reaches every argument of the ones that take more than one.
		{query: `SELECT REGEXP_LIKE('a', NULL)`, wantNull: true},
		{query: `SELECT REGEXP_LIKE('a', 'a', NULL)`, wantNull: true},
		{query: `SELECT REGEXP_SUBSTR('a', 'a', NULL)`, wantNull: true},
		{query: `SELECT REGEXP_SUBSTR('a', 'a', 1, NULL)`, wantNull: true},
		{query: `SELECT REGEXP_INSTR('a', 'a', 1, 1, NULL)`, wantNull: true},
		{query: `SELECT CONV('ff', NULL, 10)`, wantNull: true},
		{query: `SELECT SHA2('abc', NULL)`, wantNull: true},
		{query: `SELECT INSERT('abc', NULL, 1, 'X')`, wantNull: true},
		{query: `SELECT MID(NULL, 1, 1)`, wantNull: true},
		{query: `SELECT COT(NULL)`, wantNull: true},

		// The other half of the digit table, and the saturation above it.
		{query: `SELECT CONV('FF', 16, 10)`, want: "255"},
		{query: `SELECT CONV('Zz', 36, 10)`, want: "1295"},
		{query: `SELECT CONV('ffffffffffffffffff', 16, 10)`, want: "18446744073709551615"},
		{query: `SELECT CONV('+15', 10, 2)`, want: "1111"},
		{query: `SELECT IS_IPV4('1.2.3.4444')`, want: "0"},
		{query: `SELECT IS_IPV4('1.2.3.-4')`, want: "0"},
		{query: `SELECT INET_ATON('1.2.3.-4')`, wantNull: true},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			got, err := runDialect(t, db, MySQL, tt.query)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("%s: expected an error, got %q", tt.query, got.String)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			if got.Valid == tt.wantNull {
				t.Fatalf("%s returned valid=%v (%q), want null=%v", tt.query, got.Valid, got.String, tt.wantNull)
			}
			if tt.wantNull {
				return
			}
			if got.String != tt.want {
				t.Errorf("%s = %q, want %q", tt.query, got.String, tt.want)
			}
		})
	}
}

// TestMySQLOnlyFunctionRelations checks the relations these functions promise,
// rather than one more fixed value each: an alias answers what it is a name
// for, and a pair that inverts each other round-trips. A fixed case pins the
// answer that was measured; a relation catches the alias that drifts away from
// the function beside it.
func TestMySQLOnlyFunctionRelations(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	aliases := []struct {
		alias string
		of    string
	}{
		{`LCASE(x)`, `LOWER(x)`},
		{`UCASE(x)`, `UPPER(x)`},
		{`MID(x, 2, 3)`, `SUBSTRING(x, 2, 3)`},
		{`MID(x, -2)`, `SUBSTRING(x, -2)`},
		{`BIN(LENGTH(x))`, `CONV(LENGTH(x), 10, 2)`},
		{`OCT(LENGTH(x))`, `CONV(LENGTH(x), 10, 8)`},
		{`BIT_LENGTH(x)`, `8 * LENGTH(x)`},
	}
	values := []string{`'abc'`, `'École'`, `'日本語'`, `''`, `'0'`}
	for _, a := range aliases {
		for _, v := range values {
			query := "SELECT " + a.alias + " = " + a.of + " FROM (SELECT " + v + " AS x)"
			t.Run(a.alias+" over "+v, func(t *testing.T) {
				got, err := runDialect(t, db, MySQL, query)
				if err != nil {
					t.Fatalf("%s: %v", query, err)
				}
				if got.String != "1" {
					t.Errorf("%s = %q, want 1", query, got.String)
				}
			})
		}
	}

	// DAYOFMONTH is the alias of a function that answers NULL for a value it
	// cannot read, so it is checked with IS rather than with equality.
	for _, v := range []string{`'2024-02-29'`, `'not-a-date'`} {
		query := "SELECT DAYOFMONTH(" + v + ") IS DAY(" + v + ")"
		t.Run("DAYOFMONTH over "+v, func(t *testing.T) {
			got, err := runDialect(t, db, MySQL, query)
			if err != nil {
				t.Fatalf("%s: %v", query, err)
			}
			if got.String != "1" {
				t.Errorf("%s = %q, want 1", query, got.String)
			}
		})
	}

	roundTrips := []struct {
		name  string
		query string
	}{
		{"base 64", `SELECT FROM_BASE64(TO_BASE64(x)) = x FROM (SELECT 'abc' AS x)`},
		{"base 64 over a wrapped line", `SELECT FROM_BASE64(TO_BASE64(x)) = x FROM (SELECT REPEAT('ab', 100) AS x)`},
		{"base 64 of an empty string", `SELECT FROM_BASE64(TO_BASE64(x)) = x FROM (SELECT '' AS x)`},
		{"conv", `SELECT CONV(CONV(x, 10, 36), 36, 10) = x FROM (SELECT '1234567890' AS x)`},
		{"address", `SELECT INET_NTOA(INET_ATON(x)) = x FROM (SELECT '192.168.1.1' AS x)`},
		{"address at the top of the range", `SELECT INET_NTOA(INET_ATON(x)) = x FROM (SELECT '255.255.255.255' AS x)`},
		{"insert replaces what it removes", `SELECT INSERT(x, 2, 3, SUBSTRING(x, 2, 3)) = x FROM (SELECT 'abcdef' AS x)`},
	}
	for _, rt := range roundTrips {
		t.Run(rt.name, func(t *testing.T) {
			got, err := runDialect(t, db, MySQL, rt.query)
			if err != nil {
				t.Fatalf("%s: %v", rt.query, err)
			}
			if got.String != "1" {
				t.Errorf("%s = %q, want 1", rt.query, got.String)
			}
		})
	}
}

// TestCotIsTheReciprocalOfTheTangent checks COT to the precision MySQL and Go
// agree to. The two can differ in the last bit, because Go's tangent and the C
// library's round differently, so the value is compared with a tolerance rather
// than pinned like the rest.
func TestCotIsTheReciprocalOfTheTangent(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	for _, tt := range []struct {
		query string
		want  float64
	}{
		{`SELECT COT(1)`, 0.6420926159343306},
		{`SELECT COT(-1)`, -0.6420926159343306},
		{`SELECT COT(0.5)`, 1.830487721712452},
	} {
		t.Run(tt.query, func(t *testing.T) {
			got, err := runDialect(t, db, MySQL, tt.query)
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			var f float64
			if _, err := fmt.Sscanf(got.String, "%g", &f); err != nil {
				t.Fatalf("%s = %q: %v", tt.query, got.String, err)
			}
			if math.Abs(f-tt.want) > 1e-15 {
				t.Errorf("%s = %v, want %v", tt.query, f, tt.want)
			}
		})
	}
}
