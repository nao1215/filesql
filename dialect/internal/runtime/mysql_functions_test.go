package runtime

import (
	"fmt"
	"math"
	"testing"

	"github.com/nao1215/filesql/dialect/internal/dialects"
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
		{query: `SELECT INSERT('abc', 2, 9223372036854775807, 'X')`, want: "aX"},
		{query: `SELECT INSERT('abc', 2, -9223372036854775808, 'X')`, want: "aX"},
		// An occurrence past the number of matches has none. MySQL 8.4 answers
		// the first match for an occurrence at or above 2^32, where its own
		// counter wraps, and NULL for every value below that; the wrap is not a
		// behavior worth copying, so these follow the answer MySQL gives for
		// 2147483647 rather than the one it gives for 4294967296.
		{query: `SELECT REGEXP_SUBSTR('abc', 'b', 1, 9223372036854775807)`, wantNull: true},
		{query: `SELECT REGEXP_INSTR('abc', 'b', 1, 9223372036854775807)`, want: "0"},
		{query: `SELECT REGEXP_SUBSTR('abc', 'b', 1, 3)`, wantNull: true},
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
			got, err := runDialect(t, db, dialects.MySQL, tt.query)
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
// TestMySQLValueRulesMatchTheEngine pins the helpers whose answer used to be
// SQLite's rather than MySQL's for a value at the edge of what they take. Every
// expected value here was read from mysql:8.4.
func TestMySQLValueRulesMatchTheEngine(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		query    string
		want     string
		wantNull bool
	}{
		// A count reads a string as the number its leading run spells and
		// truncates it, so a value from a text column counts rather than
		// making the whole call NULL.
		{query: `SELECT LENGTH(SPACE('12.5'))`, want: "12"},
		{query: `SELECT LENGTH(SPACE(12.5))`, want: "13"},
		{query: `SELECT LENGTH(SPACE('hello'))`, want: "0"},
		{query: `SELECT LENGTH(SPACE(3.7))`, want: "4"},
		{query: `SELECT SPACE(NULL)`, wantNull: true},
		{query: `SELECT REPEAT('ab', '2.5')`, want: "abab"},
		{query: `SELECT REPEAT('ab', 'x')`, want: ""},
		{query: `SELECT LEFT('hello', '2.7')`, want: "he"},
		{query: `SELECT RIGHT('hello', '2.7')`, want: "lo"},
		{query: `SELECT LEFT('hello', 2.7)`, want: "hel"},
		{query: `SELECT LPAD('a', '3.7', '-')`, want: "--a"},
		{query: `SELECT SUBSTRING('hello', '2', '2.7')`, want: "el"},

		// SOUNDEX propagates NULL, answers the empty string for a value with no
		// letter in it, emits a digit per coded consonant however many there
		// are, and keeps a first letter it has no code for as it was written.
		{query: `SELECT SOUNDEX(NULL)`, wantNull: true},
		{query: `SELECT SOUNDEX('')`, want: ""},
		{query: `SELECT SOUNDEX('0')`, want: ""},
		{query: `SELECT SOUNDEX('Hello World')`, want: "H4643"},
		{query: `SELECT SOUNDEX('Robert')`, want: "R163"},
		{query: `SELECT SOUNDEX('Rupert')`, want: "R163"},
		{query: `SELECT SOUNDEX('Ashcraft')`, want: "A2613"},
		{query: `SELECT SOUNDEX('Tymczak')`, want: "T520"},
		{query: `SELECT SOUNDEX('Pfister')`, want: "P236"},
		{query: `SELECT SOUNDEX('Honeyman')`, want: "H500"},
		{query: `SELECT SOUNDEX('Wright')`, want: "W623"},
		{query: `SELECT SOUNDEX('  bob')`, want: "B000"},
		{query: `SELECT SOUNDEX('123abc')`, want: "A120"},
		{query: `SELECT SOUNDEX('éèê')`, want: "é000"},
		{query: `SELECT SOUNDEX('abcdefghijklmnopqrstuvwxyz')`, want: "A12312451262312"},

		// UNHEX reads an odd digit count as having a leading zero, and answers
		// bytes rather than text so a zero byte in them survives.
		{query: `SELECT HEX(UNHEX('ABC'))`, want: "0ABC"},
		{query: `SELECT HEX(UNHEX('0'))`, want: "00"},
		{query: `SELECT HEX(UNHEX('41'))`, want: "41"},
		{query: `SELECT HEX(UNHEX('0041'))`, want: "0041"},
		{query: `SELECT HEX(UNHEX('4100'))`, want: "4100"},
		{query: `SELECT UNHEX('zz')`, wantNull: true},

		// A NULL length makes the call NULL even where the position already put
		// the range outside the string.
		{query: `SELECT SUBSTRING('hello', 10, NULL)`, wantNull: true},
		{query: `SELECT SUBSTRING('hello', 0, NULL)`, wantNull: true},
		{query: `SELECT SUBSTRING('hello', -10, NULL)`, wantNull: true},
		{query: `SELECT SUBSTRING('hello', 2, NULL)`, wantNull: true},

		// REPLACE looks at every argument even when the search string is empty.
		{query: `SELECT REPLACE('hello', '', NULL)`, wantNull: true},
		{query: `SELECT REPLACE('hello', 'l', 'L')`, want: "heLLo"},

		// CHAR builds bytes, so a zero argument is one zero byte and an
		// argument past 255 is written big-endian rather than as UTF-8.
		{query: `SELECT HEX(CHAR(0))`, want: "00"},
		{query: `SELECT HEX(CHAR(65))`, want: "41"},
		{query: `SELECT HEX(CHAR(256))`, want: "0100"},
		{query: `SELECT HEX(CHAR(65536))`, want: "010000"},
		{query: `SELECT HEX(CHAR(65, 66, 67))`, want: "414243"},
		{query: `SELECT HEX(CHAR(65, NULL, 66))`, want: "4142"},

		// FORMAT groups an integer from its own digits rather than through a
		// float64, which lost the last digits of anything past 2^53.
		{query: `SELECT FORMAT(9223372036854775807, 0)`, want: "9,223,372,036,854,775,807"},
		{query: `SELECT FORMAT(9223372036854775807, 2)`, want: "9,223,372,036,854,775,807.00"},
		{query: `SELECT FORMAT(9007199254740993, 0)`, want: "9,007,199,254,740,993"},
		{query: `SELECT FORMAT(-9223372036854775808, 0)`, want: "-9,223,372,036,854,775,808"},
		{query: `SELECT FORMAT(1234.5678, 2)`, want: "1,234.57"},
		{query: `SELECT FORMAT(-1234567, 3)`, want: "-1,234,567.000"},

		// NULLIF compares the way MySQL's "=" does rather than the way SQLite
		// does: numerically as soon as either side is a number, where a string
		// that spells no number reads as zero, and by a case-folding collation
		// when both sides are strings.
		{query: `SELECT NULLIF('abc', 0)`, wantNull: true},
		{query: `SELECT NULLIF('', 0)`, wantNull: true},
		{query: `SELECT NULLIF(0, '')`, wantNull: true},
		{query: `SELECT NULLIF('abc', 0.0)`, wantNull: true},
		{query: `SELECT NULLIF('0x10', 0)`, wantNull: true},
		{query: `SELECT NULLIF('  1', 1)`, wantNull: true},
		{query: `SELECT NULLIF('1', 1)`, wantNull: true},
		{query: `SELECT NULLIF(1, '1')`, wantNull: true},
		{query: `SELECT NULLIF('1.0', 1)`, wantNull: true},
		{query: `SELECT NULLIF('1e3', 1000)`, wantNull: true},
		{query: `SELECT NULLIF(1.0, 1)`, wantNull: true},
		{query: `SELECT NULLIF(-0.0, 0)`, wantNull: true},
		{query: `SELECT NULLIF(0, 0)`, wantNull: true},
		{query: `SELECT NULLIF('abc', 'abc')`, wantNull: true},
		{query: `SELECT NULLIF('abc', 'ABC')`, wantNull: true},
		{query: `SELECT NULLIF('日本', '日本')`, wantNull: true},
		{query: `SELECT NULLIF('', '')`, wantNull: true},
		{query: `SELECT NULLIF(NULL, 1)`, wantNull: true},
		{query: `SELECT NULLIF('abc', 'abd')`, want: "abc"},
		{query: `SELECT NULLIF('00', '0')`, want: "00"},
		{query: `SELECT NULLIF('abc ', 'abc')`, want: "abc "},
		{query: `SELECT NULLIF('abc', NULL)`, want: "abc"},
		{query: `SELECT NULLIF(2, 10)`, want: "2"},
		{query: `SELECT NULLIF(9223372036854775807, 9223372036854775806)`, want: "9223372036854775807"},
	}

	for _, tt := range tests {
		got, err := runDialect(t, db, dialects.MySQL, tt.query)
		if err != nil {
			t.Errorf("%s: %v", tt.query, err)
			continue
		}
		if tt.wantNull {
			if got.Valid {
				t.Errorf("%s = %q, want NULL", tt.query, got.String)
			}
			continue
		}
		if !got.Valid || got.String != tt.want {
			t.Errorf("%s = %v, want %q", tt.query, got, tt.want)
		}
	}
}

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
				got, err := runDialect(t, db, dialects.MySQL, query)
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
			got, err := runDialect(t, db, dialects.MySQL, query)
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
			got, err := runDialect(t, db, dialects.MySQL, rt.query)
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
			got, err := runDialect(t, db, dialects.MySQL, tt.query)
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

// TestMySQLComparisonFoldsCaseLikeItsCollation pins GREATEST and LEAST against
// STRCMP, which already folds case because MySQL's default collation does. The
// three used to disagree about which of two strings is larger. JSON_TYPE is
// here for the same reason: SQLite spells its answer in lower case and MySQL in
// upper, so a query comparing it against the name MySQL's documentation prints
// matched nothing. Every value was read from mysql:8.4.
func TestMySQLComparisonFoldsCaseLikeItsCollation(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		query    string
		want     string
		wantNull bool
	}{
		{query: `SELECT GREATEST('a', 'B')`, want: "B"},
		{query: `SELECT LEAST('a', 'B')`, want: "a"},
		{query: `SELECT GREATEST('A', 'b')`, want: "b"},
		{query: `SELECT LEAST('A', 'b')`, want: "A"},
		{query: `SELECT STRCMP('a', 'B')`, want: "-1"},
		{query: `SELECT GREATEST(1, 2, 3)`, want: "3"},
		{query: `SELECT LEAST('a', 'a')`, want: "a"},
		{query: `SELECT GREATEST('a', NULL)`, wantNull: true},

		{query: `SELECT JSON_TYPE('{}')`, want: "OBJECT"},
		{query: `SELECT JSON_TYPE('[]')`, want: "ARRAY"},
		{query: `SELECT JSON_TYPE('1')`, want: "INTEGER"},
		{query: `SELECT JSON_TYPE('1.5')`, want: "DOUBLE"},
		{query: `SELECT JSON_TYPE('"s"')`, want: "STRING"},
		{query: `SELECT JSON_TYPE('true')`, want: "BOOLEAN"},
		{query: `SELECT JSON_TYPE('null')`, want: "NULL"},
		// A document built here still nests, which is what a renderer that
		// re-rendered the text at every level would have broken.
		{query: `SELECT JSON_ARRAY(1, JSON_OBJECT('b', 2))`, want: `[1,{"b":2}]`},
		// A boolean written as a literal is the JSON boolean, not the 1 SQLite
		// stores it as. Every want was read from mysql:8.4, allowing for the
		// space MySQL writes after a colon.
		{query: `SELECT JSON_ARRAY(TRUE)`, want: "[true]"},
		{query: `SELECT JSON_ARRAY(FALSE)`, want: "[false]"},
		{query: `SELECT JSON_ARRAY(TRUE, 1, 'a')`, want: `[true,1,"a"]`},
		{query: `SELECT JSON_OBJECT('a', TRUE)`, want: `{"a":true}`},
		{query: `SELECT JSON_ARRAY(JSON_OBJECT('a', TRUE))`, want: `[{"a":true}]`},
		{query: `SELECT JSON_SET('{}', '$.a', TRUE)`, want: `{"a":true}`},
		// The boundary: a boolean that is not a literal is the number SQLite
		// stores, since nothing downstream can tell the two apart.
		{query: `SELECT JSON_ARRAY(1 = 1)`, want: "[1]"},

		// The JSON functions that reached SQLite as unknown names. Every want
		// was read from mysql:8.4, allowing for the space MySQL writes after a
		// colon.
		{query: `SELECT JSON_VALUE('{"a":1}', '$.a')`, want: "1"},
		{query: `SELECT JSON_VALUE('{"a":"x"}', '$.a')`, want: "x"},
		{query: `SELECT JSON_VALUE('{"a":1}', '$.b') IS NULL`, want: "1"},
		{query: `SELECT JSON_MERGE_PATCH('{"a":1}', '{"b":2}')`, want: `{"a":1,"b":2}`},
		{query: `SELECT JSON_MERGE_PATCH('{"a":1}', '{"a":null}')`, want: "{}"},
		// JSON_ARRAY_APPEND adds to the end of the array a path names, which
		// SQLite writes as the index "#" -- one past the last element.
		{query: `SELECT JSON_ARRAY_APPEND('[1]', '$', 2)`, want: "[1,2]"},
		{query: `SELECT JSON_ARRAY_APPEND('{"a":[1]}', '$.a', 2)`, want: `{"a":[1,2]}`},
		{query: `SELECT JSON_ARRAY_APPEND('[1]', '$', 2, '$', 3)`, want: "[1,2,3]"},
		{query: `SELECT JSON_ARRAY_APPEND('[1]', '$', 'x')`, want: `[1,"x"]`},

		// IS_UUID reads three spellings and nothing else: the hyphenated form,
		// the same in braces, and the thirty-two digits. Letter case does not
		// matter and surrounding space is not trimmed.
		{query: `SELECT IS_UUID('6ccd780c-baba-1026-9564-5b8c656024db')`, want: "1"},
		{query: `SELECT IS_UUID('6CCD780C-BABA-1026-9564-5B8C656024DB')`, want: "1"},
		{query: `SELECT IS_UUID('6ccd780cbaba102695645b8c656024db')`, want: "1"},
		{query: `SELECT IS_UUID('{6ccd780c-baba-1026-9564-5b8c656024db}')`, want: "1"},
		{query: `SELECT IS_UUID('{6ccd780cbaba102695645b8c656024db}')`, want: "0"},
		{query: `SELECT IS_UUID('6ccd780cb-aba-1026-9564-5b8c656024db')`, want: "0"},
		{query: `SELECT IS_UUID(' 6ccd780c-baba-1026-9564-5b8c656024db')`, want: "0"},
		{query: `SELECT IS_UUID('6ccd780c-baba-1026-9564-5b8c656024dz')`, want: "0"},
		{query: `SELECT IS_UUID('a')`, want: "0"},
	}

	for _, tt := range tests {
		got, err := runDialect(t, db, dialects.MySQL, tt.query)
		if err != nil {
			t.Errorf("%s: %v", tt.query, err)
			continue
		}
		if tt.wantNull {
			if got.Valid {
				t.Errorf("%s = %q, want NULL", tt.query, got.String)
			}
			continue
		}
		if !got.Valid || got.String != tt.want {
			t.Errorf("%s = %v, want %q", tt.query, got, tt.want)
		}
	}

	// A value that is not a document is refused rather than answered.
	if _, err := runDialect(t, db, dialects.MySQL, `SELECT JSON_TYPE('bad')`); err == nil {
		t.Error("JSON_TYPE('bad'): want an error, got none")
	}
}

// TestMySQLFunctionsAddedForTheEngine covers the functions that had no
// translation at all and reached SQLite as "no such function", together with
// the text a REAL is written as. Every want was read from MySQL 8.4.11.
func TestMySQLFunctionsAddedForTheEngine(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	for _, tt := range []struct {
		query    string
		want     string
		wantNull bool
		wantErr  bool
	}{
		// MAKE_SET reads the bits of the first argument from the low end and
		// keeps the strings they select, skipping a NULL among them.
		{query: `SELECT MAKE_SET(1|4,'a','b','c')`, want: "a,c"},
		{query: `SELECT MAKE_SET(0,'a','b')`, want: ""},
		{query: `SELECT MAKE_SET(NULL,'a')`, wantNull: true},
		{query: `SELECT MAKE_SET(7,'a',NULL,'c')`, want: "a,c"},
		{query: `SELECT MAKE_SET(-1,'a','b')`, want: "a,b"},

		// EXPORT_SET writes 64 bits when it is not told how many.
		{query: `SELECT EXPORT_SET(5,'Y','N',',',4)`, want: "Y,N,Y,N"},
		{query: `SELECT EXPORT_SET(6,'1','0','',10)`, want: "0110000000"},
		{query: `SELECT EXPORT_SET(5,'Y','N',',',0)`, want: ""},
		{query: `SELECT LENGTH(EXPORT_SET(5,'Y','N',''))`, want: "64"},

		// INTERVAL answers the position of the first argument among the rest,
		// which are read as an ascending list.
		{query: `SELECT INTERVAL(23,1,15,17,30,44,200)`, want: "3"},
		{query: `SELECT INTERVAL(10,1,10,100,1000)`, want: "2"},
		{query: `SELECT INTERVAL(NULL,1,2)`, want: "-1"},
		{query: `SELECT INTERVAL(-1,1,2)`, want: "0"},

		{query: `SELECT JSON_LENGTH('[1,2,3]')`, want: "3"},
		{query: `SELECT JSON_LENGTH('{"a":1,"b":2}')`, want: "2"},
		{query: `SELECT JSON_LENGTH('{"a":{"b":1,"c":2}}','$.a')`, want: "2"},
		{query: `SELECT JSON_LENGTH('"x"')`, want: "1"},
		{query: `SELECT JSON_LENGTH('not json')`, wantErr: true},
		{query: `SELECT JSON_CONTAINS('[1,2,3]','2')`, want: "1"},
		{query: `SELECT JSON_CONTAINS('{"a":1}','1','$.a')`, want: "1"},
		{query: `SELECT JSON_CONTAINS('[1,2]','[1,2,3]')`, want: "0"},
		{query: `SELECT JSON_CONTAINS('{"a":1,"b":2}','{"a":1}')`, want: "1"},

		// The numeric functions read the leading number of a string rather than
		// answering NULL, and raise nothing for a value out of range.
		{query: `SELECT CEIL('2024-02-29')`, want: "2024"},
		{query: `SELECT CEILING(-1.5)`, want: "-1"},
		{query: `SELECT FLOOR('2.9abc')`, want: "2"},
		{query: `SELECT SIGN('-3x')`, want: "-1"},
		{query: `SELECT SQRT('4a')`, want: "2"},
		{query: `SELECT EXP(1)`, want: "2.718281828459045"},
		{query: `SELECT LN(0)`, wantNull: true},
		{query: `SELECT LN(-1)`, wantNull: true},
		{query: `SELECT LOG2(8)`, want: "3"},
		{query: `SELECT LOG10('100')`, want: "2"},

		{query: `SELECT COALESCE('a')`, want: "a"},

		// A REAL is written plainly, not in exponent notation. The failures
		// differ in kind, so each family is here: padding truncated the wrong
		// string, a position moved, and a substring returned characters the
		// number does not contain.
		{query: `SELECT CONCAT(1234567.5,'x')`, want: "1234567.5x"},
		{query: `SELECT UPPER(1234567.5)`, want: "1234567.5"},
		{query: `SELECT LPAD(1234567.5,12,'0')`, want: "0001234567.5"},
		{query: `SELECT REVERSE(1234567.5)`, want: "5.7654321"},
		{query: `SELECT SUBSTRING(1234567.5,1,3)`, want: "123"},
		{query: `SELECT REPLACE(1234567.5,'5','9')`, want: "1234967.9"},
		{query: `SELECT LOCATE('5',1234567.5)`, want: "5"},
		{query: `SELECT CAST(1234567.5 AS CHAR)`, want: "1234567.5"},
	} {
		t.Run(tt.query, func(t *testing.T) {
			got, err := runDialect(t, db, dialects.MySQL, tt.query)
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
			if !tt.wantNull && got.String != tt.want {
				t.Errorf("%s = %q, want %q", tt.query, got.String, tt.want)
			}
		})
	}
}

// TestMySQLStringFunctionsReadARealTheWayTheEngineDoes is the end of the same
// thread as TestMySQLWritesARealTheWayTheEngineDoes: the spelling is only worth
// having if the functions that write a value go through it. Every want below
// was read from a DOUBLE column on mysql:8.4 rather than derived.
//
// The functions here are the ones this dialect answers with a helper of its
// own; the ones that stay on SQLite's built-ins are in
// TestMySQLBuiltinsReadARealTheWayTheEngineDoes.
func TestMySQLStringFunctionsReadARealTheWayTheEngineDoes(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		query string
		want  string
	}{
		{query: `SELECT CONCAT(1e308, '')`, want: "1e308"},
		{query: `SELECT CONCAT(-1e308, '')`, want: "-1e308"},
		{query: `SELECT CONCAT(1e15, '')`, want: "1e15"},
		{query: `SELECT CONCAT(1e14, '')`, want: "100000000000000"},
		{query: `SELECT CONCAT(1e-5, '')`, want: "0.00001"},
		{query: `SELECT CONCAT(1e-15, '')`, want: "0.000000000000001"},
		{query: `SELECT CONCAT(1e-16, '')`, want: "1e-16"},
		{query: `SELECT CONCAT(1234567.5, '')`, want: "1234567.5"},
		{query: `SELECT CONCAT(0.1, '')`, want: "0.1"},
		{query: `SELECT CONCAT(0, '')`, want: "0"},
		{query: `SELECT QUOTE(1e308)`, want: "'1e308'"},
		{query: `SELECT QUOTE(1e15)`, want: "'1e15'"},
		{query: `SELECT QUOTE(1e-5)`, want: "'0.00001'"},
		{query: `SELECT QUOTE(0)`, want: "'0'"},
		{query: `SELECT REVERSE(1e308)`, want: "803e1"},
		{query: `SELECT REVERSE(1e15)`, want: "51e1"},
		{query: `SELECT REVERSE(1e-5)`, want: "10000.0"},
		{query: `SELECT REVERSE(0)`, want: "0"},
		{query: `SELECT TO_BASE64(1e308)`, want: "MWUzMDg="},
		{query: `SELECT TO_BASE64(1e15)`, want: "MWUxNQ=="},
		{query: `SELECT TO_BASE64(1e-5)`, want: "MC4wMDAwMQ=="},
		{query: `SELECT TO_BASE64(0)`, want: "MA=="},
		{query: `SELECT UPPER(1e308)`, want: "1E308"},
		{query: `SELECT UPPER(1e15)`, want: "1E15"},
		{query: `SELECT UPPER(1e-5)`, want: "0.00001"},
		{query: `SELECT UPPER(0)`, want: "0"},
		{query: `SELECT LPAD(1e308, 10, '-')`, want: "-----1e308"},
		{query: `SELECT LPAD(1e15, 10, '-')`, want: "------1e15"},
		{query: `SELECT LPAD(1e-5, 10, '-')`, want: "---0.00001"},
		{query: `SELECT LPAD(0, 10, '-')`, want: "---------0"},
		{query: `SELECT REPEAT(1e308, 2)`, want: "1e3081e308"},
		{query: `SELECT REPEAT(1e15, 2)`, want: "1e151e15"},
		{query: `SELECT REPEAT(1e-5, 2)`, want: "0.000010.00001"},
		{query: `SELECT REPEAT(0, 2)`, want: "00"},
		{query: `SELECT LEFT(1e308, 4)`, want: "1e30"},
		{query: `SELECT LEFT(1e15, 4)`, want: "1e15"},
		{query: `SELECT LEFT(1e-5, 4)`, want: "0.00"},
		{query: `SELECT LEFT(0, 4)`, want: "0"},
		{query: `SELECT SUBSTRING(1e308, 2, 3)`, want: "e30"},
		{query: `SELECT SUBSTRING(1e15, 2, 3)`, want: "e15"},
		{query: `SELECT SUBSTRING(1e-5, 2, 3)`, want: ".00"},
		{query: `SELECT SUBSTRING(0, 2, 3)`, want: ""},
		{query: `SELECT REPLACE(1e308, 'e', 'E')`, want: "1E308"},
		{query: `SELECT REPLACE(1e15, 'e', 'E')`, want: "1E15"},
		{query: `SELECT REPLACE(1e-5, 'e', 'E')`, want: "0.00001"},
		{query: `SELECT REPLACE(0, 'e', 'E')`, want: "0"},
		{query: `SELECT ELT(1, 1e308)`, want: "1e308"},
		{query: `SELECT ELT(1, 1e15)`, want: "1e15"},
		{query: `SELECT ELT(1, 1e-5)`, want: "0.00001"},
		{query: `SELECT ELT(1, 0)`, want: "0"},
		{query: `SELECT ASCII(1e308)`, want: "49"},
		{query: `SELECT ASCII(1e15)`, want: "49"},
		{query: `SELECT ASCII(1e-5)`, want: "48"},
		{query: `SELECT ASCII(0)`, want: "48"},
		{query: `SELECT SOUNDEX(1e308)`, want: "E000"},
		{query: `SELECT SOUNDEX(1e15)`, want: "E000"},
		{query: `SELECT SOUNDEX(1e-5)`, want: ""},
		{query: `SELECT SOUNDEX(0)`, want: ""},
		{query: `SELECT INSERT(1e308, 1, 1, 'Z')`, want: "Ze308"},
		{query: `SELECT INSERT(1e15, 1, 1, 'Z')`, want: "Ze15"},
		{query: `SELECT INSERT(1e-5, 1, 1, 'Z')`, want: "Z.00001"},
		{query: `SELECT INSERT(0, 1, 1, 'Z')`, want: "Z"},
		{query: `SELECT MD5(1e308)`, want: "a977d76bc2191e46d753b000b372e6ca"},
		{query: `SELECT MD5(1e15)`, want: "28b015ce9d58b0bc683ebdf4fe4e2a10"},
		{query: `SELECT MD5(1e-5)`, want: "4d349a3d6db9dac94357eff8a4273d7e"},
		{query: `SELECT MD5(0)`, want: "cfcd208495d565ef66e7dff9f98764da"},
		{query: `SELECT STRCMP(1e308, '1e15')`, want: "1"},
		{query: `SELECT STRCMP(1e15, '1e15')`, want: "0"},
		{query: `SELECT STRCMP(1e-5, '1e15')`, want: "-1"},
		{query: `SELECT STRCMP(0, '1e15')`, want: "-1"},
		{query: `SELECT BIT_LENGTH(1e308)`, want: "40"},
		{query: `SELECT BIT_LENGTH(1e15)`, want: "32"},
		{query: `SELECT BIT_LENGTH(1e-5)`, want: "56"},
		{query: `SELECT BIT_LENGTH(0)`, want: "8"},
		{query: `SELECT SUBSTRING_INDEX(1e308, 'e', 1)`, want: "1"},
		{query: `SELECT SUBSTRING_INDEX(1e15, 'e', 1)`, want: "1"},
		{query: `SELECT SUBSTRING_INDEX(1e-5, 'e', 1)`, want: "0.00001"},
		{query: `SELECT SUBSTRING_INDEX(0, 'e', 1)`, want: "0"},
		{query: `SELECT FIND_IN_SET(1e308, '1e15,0.1')`, want: "0"},
		{query: `SELECT FIND_IN_SET(1e15, '1e15,0.1')`, want: "1"},
		{query: `SELECT FIND_IN_SET(1e-5, '1e15,0.1')`, want: "0"},
		{query: `SELECT FIND_IN_SET(0, '1e15,0.1')`, want: "0"},
		{query: `SELECT REGEXP_LIKE(1e308, '^1e')`, want: "1"},
		{query: `SELECT REGEXP_LIKE(1e15, '^1e')`, want: "1"},
		{query: `SELECT REGEXP_LIKE(1e-5, '^1e')`, want: "0"},
		{query: `SELECT REGEXP_LIKE(0, '^1e')`, want: "0"},

		// LIKE and the three-argument LOCATE read the same value as text, and
		// 1e-5 is the case that tells the two spellings apart: MySQL matches
		// "0.00001" against '1e%' and does not, where the shared spelling
		// would offer it "1e-05" and match.
		{query: `SELECT IF(1e308 LIKE '1e%', 1, 0)`, want: "1"},
		{query: `SELECT IF(1e15 LIKE '1e%', 1, 0)`, want: "1"},
		{query: `SELECT IF(1e-5 LIKE '1e%', 1, 0)`, want: "0"},
		{query: `SELECT IF(1e-16 LIKE '1e%', 1, 0)`, want: "1"},
		{query: `SELECT LOCATE('e', 1e308, 2)`, want: "2"},
		{query: `SELECT LOCATE('e', 1e15, 2)`, want: "2"},
		{query: `SELECT LOCATE('e', 1e-5, 2)`, want: "0"},
		{query: `SELECT LOCATE('e', 1e-16, 2)`, want: "2"},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			got, err := runDialect(t, db, dialects.MySQL, tt.query)
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			if !got.Valid {
				t.Fatalf("%s = NULL, want %q", tt.query, tt.want)
			}
			if got.String != tt.want {
				t.Errorf("%s = %q, want %q", tt.query, got.String, tt.want)
			}
		})
	}
}

// TestMySQLBuiltinsReadARealTheWayTheEngineDoes is the same property for the
// calls this dialect leaves on a function SQLite answers itself, where the
// argument is wrapped rather than the call renamed. SQLite converts a REAL to
// text with its own rules, so before the wrap TRIM over a column holding 1e15
// answered "1000000000000000.0" and LENGTH of it was 18. Every want below was
// read from a DOUBLE column on mysql:8.4 rather than derived.
func TestMySQLBuiltinsReadARealTheWayTheEngineDoes(t *testing.T) {
	// Not parallel: castDB touches the process-global driver registration.
	db := castDB(t)

	tests := []struct {
		query string
		want  string
	}{
		{query: `SELECT TRIM(1e308)`, want: "1e308"},
		{query: `SELECT TRIM(1e15)`, want: "1e15"},
		{query: `SELECT TRIM(1e-5)`, want: "0.00001"},
		{query: `SELECT TRIM(0)`, want: "0"},
		{query: `SELECT LTRIM(1e308)`, want: "1e308"},
		{query: `SELECT LTRIM(1e15)`, want: "1e15"},
		{query: `SELECT LTRIM(1e-5)`, want: "0.00001"},
		{query: `SELECT LTRIM(0)`, want: "0"},
		{query: `SELECT RTRIM(1e308)`, want: "1e308"},
		{query: `SELECT RTRIM(1e15)`, want: "1e15"},
		{query: `SELECT RTRIM(1e-5)`, want: "0.00001"},
		{query: `SELECT RTRIM(0)`, want: "0"},
		{query: `SELECT LENGTH(1e308)`, want: "5"},
		{query: `SELECT LENGTH(1e15)`, want: "4"},
		{query: `SELECT LENGTH(1e-5)`, want: "7"},
		{query: `SELECT LENGTH(0)`, want: "1"},
		{query: `SELECT CHAR_LENGTH(1e308)`, want: "5"},
		{query: `SELECT CHAR_LENGTH(1e15)`, want: "4"},
		{query: `SELECT CHAR_LENGTH(1e-5)`, want: "7"},
		{query: `SELECT CHAR_LENGTH(0)`, want: "1"},
		{query: `SELECT OCTET_LENGTH(1e308)`, want: "5"},
		{query: `SELECT OCTET_LENGTH(1e15)`, want: "4"},
		{query: `SELECT OCTET_LENGTH(1e-5)`, want: "7"},
		{query: `SELECT OCTET_LENGTH(0)`, want: "1"},
		{query: `SELECT LOCATE('e', 1e308)`, want: "2"},
		{query: `SELECT LOCATE('e', 1e15)`, want: "2"},
		{query: `SELECT LOCATE('e', 1e-5)`, want: "0"},
		{query: `SELECT LOCATE('e', 0)`, want: "0"},
		{query: `SELECT INSTR(1e308, 'e')`, want: "2"},
		{query: `SELECT INSTR(1e15, 'e')`, want: "2"},
		{query: `SELECT INSTR(1e-5, 'e')`, want: "0"},
		{query: `SELECT INSTR(0, 'e')`, want: "0"},
		{query: `SELECT CONCAT_WS('-', 1e308, 1e308)`, want: "1e308-1e308"},
		{query: `SELECT CONCAT_WS('-', 1e15, 1e15)`, want: "1e15-1e15"},
		{query: `SELECT CONCAT_WS('-', 1e-5, 1e-5)`, want: "0.00001-0.00001"},
		{query: `SELECT CONCAT_WS('-', 0, 0)`, want: "0-0"},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			got, err := runDialect(t, db, dialects.MySQL, tt.query)
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			if !got.Valid {
				t.Fatalf("%s = NULL, want %q", tt.query, tt.want)
			}
			if got.String != tt.want {
				t.Errorf("%s = %q, want %q", tt.query, got.String, tt.want)
			}
		})
	}
}

// TestMySQLPositionCallsFoldCase pins the four spellings of "where does this
// substring start" against MySQL's default collation, which folds case. LIKE
// and REGEXP were routed to helpers that fold and these four were not, so each
// answered 0 -- "not found" -- for a needle differing only in case, which is a
// wrong answer in the shape these calls are most often written in.
func TestMySQLPositionCallsFoldCase(t *testing.T) {
	db := castDB(t)

	for _, tt := range []struct{ name, query, want string }{
		{"instr", `SELECT INSTR('ABC', 'b')`, "2"},
		{"instr the other way round", `SELECT INSTR('abc', 'B')`, "2"},
		{"locate", `SELECT LOCATE('b', 'ABC')`, "2"},
		{"locate from a position", `SELECT LOCATE('b', 'ABC', 1)`, "2"},
		{"locate from a later position", `SELECT LOCATE('B', 'abcabc', 3)`, "5"},
		{"position", `SELECT POSITION('b' IN 'ABC')`, "2"},
		{"find_in_set", `SELECT FIND_IN_SET('b', 'a,B,c')`, "2"},
		{"find_in_set the other way round", `SELECT FIND_IN_SET('B', 'a,b,c')`, "2"},
		{"like folds too", `SELECT 'a' LIKE 'A'`, "1"},
		{"a needle that is not there is still not there", `SELECT INSTR('ABC', 'z')`, "0"},
		{"the position is a character position", `SELECT INSTR('あいABC', 'b')`, "4"},
		{"an accent is not folded", `SELECT INSTR('abc', 'á')`, "0"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runDialect(t, db, dialects.MySQL, tt.query)
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			if got.String != tt.want {
				t.Errorf("%s = %q, want %q", tt.query, got.String, tt.want)
			}
		})
	}

	// Neither other dialect folds, so the calls they share must stay
	// case-sensitive.
	for _, tt := range []struct {
		d     dialects.Dialect
		query string
	}{
		{dialects.PostgreSQL, `SELECT POSITION('b' IN 'ABC')`},
		{dialects.GoogleSQL, `SELECT STRPOS('ABC', 'b')`},
	} {
		got, err := runDialect(t, db, tt.d, tt.query)
		if err != nil {
			t.Fatalf("%v %s: %v", tt.d, tt.query, err)
		}
		if got.String != "0" {
			t.Errorf("%v %s = %q, want %q", tt.d, tt.query, got.String, "0")
		}
	}
}

// TestMySQLNumericCallsReadAStringAsZero pins MySQL's reading of a string that
// spells no number, which is zero rather than nothing. Ten calls already read
// it that way and five answered NULL instead, so an expression that MySQL
// answers a number for lost its row here with nothing said.
func TestMySQLNumericCallsReadAStringAsZero(t *testing.T) {
	db := castDB(t)

	for _, tt := range []struct {
		name, query, want string
		wantNull          bool
	}{
		{name: "round", query: `SELECT ROUND('abc')`, want: "0"},
		{name: "round to decimals", query: `SELECT ROUND('abc', 2)`, want: "0"},
		{name: "truncate", query: `SELECT TRUNCATE('abc', 1)`, want: "0"},
		{name: "pow", query: `SELECT POW('abc', 2)`, want: "0"},
		{name: "power", query: `SELECT POWER('abc', 2)`, want: "0"},
		{name: "format", query: `SELECT FORMAT('abc', 2)`, want: "0.00"},
		{name: "interval", query: `SELECT INTERVAL('abc', 1, 2)`, want: "0"},
		{name: "interval over strings", query: `SELECT INTERVAL('b', 'A', 'C')`, want: "2"},
		{name: "abs, which already read it this way", query: `SELECT ABS('abc')`, want: "0"},
		{name: "ceil, which already read it this way", query: `SELECT CEIL('abc')`, want: "0"},
		{name: "a cast, which already read it this way", query: `SELECT CAST('abc' AS SIGNED)`, want: "0"},

		// The rule is a numeric prefix rather than an all-or-nothing parse,
		// and it is the same rule for a value that does spell a number.
		{name: "a numeric prefix", query: `SELECT ROUND('12abc')`, want: "12"},
		{name: "a number with spaces around it", query: `SELECT ROUND(' 12 ')`, want: "12"},
		{name: "a number in a string rounds the way a double does", query: `SELECT ROUND('2.5', 0)`, want: "2"},
		{name: "a number is still a number", query: `SELECT ROUND(2.4)`, want: "2"},

		// A NULL is still nothing, and INTERVAL keeps the -1 MySQL reserves
		// for one.
		{name: "a NULL is still NULL", query: `SELECT ROUND(NULL)`, want: "", wantNull: true},
		{name: "interval of a NULL", query: `SELECT INTERVAL(NULL, 1, 2)`, want: "-1"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := runDialect(t, db, dialects.MySQL, tt.query)
			if err != nil {
				t.Fatalf("%s: %v", tt.query, err)
			}
			// A NULL and an empty string both read as "" here, and the two mean
			// opposite things for this rule: the point is that a string with no
			// number in it is a number and a NULL is still nothing.
			if got.Valid == tt.wantNull {
				t.Errorf("%s valid = %v, want %v", tt.query, got.Valid, !tt.wantNull)
			}
			if got.String != tt.want {
				t.Errorf("%s = %q, want %q", tt.query, got.String, tt.want)
			}
		})
	}
}
