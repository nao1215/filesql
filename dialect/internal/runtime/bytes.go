package runtime

import (
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/nao1215/filesql/dialect/internal/dialects"
)

// A byte string is a value SQLite carries as a BLOB and neither MySQL nor
// GoogleSQL treats as a number. Both define the bitwise operators over one --
// MySQL for BINARY, VARBINARY and the BLOB types, GoogleSQL for BYTES -- and
// both answer a byte string of the operand's length, where SQLite reads a BLOB
// in an arithmetic context as the integer 0. The work of shifting and combining
// the bytes is the same for the two dialects and lives here; what differs is
// the wording of a refusal and what a negative shift count means, which stays
// with each dialect's helpers.
//
// The driver hands text over as a string and a BLOB as a []byte, so a []byte
// argument is a byte string and nothing else is. A helper that has not got one
// falls back to the integer reading it had before.

// bytesOperand reports the bytes of an operand the driver handed over as a
// BLOB. A text operand is not one, even when it spells the same characters:
// MySQL applies these operators bytewise to a binary string and numerically to
// a nonbinary one, and SQLite's storage class is the distinction this package
// has for that.
func bytesOperand(v driver.Value) ([]byte, bool) {
	b, ok := v.([]byte)
	return b, ok
}

// bytewise combines two byte strings of equal length with op. The caller has
// already established the lengths agree, since what it says when they do not is
// its dialect's business.
func bytewise(a, b []byte, op func(x, y byte) byte) []byte {
	out := make([]byte, len(a))
	for i := range a {
		out[i] = op(a[i], b[i])
	}
	return out
}

// bytesNot is the ones' complement of a byte string, at its own length.
func bytesNot(b []byte) []byte {
	out := make([]byte, len(b))
	for i, c := range b {
		out[i] = ^c
	}
	return out
}

// bytesShift shifts the bit pattern a byte string holds, keeping its length:
// bits shifted off the end are discarded and the vacated ones are filled with
// zeros, so a count at or past the bit length answers that many zero bytes.
// count is unsigned because a dialect that accepts a negative one reads it as a
// count past the width rather than as a shift the other way.
func bytesShift(b []byte, count uint64, left bool) []byte {
	out := make([]byte, len(b))
	if count >= uint64(len(b))*8 {
		return out
	}
	// count is below the bit length here, so the whole bytes it moves are an
	// index into b.
	whole, bits := int(count/8), count%8 //nolint:gosec // bounded by the check above
	for i := range out {
		// from is the byte this one takes its high bits from, and from+1 the
		// one it takes the rest from once the shift is not a whole number of
		// bytes.
		var from int
		if left {
			from = i + whole
		} else {
			from = i - whole
		}
		if from < 0 || from >= len(b) {
			continue
		}
		if bits == 0 {
			out[i] = b[from]
			continue
		}
		if left {
			out[i] = b[from] << bits
			if from+1 < len(b) {
				out[i] |= b[from+1] >> (8 - bits)
			}
			continue
		}
		out[i] = b[from] >> bits
		if from-1 >= 0 {
			out[i] |= b[from-1] << (8 - bits)
		}
	}
	return out
}

// bitOp is one of "&", "|" and "^", in the two forms these operators need: on a
// pair of bytes for a byte-string operand, and on the whole 64 bits for the
// integer reading of any other operand.
type bitOp struct {
	onBytes func(x, y byte) byte
	onBits  func(x, y uint64) uint64
}

//nolint:gochecknoglobals // three constant-like operations, named once
var (
	bitAnd = bitOp{func(x, y byte) byte { return x & y }, func(x, y uint64) uint64 { return x & y }}
	bitOr  = bitOp{func(x, y byte) byte { return x | y }, func(x, y uint64) uint64 { return x | y }}
	bitXor = bitOp{func(x, y byte) byte { return x ^ y }, func(x, y uint64) uint64 { return x ^ y }}
)

// dialectBitOp builds "&", "|" and "^" for a dialect that defines them over
// byte strings. Two byte strings are combined bytewise and must be of equal
// length, which is what MySQL and GoogleSQL both require; any other pair of
// operands takes the unsigned 64-bit reading these operators had before, so a
// number, a numeral written as text and a NULL answer what they answered.
func dialectBitOp(d dialects.Dialect, op bitOp) scalarFn {
	return func(args []driver.Value) (driver.Value, error) {
		a, aok := bytesOperand(args[0])
		b, bok := bytesOperand(args[1])
		switch {
		case aok && bok:
			if len(a) != len(b) {
				return nil, unequalBytesError(d, len(a), len(b))
			}
			return bytewise(a, b, op.onBytes), nil
		case args[0] == nil || args[1] == nil:
			// A NULL operand is NULL, as it is for every operator in SQL, and
			// it is not a type the pair has to agree on.
			return nil, nil
		case (aok || bok) && d == dialects.GoogleSQL:
			// GoogleSQL takes the second operand as the same type as the
			// first, so a BYTES beside an integer matches no signature.
			return nil, errors.New(
				"dialect: a bitwise operator takes two byte strings or two integers, not one of each")
		}
		x, ok1 := toUint64Bits(args[0])
		y, ok2 := toUint64Bits(args[1])
		if !ok1 || !ok2 {
			return nil, nil
		}
		return int64(op.onBits(x, y)), nil //nolint:gosec // the bits are the value; SQLite has no unsigned integer
	}
}

// dialectBitNot is "~" for a dialect that defines it over byte strings. A byte
// string is complemented at its own length; anything else takes the unsigned
// 64-bit reading SQLite's own "~" gave it, where a value that is not a number
// counts as zero.
func dialectBitNot(args []driver.Value) (driver.Value, error) {
	if b, ok := bytesOperand(args[0]); ok {
		return bytesNot(b), nil
	}
	if args[0] == nil {
		return nil, nil
	}
	u, _ := toUint64Bits(args[0])
	return int64(^u), nil //nolint:gosec // the bits are the value; SQLite has no unsigned integer
}

// unequalBytesError is what a dialect says about operands of different lengths.
// Both dialects that reach it refuse them, and the message names the dialect so
// a caller reading it knows which rule they met.
func unequalBytesError(d dialects.Dialect, a, b int) error {
	return fmt.Errorf("dialect: %s bitwise operands are byte strings of different lengths: %d and %d",
		d.DisplayName(), a, b)
}
