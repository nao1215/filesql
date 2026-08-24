package reader

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"strconv"
	"strings"

	"github.com/nao1215/filesql/internal/infer"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// The reader is github.com/parquet-go/parquet-go rather than the Arrow one it
// replaced, because a Parquet file this package did not write is untrusted
// input and the Arrow page decoder was unbounded on some of it: a 433-byte
// file held a load forever while it allocated hundreds of megabytes a second,
// it did not check the context it was given, and Go has no way to stop a
// goroutine that is allocating. parquet-go refuses the same files in
// microseconds, with time and memory bounded by the file's own size.

// parquetMagic is the four bytes a Parquet file begins and ends with. The
// format defines both, and checking the leading one before handing the bytes
// to a library is what keeps arbitrary non-Parquet input out of a decoder:
// fuzzing the previous reader with the check in place ran 1.4 million inputs
// without a hang or a panic; without it, a worker died within thirty seconds.
var parquetMagic = []byte("PAR1") //nolint:gochecknoglobals // constant-like

// errNotParquet reports bytes that do not begin the way the format says.
func errNotParquet(head []byte) error {
	return parseError(nil, "not a parquet file: it begins %q rather than %q", head, parquetMagic)
}

// readParquet reads a Parquet file in chunks. The whole file is buffered first
// because the format is read back to front: its metadata is at the end.
func readParquet(src io.Reader, opts Options, emit Emit) (Result, error) {
	data, err := io.ReadAll(src)
	if err != nil {
		return Result{}, parseError(err, "failed to read parquet data")
	}
	if len(data) == 0 {
		return Result{}, emptyError("empty parquet file")
	}
	if !bytes.HasPrefix(data, parquetMagic) {
		return Result{}, errNotParquet(data[:min(len(data), len(parquetMagic))])
	}

	file, err := openParquet(data)
	if err != nil {
		return Result{}, err
	}

	header, columns, leafField, flat, err := parquetSchemaLayout(file)
	if err != nil {
		return Result{}, err
	}
	// Parquet declares the type of every column, so the schema is read rather
	// than inferred from the rendered values: inference cannot tell a STRING
	// column of digits from an INT64 one, and would turn a zip code into a
	// number.
	types := make([]infer.Type, len(columns))
	for i, col := range columns {
		types[i] = col.columnType(opts.Rendering)
	}
	result := Result{Header: header, Types: types}

	// A file with a schema and no rows still names its columns.
	if file.NumRows() == 0 {
		return result, emit(&Chunk{Header: header, Types: types})
	}

	chunkSize := chunkSizeOf(opts)
	chunkCap := min(chunkSize, 1024)
	records := make([][]string, 0, chunkCap)
	nulls := make([][]bool, 0, chunkCap)
	flush := func() error {
		if len(records) == 0 {
			return nil
		}
		result.Rows += len(records)
		result.Total += len(records)
		err := emit(&Chunk{Header: header, Records: records, Types: types, Nulls: nulls})
		// The emitted chunk owns its slices -- a caller may keep them -- so the
		// next chunk starts fresh rather than over the same backing array.
		records = make([][]string, 0, chunkCap)
		nulls = make([][]bool, 0, chunkCap)
		return err
	}

	buf := make([]parquet.Row, min(chunkSize, 1024))
	for _, rowGroup := range file.RowGroups() {
		rows, err := openParquetRows(rowGroup)
		if err != nil {
			return Result{}, err
		}
		readErr := func() error {
			defer rows.Close()
			for {
				n, err := readParquetRows(rows, buf)
				for _, parquetRow := range buf[:n] {
					row, nullRow := renderParquetRow(parquetRow, columns, leafField, flat, opts.Rendering)
					records = append(records, row)
					nulls = append(nulls, nullRow)
					if len(records) >= chunkSize {
						if err := flush(); err != nil {
							return err
						}
					}
				}
				if errors.Is(err, io.EOF) {
					return nil
				}
				if err != nil {
					return parseError(err, "failed to read table")
				}
				// No rows, no error, and no EOF is a read that made no
				// progress; looping on it would never return. A healthy read
				// into a non-empty buffer always does one of the three.
				if n == 0 {
					return parseError(nil, "parquet data is damaged: a read returned no rows and no error")
				}
			}
		}()
		if readErr != nil {
			return Result{}, readErr
		}
	}
	if err := flush(); err != nil {
		return Result{}, err
	}
	return result, nil
}

// openParquet opens the file's metadata, with any panic the library raises on
// damaged input turned into an error: a caller loading a file chosen by
// someone else cannot defend against a panic, and every other malformed input
// here is an error.
func openParquet(data []byte) (file *parquet.File, err error) {
	defer func() {
		if r := recover(); r != nil {
			file = nil
			err = parseError(nil, "parquet data is damaged: %v", r)
		}
	}()
	// The page index and bloom filters are for pruning reads; this read scans
	// every row, so neither is worth decoding.
	file, err = parquet.OpenFile(bytes.NewReader(data), int64(len(data)),
		parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
	if err != nil {
		return nil, parseError(err, "failed to create parquet reader")
	}
	return file, nil
}

// openParquetRows is RowGroup.Rows with the panic a damaged schema can raise
// turned into an error: building the row reader asks every leaf type its kind,
// which panics on the same inconsistent metadata parquetSchemaLayout guards
// against, this time inside the library's own walk.
func openParquetRows(rowGroup parquet.RowGroup) (rows parquet.Rows, err error) {
	defer func() {
		if r := recover(); r != nil {
			rows = nil
			err = parseError(nil, "parquet data is damaged: %v", r)
		}
	}()
	return rowGroup.Rows(), nil
}

// readParquetRows is Rows.ReadRows with the panic a damaged page can raise
// turned into an error.
func readParquetRows(rows parquet.Rows, buf []parquet.Row) (n int, err error) {
	defer func() {
		if r := recover(); r != nil {
			n = 0
			err = fmt.Errorf("parquet data is damaged: %v", r)
		}
	}()
	return rows.ReadRows(buf)
}

// parquetColumn is one top-level field of the schema and how its values are
// rendered.
type parquetColumn struct {
	leaf     bool // a single non-repeated primitive, the common case
	kind     parquet.Kind
	unsigned bool  // INT(bits, false): the physical int carries an unsigned value
	float16  bool  // FLOAT16: two bytes carrying a half-precision float
	decimal  bool  // DECIMAL(precision, scale) over an int or fixed bytes
	scale    int32 // the DECIMAL scale
	uuid     bool  // UUID: sixteen bytes rendered in the canonical hex form
}

// parquetSchemaLayout reads the file's schema into the header, the column
// descriptions, and the leaf-to-field mapping, with any panic the library
// raises on a damaged schema turned into an error: the type of a leaf whose
// metadata is inconsistent -- a MAP annotation on a node with a physical type,
// found by fuzzing -- panics when asked its kind.
func parquetSchemaLayout(file *parquet.File) (header []string, columns []parquetColumn, leafField []int, flat bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = parseError(nil, "parquet data is damaged: %v", r)
		}
	}()
	fields := file.Schema().Fields()
	header = make([]string, len(fields))
	for i, field := range fields {
		header[i] = field.Name()
	}
	columns = parquetColumns(file)
	leafField, flat = parquetLeafFields(file.Schema(), columns)
	return header, columns, leafField, flat, nil
}

// parquetColumns describes each top-level field of the file's schema.
//
// The FLOAT16 annotation is read from the file's raw metadata rather than from
// the parsed schema, because the library's schema normalization does not carry
// it: the parsed leaf presents as a plain fixed-length byte array.
func parquetColumns(file *parquet.File) []parquetColumn {
	halfFloats := float16Leaves(file.Metadata().Schema)
	fields := file.Schema().Fields()
	columns := make([]parquetColumn, len(fields))
	leafIndex := 0
	for i, field := range fields {
		leaves := countParquetLeaves(field)
		col := parquetColumn{leaf: field.Leaf() && !field.Repeated()}
		if col.leaf {
			typ := field.Type()
			if lt := typ.LogicalType(); lt != nil {
				switch v := lt.Value.(type) {
				case *format.IntType:
					col.unsigned = !v.IsSigned
				case *format.DecimalType:
					col.decimal = true
					col.scale = v.Scale
				case *format.UUIDType:
					col.uuid = true
				case *format.Float16Type:
					// The format allows FLOAT16 on two fixed bytes alone; on
					// any other width the annotation is inconsistent metadata
					// and the bytes are left as they are.
					col.float16 = typ.Length() == 2
				case *format.MapType, *format.ListType:
					// A group annotation on a node with a physical type is
					// inconsistent metadata; asking such a type its kind
					// panics. The values still render, by their own kind.
					col.leaf = false
				}
			}
		}
		if col.leaf {
			col.kind = field.Type().Kind()
			col.float16 = col.float16 || halfFloats[leafIndex]
		}
		columns[i] = col
		leafIndex += leaves
	}
	return columns
}

// float16Leaves reports which leaf columns a file's own metadata annotates as
// FLOAT16, by leaf index. The format allows the annotation on two fixed bytes
// alone, so a length of anything else is inconsistent metadata and the column
// is left as the bytes it holds rather than declared a real number it cannot
// render.
func float16Leaves(elements []format.SchemaElement) map[int]bool {
	out := map[int]bool{}
	leaf := 0
	for _, element := range elements {
		if !element.Type.Valid {
			continue // a group node has no physical type and holds no column
		}
		if _, ok := element.LogicalType.Value.(*format.Float16Type); ok &&
			element.TypeLength.Valid && element.TypeLength.V == 2 {
			out[leaf] = true
		}
		leaf++
	}
	return out
}

// countParquetLeaves is how many leaf columns sit under a field.
func countParquetLeaves(field parquet.Field) int {
	if field.Leaf() {
		return 1
	}
	total := 0
	for _, child := range field.Fields() {
		total += countParquetLeaves(child)
	}
	return total
}

// parquetLeafFields maps each leaf column index onto the top-level field it
// belongs to, and reports whether the schema is flat: every field one
// non-repeated leaf, so a row's values line up with the fields one to one.
func parquetLeafFields(schema *parquet.Schema, columns []parquetColumn) ([]int, bool) {
	position := make(map[string]int, len(columns))
	for i, field := range schema.Fields() {
		position[field.Name()] = i
	}
	paths := schema.Columns()
	leafField := make([]int, len(paths))
	for i, path := range paths {
		leafField[i] = position[path[0]]
	}
	flat := len(paths) == len(columns)
	for _, col := range columns {
		flat = flat && col.leaf
	}
	return leafField, flat
}

// columnType is the column type one field calls for. It has to agree with what
// renderParquetValue renders under the same rendering, because a value is read
// back by parsing that string -- and, for a load, because SQLite applies the
// column's affinity to it. A mismatch is worse than text: it would store a
// value the column claims not to hold.
//
// The temporal annotations (DATE, TIME, TIMESTAMP) render as the raw count the
// file stores -- days, or ticks of the schema's unit -- which is an integer. A
// nested or repeated field, and any leaf not named here, stays text, the safe
// answer for a shape this function does not know.
func (c parquetColumn) columnType(rendering Rendering) infer.Type {
	switch {
	case !c.leaf, c.decimal, c.uuid:
		return infer.Text
	case c.float16:
		return infer.Real
	}
	switch c.kind {
	case parquet.Boolean:
		// A boolean renders as 1 or 0 for SQLite, which is an integer there,
		// and as "true" or "false" otherwise, which is not.
		if rendering == RenderSQLite {
			return infer.Integer
		}
		return infer.Text
	case parquet.Int32, parquet.Int64, parquet.Int96:
		return infer.Integer
	case parquet.Float, parquet.Double:
		return infer.Real
	default:
		return infer.Text
	}
}

// renderParquetRow renders one row into its cells and their null marks. In a
// flat schema the row's values line up with the fields; otherwise the values
// are gathered per field, and a field whose values are all null is a null cell
// while any other set renders bracketed, the way a list prints.
func renderParquetRow(row parquet.Row, columns []parquetColumn, leafField []int, flat bool, rendering Rendering) ([]string, []bool) {
	cells := make([]string, len(columns))
	nullRow := make([]bool, len(columns))
	if flat {
		for i, value := range row {
			if i >= len(columns) {
				break
			}
			if parquetCellIsNull(value, columns[i], rendering) {
				nullRow[i] = true
				continue
			}
			cells[i] = renderParquetValue(value, columns[i], rendering)
		}
		return cells, nullRow
	}

	parts := make([][]string, len(columns))
	seen := make([]bool, len(columns))
	for _, value := range row {
		leaf := value.Column()
		if leaf < 0 || leaf >= len(leafField) {
			continue
		}
		i := leafField[leaf]
		if parquetCellIsNull(value, columns[i], rendering) {
			parts[i] = append(parts[i], "")
			continue
		}
		seen[i] = true
		parts[i] = append(parts[i], renderParquetValue(value, columns[i], rendering))
	}
	for i, col := range columns {
		switch {
		case !seen[i]:
			nullRow[i] = true
		case col.leaf && len(parts[i]) == 1:
			cells[i] = parts[i][0]
		default:
			cells[i] = "[" + strings.Join(parts[i], " ") + "]"
		}
	}
	return cells, nullRow
}

// parquetCellIsNull reports whether a cell has no value the destination can
// store: a Parquet null always, and under RenderSQLite a NaN as well, which
// SQLite has no representation for at all -- a computed NaN is NULL there, so
// NULL is what the value already means. Left as text it would sit in a column
// declared REAL as the word "NaN".
func parquetCellIsNull(v parquet.Value, col parquetColumn, rendering Rendering) bool {
	if v.IsNull() {
		return true
	}
	if rendering != RenderSQLite {
		return false
	}
	switch v.Kind() {
	case parquet.Float:
		return math.IsNaN(float64(v.Float()))
	case parquet.Double:
		return math.IsNaN(v.Double())
	case parquet.FixedLenByteArray:
		if b := v.ByteArray(); col.float16 && len(b) == 2 {
			return math.IsNaN(float64(float16To32(uint16(b[0]) | uint16(b[1])<<8)))
		}
	}
	return false
}

// renderParquetValue renders one leaf value as the text its column type calls
// for.
func renderParquetValue(v parquet.Value, col parquetColumn, rendering Rendering) string {
	switch v.Kind() {
	case parquet.Boolean:
		return boolText(v.Boolean(), rendering)
	case parquet.Int32:
		if col.decimal {
			return decimalText(big.NewInt(int64(v.Int32())), col.scale)
		}
		if col.unsigned {
			return strconv.FormatUint(uint64(uint32(v.Int32())), 10) //nolint:gosec // the unsigned column stores its value in the physical int's bits
		}
		return strconv.FormatInt(int64(v.Int32()), 10)
	case parquet.Int64:
		if col.decimal {
			return decimalText(big.NewInt(v.Int64()), col.scale)
		}
		if col.unsigned {
			return strconv.FormatUint(uint64(v.Int64()), 10) //nolint:gosec // the unsigned column stores its value in the physical int's bits
		}
		return strconv.FormatInt(v.Int64(), 10)
	case parquet.Int96:
		return strconv.FormatInt(int96EpochNanos([3]uint32(v.Int96())), 10)
	case parquet.Float:
		return floatText(float64(v.Float()), 32, rendering)
	case parquet.Double:
		return floatText(v.Double(), 64, rendering)
	case parquet.ByteArray, parquet.FixedLenByteArray:
		b := v.ByteArray()
		switch {
		// A half float is rendered at 32 bits, the narrowest width Go can
		// format it at. The bytes are little-endian per the format.
		case col.float16 && len(b) == 2:
			return floatText(float64(float16To32(uint16(b[0])|uint16(b[1])<<8)), 32, rendering)
		case col.decimal:
			return decimalText(twosComplementBig(b), col.scale)
		case col.uuid && len(b) == 16:
			return uuidText(b)
		}
		return string(b)
	default:
		return v.String()
	}
}

// int96EpochNanos converts the deprecated INT96 timestamp -- nanoseconds of
// the day in its low eight bytes and a Julian day in its high four -- to
// nanoseconds since the Unix epoch, which is what the Arrow reader rendered
// for the same column.
func int96EpochNanos(v [3]uint32) int64 {
	const julianUnixEpoch = 2440588
	const nanosPerDay = 24 * 60 * 60 * 1_000_000_000
	nanos := int64(uint64(v[0]) | uint64(v[1])<<32) //nolint:gosec // a damaged count wraps to a wrong number, never past the slice it renders into
	days := int64(v[2]) - julianUnixEpoch
	return days*nanosPerDay + nanos
}

// float16To32 widens an IEEE 754 half-precision float to single precision.
func float16To32(bits uint16) float32 {
	sign := uint32(bits>>15) << 31
	exponent := uint32(bits >> 10 & 0x1f)
	mantissa := uint32(bits & 0x3ff)
	switch exponent {
	case 0:
		if mantissa == 0 {
			return math.Float32frombits(sign) // ±0
		}
		// A subnormal half is a normal float32: shift the mantissa up to its
		// implicit leading bit, lowering the exponent as it goes.
		exponent = 1
		for mantissa&0x400 == 0 {
			mantissa <<= 1
			exponent--
		}
		mantissa &= 0x3ff
	case 0x1f: // infinities and NaN
		return math.Float32frombits(sign | 0x7f800000 | mantissa<<13)
	}
	return math.Float32frombits(sign | (exponent+112)<<23 | mantissa<<13)
}

// decimalText renders a DECIMAL's unscaled integer at its scale: "123.45" for
// 12345 at scale 2, with a leading zero when the value is smaller than its
// scale.
func decimalText(unscaled *big.Int, scale int32) string {
	if scale <= 0 {
		if scale < 0 {
			unscaled = new(big.Int).Mul(unscaled,
				new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-scale)), nil))
		}
		return unscaled.String()
	}
	sign := ""
	digits := unscaled.String()
	if strings.HasPrefix(digits, "-") {
		sign, digits = "-", digits[1:]
	}
	if len(digits) <= int(scale) {
		digits = strings.Repeat("0", int(scale)-len(digits)+1) + digits
	}
	point := len(digits) - int(scale)
	return sign + digits[:point] + "." + digits[point:]
}

// twosComplementBig reads a big-endian two's-complement integer, which is how
// DECIMAL stores its unscaled value in a byte array.
func twosComplementBig(b []byte) *big.Int {
	n := new(big.Int).SetBytes(b)
	if len(b) > 0 && b[0]&0x80 != 0 {
		n.Sub(n, new(big.Int).Lsh(big.NewInt(1), uint(len(b)*8)))
	}
	return n
}

// uuidText renders sixteen bytes in the canonical 8-4-4-4-12 form.
func uuidText(b []byte) string {
	h := hex.EncodeToString(b)
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// SQLiteFloatText renders a float at bitSize so SQLite's REAL affinity converts
// it back to the same number, which "%g" does not for the three values that have
// no decimal spelling. It is what a load binds and what a dump writes, so a
// value that survives one survives the other.
//
// The column is declared REAL from the Parquet schema, and SQLite applies that
// affinity to the text an import binds: "+Inf" is not a number to it, so the
// cell was stored as TEXT inside a REAL column and typeof() answered "text" for
// a value the file held as a double. "9e999" overflows to infinity when SQLite
// parses it, which is the only spelling that survives.
//
// NaN renders as empty, the same as a null, because SQLite has no NaN at all: a
// computed one becomes NULL there, so NULL is what the value already means in
// the destination. Keeping the word would leave the same TEXT-in-a-REAL-column
// mismatch this exists to remove.
func SQLiteFloatText(f float64, bitSize int) string {
	return floatText(f, bitSize, RenderSQLite)
}

// floatText is SQLiteFloatText with the ".0" suffix left off for a caller that
// renders a value rather than storing it in a typed column.
func floatText(f float64, bitSize int, rendering Rendering) string {
	// A literal SQLite overflows to an infinity while parsing it. There is no
	// spelling of the value itself that its REAL affinity accepts.
	const infinityLiteral = "9e999"
	switch {
	case math.IsInf(f, 1):
		return infinityLiteral
	case math.IsInf(f, -1):
		return "-" + infinityLiteral
	case math.IsNaN(f):
		return ""
	}
	text := strconv.FormatFloat(f, 'g', -1, bitSize)
	// A whole number renders with neither a point nor an exponent, and read back
	// that spelling is an integer. The suffix is what keeps a loaded column REAL;
	// a caller that only renders the value has no column to keep.
	if rendering == RenderSQLite && !strings.ContainsAny(text, ".eE") {
		text += ".0"
	}
	return text
}

// boolText spells a boolean the way its column is declared: 1 and 0 for the
// INTEGER column a load declares, and the words otherwise.
func boolText(v bool, rendering Rendering) string {
	if rendering == RenderSQLite {
		if v {
			return "1"
		}
		return "0"
	}
	if v {
		return "true"
	}
	return "false"
}
