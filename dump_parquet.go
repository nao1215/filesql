package filesql

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"strconv"
	"strings"

	"github.com/nao1215/filesql/internal/infer"
	"github.com/nao1215/filesql/internal/reader"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// writeParquetTableData writes SQLite table data to Parquet format
func writeParquetTableData(w io.Writer, columns []string, rows *sql.Rows, prior parquetPrior) error {
	if len(columns) == 0 {
		return fmt.Errorf("%w: no columns defined", ErrEmptyData)
	}

	// The declared types are read before the scan loop. Draining Rows closes it,
	// and ColumnTypes on a closed Rows fails, which would leave a table with no
	// rows with nothing to take its schema from.
	declared := make([]string, len(columns))
	if types, err := rows.ColumnTypes(); err == nil {
		for i, ct := range types {
			if i < len(declared) {
				declared[i] = ct.DatabaseTypeName()
			}
		}
	}

	// Read all rows into memory first. The raw driver values are kept rather than
	// their rendered text because Parquet is a typed format: which Parquet type
	// each column is written as is decided from the values themselves, below.
	var allRows [][]any

	// Prepare for scanning
	values := make([]any, len(columns))
	scanArgs := make([]any, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("%w: failed to scan row: %w", ErrDatabaseOperation, err)
		}
		row := make([]any, len(columns))
		copy(row, values)
		allRows = append(allRows, row)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("%w: error iterating rows: %w", ErrDatabaseOperation, err)
	}

	return writeParquetData(w, columns, allRows, declared, prior)
}

// parquetPrior is the schema of the Parquet file a save is replacing, by column
// name. It is nil for an export, which has no file to keep faith with.
type parquetPrior map[string]parquet.Node

// readParquetPrior reads the schema of the file a save is about to replace.
//
// A Parquet file's types are the reason it is one, and nothing in the database
// remembers them, so the file is asked before it is replaced. Only the footer is
// read, straight from the file on disk.
//
// A file that cannot be read is not an error here: the save replaces it either
// way, and the columns are then written the way an export writes them.
func readParquetPrior(path string) (prior parquetPrior) {
	defer func() {
		// A damaged schema panics when its nodes are asked about themselves.
		if r := recover(); r != nil {
			prior = nil
		}
	}()
	file, err := os.Open(path) //nolint:gosec // the path is the caller's own source file
	if err != nil {
		return nil
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return nil
	}
	parquetFile, err := parquet.OpenFile(file, info.Size())
	if err != nil {
		return nil
	}
	fields := parquetFile.Schema().Fields()
	prior = make(parquetPrior, len(fields))
	for _, field := range fields {
		prior[field.Name()] = field
	}
	return prior
}

// keptParquetColumn returns the node a column is written with, the values
// rebuilt for it, and whether the file being replaced declared a type this
// package can write again.
//
// The type has to survive the round trip the values already made: each value is
// rebuilt from the text the load produced and rendered back the way the load
// renders it, and the column keeps its type only when every row comes back as
// itself. A row that does not is a value the caller set that the type cannot
// hold, and narrowing it would write a different number into their file --
// worse than writing the file with a plainer schema, which is what happens
// instead.
func keptParquetColumn(node parquet.Node, rows [][]any, col int) (parquet.Node, []parquet.Value, bool) {
	if node == nil || !node.Leaf() || node.Repeated() || !reproducibleParquetType(node.Type()) {
		return nil, nil, false
	}
	renderer, ok := reader.NewParquetRenderer(node)
	if !ok {
		return nil, nil, false
	}
	// The rebuilt values are kept rather than built again when the rows are
	// written: they were built here to be checked, and building them twice
	// would read every cell of the table twice.
	rebuilt := make([]parquet.Value, len(rows))
	holdsNull := false
	for r, row := range rows {
		var value any
		if col < len(row) {
			value = row[col]
		}
		text, held := parquetCellText(value)
		if !held {
			holdsNull = true
			rebuilt[r] = parquet.NullValue()
			continue
		}
		cell, ok := parquetValueOf(node, text)
		if !ok {
			return nil, nil, false
		}
		back, held := renderer.Text(cell)
		if !held || back != text {
			return nil, nil, false
		}
		rebuilt[r] = cell
	}
	leaf := parquet.Leaf(node.Type())
	// A required column cannot hold a null the caller put in it, so the one
	// thing the data demands is the one thing that changes.
	if node.Optional() || holdsNull {
		return parquet.Optional(leaf), rebuilt, true
	}
	return parquet.Required(leaf), rebuilt, true
}

// presentLevel is the definition level a cell is written at: one for a value
// the column holds, zero for a null.
func presentLevel(present bool) int {
	if present {
		return 1
	}
	return 0
}

// parquetCellText is the text a value of the table holds, and whether the cell
// holds anything at all. It is the same reading parquetCellValue does of a null,
// so the two agree about which cells are written.
func parquetCellText(value any) (string, bool) {
	if value == nil {
		return "", false
	}
	if text, isText := value.(string); isText && text == "" {
		return "", false
	}
	return formatDumpValue(value), true
}

// reproducibleParquetType reports whether a value of this type can be rebuilt
// from the text a load renders for it.
//
// A list or a map, an INT96 and a FLOAT16 cannot. A list or a map is rendered as
// text such as "[1 2 3]", which says nothing about the levels underneath it. An
// INT96 is rendered as nanoseconds since the Unix epoch rather than as the
// Julian day and offset it stores. A FLOAT16 is widened to 32 bits to be
// rendered, and this package has no way to write the half-precision bytes back.
// A column of one of those is written the way an export writes it.
func reproducibleParquetType(typ parquet.Type) bool {
	if annotation, ok := parquetAnnotation(typ); ok {
		switch annotation.(type) {
		case *format.Float16Type, *format.MapType, *format.ListType:
			return false
		}
	}
	switch typ.Kind() {
	case parquet.Boolean, parquet.Int32, parquet.Int64, parquet.Float, parquet.Double, parquet.ByteArray:
		return true
	case parquet.FixedLenByteArray:
		// Fixed bytes carry a value this can rebuild only when the annotation
		// says how to read them, or when the text is the bytes themselves.
		return true
	default: // parquet.Int96
		return false
	}
}

// parquetAnnotation is the logical type a node carries, if it carries one.
func parquetAnnotation(typ parquet.Type) (format.LogicalTypeValue, bool) {
	logical := typ.LogicalType()
	if logical == nil || logical.Value == nil {
		return nil, false
	}
	return logical.Value, true
}

// parquetValueOf rebuilds the value a field stores from the text a load of that
// field produced, reporting false for a text the field cannot hold.
func parquetValueOf(node parquet.Node, text string) (parquet.Value, bool) {
	typ := node.Type()
	if annotation, ok := parquetAnnotation(typ); ok {
		switch a := annotation.(type) {
		case *format.DecimalType:
			return parquetDecimalValue(typ, a, text)
		case *format.UUIDType:
			return parquetUUIDValue(text)
		case *format.IntType:
			return parquetIntValue(typ, int(a.BitWidth), a.IsSigned, text)
		}
	}
	switch typ.Kind() {
	case parquet.Boolean:
		switch text {
		case "1":
			return parquet.BooleanValue(true), true
		case "0":
			return parquet.BooleanValue(false), true
		}
		return parquet.Value{}, false
	case parquet.Int32:
		return parquetIntValue(typ, 32, true, text)
	case parquet.Int64:
		return parquetIntValue(typ, 64, true, text)
	case parquet.Float:
		f, ok := parquetFloatOf(text, 32)
		return parquet.FloatValue(float32(f)), ok
	case parquet.Double:
		f, ok := parquetFloatOf(text, 64)
		return parquet.DoubleValue(f), ok
	case parquet.ByteArray:
		return parquet.ByteArrayValue([]byte(text)), true
	case parquet.FixedLenByteArray:
		if len(text) != typ.Length() {
			return parquet.Value{}, false
		}
		return parquet.FixedLenByteArrayValue([]byte(text)), true
	}
	return parquet.Value{}, false
}

// parquetIntValue reads an integer, holding it to the width its annotation
// names. The width matters because nothing downstream enforces it: an INT8
// column takes a 300 without complaint and hands it back as 300, so a check
// that wrote and read again would not see it.
func parquetIntValue(typ parquet.Type, bits int, signed bool, text string) (parquet.Value, bool) {
	if signed {
		n, err := strconv.ParseInt(text, 10, bits)
		if err != nil {
			return parquet.Value{}, false
		}
		if typ.Kind() == parquet.Int32 {
			return parquet.Int32Value(int32(n)), true //nolint:gosec // ParseInt held it to 32 bits or fewer
		}
		return parquet.Int64Value(n), true
	}
	n, err := strconv.ParseUint(text, 10, bits)
	if err != nil {
		return parquet.Value{}, false
	}
	// An unsigned value is stored in the physical integer's bits, which is how
	// the reader reads it back.
	if typ.Kind() == parquet.Int32 {
		return parquet.Int32Value(int32(uint32(n))), true //nolint:gosec // the unsigned value rides in the signed int's bits
	}
	return parquet.Int64Value(int64(n)), true //nolint:gosec // the unsigned value rides in the signed int's bits
}

// parquetFloatOf reads a float, the infinity this package spells as a literal
// among them: ParseFloat answers the infinity and reports the range it left.
func parquetFloatOf(text string, bits int) (float64, bool) {
	f, err := strconv.ParseFloat(text, bits)
	if err == nil {
		return f, true
	}
	if errors.Is(err, strconv.ErrRange) && math.IsInf(f, 0) {
		return f, true
	}
	return 0, false
}

// parquetDecimalValue reads a decimal back into the unscaled integer its column
// stores, at the scale the column names.
func parquetDecimalValue(typ parquet.Type, annotation *format.DecimalType, text string) (parquet.Value, bool) {
	scale := int(annotation.Scale)
	if scale < 0 {
		return parquet.Value{}, false
	}
	negative := strings.HasPrefix(text, "-")
	digits := strings.TrimPrefix(text, "-")
	whole, fraction, _ := strings.Cut(digits, ".")
	if len(fraction) > scale {
		return parquet.Value{}, false
	}
	fraction += strings.Repeat("0", scale-len(fraction))
	unscaled, ok := new(big.Int).SetString(whole+fraction, 10)
	if !ok {
		return parquet.Value{}, false
	}
	// The precision is how many digits the column is declared to hold, and
	// nothing downstream enforces it either.
	if annotation.Precision > 0 && len(strings.TrimLeft(unscaled.String(), "0")) > int(annotation.Precision) {
		return parquet.Value{}, false
	}
	if negative {
		unscaled.Neg(unscaled)
	}
	switch typ.Kind() {
	case parquet.Int32:
		if !unscaled.IsInt64() || unscaled.Int64() < math.MinInt32 || unscaled.Int64() > math.MaxInt32 {
			return parquet.Value{}, false
		}
		return parquet.Int32Value(int32(unscaled.Int64())), true //nolint:gosec // the bound above holds it to 32 bits
	case parquet.Int64:
		if !unscaled.IsInt64() {
			return parquet.Value{}, false
		}
		return parquet.Int64Value(unscaled.Int64()), true
	case parquet.FixedLenByteArray:
		bytes, ok := twosComplementBytes(unscaled, typ.Length())
		if !ok {
			return parquet.Value{}, false
		}
		return parquet.FixedLenByteArrayValue(bytes), true
	}
	return parquet.Value{}, false
}

// twosComplementBytes writes an integer big-endian in two's complement across
// width bytes, which is how DECIMAL stores its unscaled value in fixed bytes.
// It reports false for a value the width cannot hold.
func twosComplementBytes(n *big.Int, width int) ([]byte, bool) {
	if width <= 0 {
		return nil, false
	}
	limit := new(big.Int).Lsh(big.NewInt(1), uint(width*8-1))
	if n.Sign() >= 0 && n.Cmp(limit) >= 0 {
		return nil, false
	}
	if n.Sign() < 0 && new(big.Int).Neg(n).Cmp(limit) > 0 {
		return nil, false
	}
	value := n
	if n.Sign() < 0 {
		value = new(big.Int).Add(n, new(big.Int).Lsh(big.NewInt(1), uint(width*8)))
	}
	out := make([]byte, width)
	value.FillBytes(out)
	return out, true
}

// parquetUUIDValue reads the canonical form back into the sixteen bytes a UUID
// column stores.
func parquetUUIDValue(text string) (parquet.Value, bool) {
	hex := strings.ReplaceAll(text, "-", "")
	if len(hex) != 32 {
		return parquet.Value{}, false
	}
	out := make([]byte, 16)
	for i := range out {
		octet, err := strconv.ParseUint(hex[i*2:i*2+2], 16, 8)
		if err != nil {
			return parquet.Value{}, false
		}
		out[i] = byte(octet)
	}
	return parquet.FixedLenByteArrayValue(out), true
}

// parquetKind is the Parquet type one column is written as.
type parquetKind int

const (
	parquetString parquetKind = iota
	parquetInt
	parquetFloat
)

// parquetColumnKind decides how one column is written. A column is numeric only
// when every value in it is, so a column that mixes a number with text is
// written as STRING rather than losing the text: SQLite types values, not
// columns, and a dump has to carry back whatever the rows actually held.
//
// The declared type decides an empty column, and only an empty one. It is what a
// table emptied by the session still knows about itself, so an auto-save keeps
// the schema instead of rewriting every column as STRING; for a column with rows
// the values are the better witness, because a query's result columns carry no
// declared type at all.
func parquetColumnKind(rows [][]any, col int, declaredType string) parquetKind {
	kind := parquetKind(-1)
	lossy := false // an int64 in this column that float64 cannot carry back
	for _, row := range rows {
		// A blank cell says nothing about the column's type. SQLite stores a
		// blank in a numeric column as the empty string, since "" has no numeric
		// value to convert to, and letting that decide the column wrote a column
		// of numbers as text the moment one row was missing an entry.
		if col >= len(row) || row[col] == nil || row[col] == "" {
			continue
		}
		var cell parquetKind
		switch v := row[col].(type) {
		case int64:
			cell = parquetInt
			lossy = lossy || !infer.Int64SurvivesFloat64(v)
		case float64:
			cell = parquetFloat
		default:
			return parquetString
		}
		switch {
		case kind < 0:
			kind = cell
		case kind != cell:
			// int64 and float64 in one column widen to float64, which holds
			// both while every integer survives a float64 round-trip; one that
			// does not makes the column STRING below, the answer a
			// number-beside-text column already gets, because a dump has to
			// carry back the value and DOUBLE would write a different number.
			kind = parquetFloat
		}
	}
	if kind == parquetFloat && lossy {
		return parquetString
	}
	if kind >= 0 {
		return kind
	}
	switch strings.ToUpper(declaredType) {
	case "INTEGER", "INT", "BIGINT":
		return parquetInt
	case "REAL", "FLOAT", "DOUBLE":
		return parquetFloat
	default:
		return parquetString
	}
}

// parquetNodeFor is the Parquet schema node a parquetKind is written as. Every
// column is optional, so a stored null survives the round-trip.
func parquetNodeFor(kind parquetKind) parquet.Node {
	switch kind {
	case parquetInt:
		return parquet.Optional(parquet.Leaf(parquet.Int64Type))
	case parquetFloat:
		return parquet.Optional(parquet.Leaf(parquet.DoubleType))
	default:
		return parquet.Optional(parquet.String())
	}
}

// orderedGroup is parquet.Group with its fields kept in the order the table
// declares its columns; parquet.Group alone sorts them by name, and a dump
// that reordered its columns would hand every downstream reader a different
// table than the one it was asked to write.
type orderedGroup struct {
	parquet.Group
	names []string
}

// Fields returns the group's fields in declaration order.
func (g orderedGroup) Fields() []parquet.Field {
	sorted := g.Group.Fields()
	byName := make(map[string]parquet.Field, len(sorted))
	for _, field := range sorted {
		byName[field.Name()] = field
	}
	fields := make([]parquet.Field, 0, len(g.names))
	for _, name := range g.names {
		fields = append(fields, byName[name])
	}
	return fields
}

// parquetCellValue renders one cell as the column's Parquet value. ok is false
// for a null: a nil value is the SQL NULL the row carried, and a blank in a
// numeric column is written as the null it means, since a number has no
// spelling for a blank. A text column keeps its empty string. A value that does
// not match a numeric column's kind cannot occur, because parquetColumnKind
// only chooses a numeric kind when every value in the column is numeric.
func parquetCellValue(kind parquetKind, value any) (parquet.Value, bool, error) {
	if value == nil {
		return parquet.Value{}, false, nil
	}
	if text, isText := value.(string); isText && text == "" && kind != parquetString {
		return parquet.Value{}, false, nil
	}
	switch kind {
	case parquetInt:
		n, ok := value.(int64)
		if !ok {
			return parquet.Value{}, false, fmt.Errorf("%w: %T in an integer column", ErrInvalidData, value)
		}
		return parquet.Int64Value(n), true, nil
	case parquetFloat:
		switch v := value.(type) {
		case float64:
			return parquet.DoubleValue(v), true, nil
		case int64:
			return parquet.DoubleValue(float64(v)), true, nil
		default:
			return parquet.Value{}, false, fmt.Errorf("%w: %T in a real column", ErrInvalidData, value)
		}
	default:
		return parquet.ByteArrayValue([]byte(formatDumpValue(value))), true, nil
	}
}

// writeParquetData writes data to Parquet format. A nil cell in rows is stored
// as a Parquet null, so a SQL NULL survives the round-trip instead of collapsing
// into an empty string. declared holds each column's declared SQL type, which
// decides a column that has no rows to speak for it.
//
// Each column is written as the Parquet type its values call for rather than as
// STRING. Parquet states the type of every column in its schema and readers
// trust it, so writing a numeric column as digit strings hands the next tool a
// column it will compare and sort as text.
//
// A table with no rows is written as a schema with no row groups, which is a
// valid Parquet file: the other formats write their header and nothing else, and
// a dump that refused to write an emptied table let an auto-save keep the rows
// the caller had deleted.
func writeParquetData(w io.Writer, columns []string, rows [][]any, declared []string, prior parquetPrior) error {
	if len(columns) == 0 {
		return fmt.Errorf("%w: no columns defined", ErrEmptyData)
	}

	kinds := make([]parquetKind, len(columns))
	// kept[i] is the node the file being replaced declared, for a column this
	// can write again. A column with none is written the way every column was
	// written before this.
	kept := make([]parquet.Node, len(columns))
	keptValues := make([][]parquet.Value, len(columns))
	group := orderedGroup{Group: make(parquet.Group, len(columns)), names: columns}
	for i, col := range columns {
		if node, values, ok := keptParquetColumn(prior[col], rows, i); ok {
			kept[i] = node
			keptValues[i] = values
			group.Group[col] = node
			continue
		}
		declaredType := ""
		if i < len(declared) {
			declaredType = declared[i]
		}
		kinds[i] = parquetColumnKind(rows, i, declaredType)
		group.Group[col] = parquetNodeFor(kinds[i])
	}
	schema := parquet.NewSchema("table", group)

	// The writer takes an io.Writer and never closes it, so the caller keeps
	// ownership of the destination.
	writer := parquet.NewGenericWriter[any](w, schema)

	// Rows are built as Parquet values directly rather than deconstructed from
	// Go values by reflection, which reads a zero value in an optional column
	// as a null and would turn a stored 0 or "" into a missing cell.
	buf := make([]parquet.Row, 0, min(len(rows), 1024))
	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		if _, err := writer.WriteRows(buf); err != nil {
			return fmt.Errorf("%w: failed to write rows to parquet: %w", ErrIOOperation, err)
		}
		buf = buf[:0]
		return nil
	}
	for r, row := range rows {
		out := make(parquet.Row, len(columns))
		for i := range columns {
			if kept[i] != nil {
				// The value was rebuilt and checked when the schema was chosen.
				cell := keptValues[i][r]
				out[i] = cell.Level(0, presentLevel(!cell.IsNull()), i)
				continue
			}
			var value any
			if i < len(row) {
				value = row[i]
			}
			cell, present, err := parquetCellValue(kinds[i], value)
			if err != nil {
				return err
			}
			if present {
				out[i] = cell.Level(0, 1, i)
			} else {
				out[i] = parquet.NullValue().Level(0, 0, i)
			}
		}
		buf = append(buf, out)
		if len(buf) == cap(buf) {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}

	// Close writes the footer, so this is where an incomplete file shows up.
	// For a table with no rows it writes the schema and the footer, which is
	// the whole file.
	if err := writer.Close(); err != nil {
		return fmt.Errorf("%w: failed to close parquet writer: %w", ErrIOOperation, err)
	}
	return nil
}
