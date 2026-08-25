package filesql

import (
	"database/sql"
	"fmt"
	"io"
	"strings"

	"github.com/nao1215/filesql/internal/infer"
	"github.com/parquet-go/parquet-go"
)

// writeParquetTableData writes SQLite table data to Parquet format
func writeParquetTableData(w io.Writer, columns []string, rows *sql.Rows) error {
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

	return writeParquetData(w, columns, allRows, declared)
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
func writeParquetData(w io.Writer, columns []string, rows [][]any, declared []string) error {
	if len(columns) == 0 {
		return fmt.Errorf("%w: no columns defined", ErrEmptyData)
	}

	kinds := make([]parquetKind, len(columns))
	group := orderedGroup{Group: make(parquet.Group, len(columns)), names: columns}
	for i, col := range columns {
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
	for _, row := range rows {
		out := make(parquet.Row, len(columns))
		for i := range columns {
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
