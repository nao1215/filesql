package frame

import (
	"cmp"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/nao1215/filesql/parser"
)

// DataFrame is a simple representation of tabular data.
// It stores data in row-oriented format with immediate execution (no lazy evaluation).
type DataFrame struct {
	columns []string         // column names
	rows    []map[string]any // row data
}

func dataFrameFromParseResult(result *parser.TableData) *DataFrame {
	columns := result.Headers
	dfRows := make([]map[string]any, len(result.Records))
	for i, record := range result.Records {
		row := make(map[string]any, len(columns))
		for j, col := range columns {
			if j < len(record) {
				row[col] = convertStringValue(record[j], result.ColumnTypes[j])
			} else {
				row[col] = nil
			}
		}
		dfRows[i] = row
	}

	return &DataFrame{
		columns: columns,
		rows:    dfRows,
	}
}

// copyRow creates a deep copy of a row to prevent shared references.
// This is critical for maintaining immutability of DataFrame operations.
func copyRow(row map[string]any) map[string]any {
	copied := make(map[string]any, len(row))
	for k, v := range row {
		copied[k] = v
	}
	return copied
}

// NewDataFrame creates a DataFrame from an io.Reader.
// It supports CSV, TSV, LTSV, XLSX, and Parquet formats.
//
// Example:
//
//	f, _ := os.Open("data.csv")
//	defer f.Close()
//	df, err := frame.NewDataFrame(f, frame.CSV)
func NewDataFrame(reader io.Reader, fileType FileType) (*DataFrame, error) {
	if reader == nil {
		return nil, errors.New("reader cannot be nil")
	}

	// Use the integrated parser package directly.
	result, err := parser.Parse(reader, fileType)
	if err != nil {
		return nil, fmt.Errorf("failed to parse: %w", err)
	}

	return dataFrameFromParseResult(result), nil
}

// NewDataFrameFromPath creates a DataFrame from a file path.
// It automatically detects the file type and handles compressed files
// (gzip, bzip2, xz, zstd, zlib, snappy, s2, lz4).
//
// Supported formats: CSV, TSV, LTSV, XLSX, Parquet, and their compressed variants.
// For XLSX files with multiple sheets, the first sheet is used.
//
// Example:
//
//	df, err := frame.NewDataFrameFromPath("data.csv.gz")
//	df, err := frame.NewDataFrameFromPath("data.csv.snappy")
func NewDataFrameFromPath(path string) (*DataFrame, error) {
	// Detect file type from path
	fileType := parser.DetectFileType(path)
	switch parser.BaseFileType(fileType) {
	case parser.CSV, parser.TSV, parser.LTSV, parser.Parquet, parser.XLSX:
		// Supported by frame.
	default:
		return nil, fmt.Errorf("unsupported file type: %s", path)
	}

	// Open file for parsing; parser handles decompression internally.
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	result, err := parser.Parse(file, fileType)
	if err != nil {
		return nil, fmt.Errorf("failed to parse: %w", err)
	}

	return dataFrameFromParseResult(result), nil
}

// convertStringValue converts a string value to the appropriate Go type based on
// ColumnType.
//
// The column's type is decided from a sample, so a value that only text holds
// can arrive in a column already called numeric — a zero-padded code below the
// codes the sample saw. Such a value keeps its text form: the type says what the
// column mostly is, and the value says what it is.
//
// Losslessness is asked of the integer conversion by rendering it back. It is
// not asked of the real one: "1.50" is the real 1.5 here as it is everywhere
// else in filesql, because the quantity survives and only the way it was
// written does not.
func convertStringValue(s string, ct parser.ColumnType) any {
	switch ct {
	case parser.TypeInteger:
		if s == "" {
			return s
		}
		if i, err := strconv.ParseInt(s, 10, 64); err == nil && strconv.FormatInt(i, 10) == s {
			return i
		}
		return s
	case parser.TypeReal:
		if s == "" {
			return s
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f
		}
		return s
	default:
		return s
	}
}

// NewDataFrameFromRecords creates a DataFrame from a slice of maps.
// Each map represents a row with column names as keys.
// Column order is determined by processing records in order, and within each record,
// keys are sorted alphabetically. New columns are appended as they are encountered.
//
// Example:
//
//	records := []map[string]any{
//	    {"name": "Alice", "age": 30},
//	    {"name": "Bob", "age": 25},
//	}
//	df := frame.NewDataFrameFromRecords(records)
func NewDataFrameFromRecords(records []map[string]any) *DataFrame {
	if len(records) == 0 {
		return &DataFrame{
			columns: []string{},
			rows:    []map[string]any{},
		}
	}

	// Extract column names preserving first-seen order
	var columns []string
	columnSeen := make(map[string]struct{})
	for _, record := range records {
		// For deterministic iteration within each record, sort keys
		recordKeys := make([]string, 0, len(record))
		for col := range record {
			recordKeys = append(recordKeys, col)
		}
		slices.Sort(recordKeys)

		for _, col := range recordKeys {
			if _, seen := columnSeen[col]; !seen {
				columnSeen[col] = struct{}{}
				columns = append(columns, col)
			}
		}
	}

	// Copy all rows to ensure immutability
	rows := make([]map[string]any, len(records))
	for i, record := range records {
		rows[i] = copyRow(record)
	}

	return &DataFrame{
		columns: columns,
		rows:    rows,
	}
}

// Columns returns a copy of the column names.
func (df *DataFrame) Columns() []string {
	result := make([]string, len(df.columns))
	copy(result, df.columns)
	return result
}

// Len returns the number of rows in the DataFrame.
func (df *DataFrame) Len() int {
	return len(df.rows)
}

// ToRecords returns the data as a slice of maps.
// Each map is a copy to ensure immutability.
func (df *DataFrame) ToRecords() []map[string]any {
	result := make([]map[string]any, len(df.rows))
	for i, row := range df.rows {
		result[i] = copyRow(row)
	}
	return result
}

// ToCSV writes the DataFrame to a CSV file.
//
// Example:
//
//	err := df.ToCSV("output.csv")
func (df *DataFrame) ToCSV(path string) error {
	return df.toDelimitedFile(path, ',')
}

// ToTSV writes the DataFrame to a TSV file.
//
// Example:
//
//	err := df.ToTSV("output.tsv")
func (df *DataFrame) ToTSV(path string) error {
	return df.toDelimitedFile(path, '\t')
}

// toDelimitedFile writes the DataFrame to a file with the specified delimiter.
func (df *DataFrame) toDelimitedFile(path string, delimiter rune) error {
	f, err := os.Create(path) //nolint:gosec // path is provided by the user
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer f.Close()

	writer := csv.NewWriter(f)
	writer.Comma = delimiter

	// Write header
	if err := writer.Write(df.columns); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Write rows
	for _, row := range df.rows {
		record := make([]string, len(df.columns))
		for i, col := range df.columns {
			record[i] = formatValue(row[col])
		}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write row: %w", err)
		}
	}

	// Flush buffered data and check for errors
	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("failed to flush writer: %w", err)
	}

	return nil
}

// formatValue converts a value to its string representation for CSV output.
func formatValue(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}

// Select returns a new DataFrame with only the specified columns.
// Columns that do not exist are silently ignored.
//
// Example:
//
//	selected := df.Select("name", "age")
func (df *DataFrame) Select(columns ...string) *DataFrame {
	if len(columns) == 0 {
		return &DataFrame{
			columns: []string{},
			rows:    []map[string]any{},
		}
	}

	// Filter to only existing columns, preserving order
	existingCols := make(map[string]struct{}, len(df.columns))
	for _, col := range df.columns {
		existingCols[col] = struct{}{}
	}

	selectedCols := make([]string, 0, len(columns))
	for _, col := range columns {
		if _, exists := existingCols[col]; exists {
			selectedCols = append(selectedCols, col)
		}
	}

	// Create new rows with only selected columns
	newRows := make([]map[string]any, len(df.rows))
	for i, row := range df.rows {
		newRow := make(map[string]any, len(selectedCols))
		for _, col := range selectedCols {
			newRow[col] = row[col]
		}
		newRows[i] = newRow
	}

	return &DataFrame{
		columns: selectedCols,
		rows:    newRows,
	}
}

// Filter returns a new DataFrame containing only rows that satisfy the predicate.
// The predicate function receives a copy of each row to prevent accidental mutation
// of the original DataFrame.
//
// Example:
//
//	filtered := df.Filter(func(row map[string]any) bool {
//	    age, ok := row["age"].(int64)
//	    return ok && age >= 18
//	})
func (df *DataFrame) Filter(fn func(row map[string]any) bool) *DataFrame {
	if fn == nil {
		return df.clone()
	}

	var filtered []map[string]any
	for _, row := range df.rows {
		// Pass a copy to the predicate to prevent mutation of original data
		if fn(copyRow(row)) {
			filtered = append(filtered, copyRow(row))
		}
	}

	columns := make([]string, len(df.columns))
	copy(columns, df.columns)

	return &DataFrame{
		columns: columns,
		rows:    filtered,
	}
}

// Mutate returns a new DataFrame with a new or modified column.
// The function receives a copy of each row and returns the value for the new column.
// The original DataFrame is not modified.
//
// If the column name is empty or the function is nil, Mutate returns a clone
// of the original DataFrame without any modifications.
//
// Example:
//
//	mutated := df.Mutate("full_name", func(row map[string]any) any {
//	    first := row["first_name"].(string)
//	    last := row["last_name"].(string)
//	    return first + " " + last
//	})
func (df *DataFrame) Mutate(column string, fn func(row map[string]any) any) *DataFrame {
	if fn == nil || column == "" {
		return df.clone()
	}

	newRows := make([]map[string]any, len(df.rows))
	for i, row := range df.rows {
		newRow := copyRow(row)
		// Pass a copy to the function to prevent mutation of original data
		newRow[column] = fn(copyRow(row))
		newRows[i] = newRow
	}

	// Update columns list if new column
	isNewColumn := true
	for _, col := range df.columns {
		if col == column {
			isNewColumn = false
			break
		}
	}

	var newColumns []string
	if isNewColumn {
		newColumns = make([]string, len(df.columns), len(df.columns)+1)
		copy(newColumns, df.columns)
		newColumns = append(newColumns, column)
	} else {
		newColumns = make([]string, len(df.columns))
		copy(newColumns, df.columns)
	}

	return &DataFrame{
		columns: newColumns,
		rows:    newRows,
	}
}

// clone creates a deep copy of the DataFrame.
func (df *DataFrame) clone() *DataFrame {
	columns := make([]string, len(df.columns))
	copy(columns, df.columns)

	rows := make([]map[string]any, len(df.rows))
	for i, row := range df.rows {
		rows[i] = copyRow(row)
	}

	return &DataFrame{
		columns: columns,
		rows:    rows,
	}
}

// JoinType represents the type of join operation.
// Four join types are supported: InnerJoin, LeftJoin, RightJoin, and OuterJoin.
type JoinType int

const (
	// InnerJoin returns only rows that have matching values in both DataFrames.
	// This is the most restrictive join type - rows without matches are excluded.
	//
	// Example: If users has ids [1, 2, 3] and orders has user_ids [1, 2, 4],
	// an inner join returns only rows for users 1 and 2.
	InnerJoin JoinType = iota

	// LeftJoin returns all rows from the left DataFrame and matched rows from the right DataFrame.
	// For left rows without matches, the right columns will have nil values.
	//
	// Example: If users has ids [1, 2, 3] and orders has user_ids [1, 2],
	// a left join returns all 3 users, with user 3 having nil for order columns.
	LeftJoin

	// RightJoin returns all rows from the right DataFrame and matched rows from the left DataFrame.
	// For right rows without matches, the left columns will have nil values.
	//
	// Example: If users has ids [1, 2] and orders has user_ids [1, 2, 3],
	// a right join returns all 3 orders, with order 3 having nil for user columns.
	RightJoin

	// OuterJoin returns all rows from both DataFrames.
	// Unmatched rows will have nil values for columns from the other DataFrame.
	// This is the most inclusive join type - no rows are excluded.
	//
	// Example: If users has ids [1, 2] and orders has user_ids [2, 3],
	// an outer join returns users 1, 2 and orders 2, 3 (4 rows total).
	OuterJoin
)

// JoinOption specifies options for the Join operation.
//
// On field specifies the join column(s):
//   - One column: Used for both DataFrames (e.g., On: []string{"id"})
//   - Two columns: First for left DataFrame, second for right (e.g., On: []string{"id", "user_id"})
//
// How field specifies the join type (InnerJoin, LeftJoin, RightJoin, OuterJoin).
//
// Example:
//
//	// Same column name in both DataFrames
//	opt := frame.JoinOption{On: []string{"id"}, How: frame.InnerJoin}
//
//	// Different column names
//	opt := frame.JoinOption{On: []string{"id", "user_id"}, How: frame.LeftJoin}
type JoinOption struct {
	// On specifies the column(s) to join on.
	// If one column is specified, it is used for both DataFrames.
	// If two columns are specified, the first is for the left DataFrame and the second for the right.
	On []string
	// How specifies the type of join (InnerJoin, LeftJoin, RightJoin, OuterJoin).
	How JoinType
}

// Join combines two DataFrames based on a common column or column pair.
// This method enables SQL-like join operations between DataFrames.
//
// Join Types:
//   - InnerJoin: Returns only matching rows from both DataFrames
//   - LeftJoin: Returns all left rows, with nil for unmatched right columns
//   - RightJoin: Returns all right rows, with nil for unmatched left columns
//   - OuterJoin: Returns all rows from both, with nil for unmatched columns
//
// Column Handling:
//   - The join column from the right DataFrame is excluded from the result
//   - Conflicting column names are prefixed with "right_"
//   - Result column order: left columns first, then right columns
//
// Limitations:
//   - Currently supports joining on a single column pair (1 or 2 columns in On)
//   - For complex joins with multiple keys, consider using filesql
//
// Example - Inner Join with same column name:
//
//	users := frame.NewDataFrameFromRecords([]map[string]any{
//	    {"id": 1, "name": "Alice"},
//	    {"id": 2, "name": "Bob"},
//	})
//	orders := frame.NewDataFrameFromRecords([]map[string]any{
//	    {"id": 1, "product": "Laptop"},
//	    {"id": 1, "product": "Mouse"},
//	})
//	result, err := users.Join(orders, frame.JoinOption{
//	    On:  []string{"id"},
//	    How: frame.InnerJoin,
//	})
//	// Result: [{id:1, name:Alice, product:Laptop}, {id:1, name:Alice, product:Mouse}]
//
// Example - Left Join with different column names:
//
//	users := frame.NewDataFrameFromRecords([]map[string]any{
//	    {"user_id": 1, "name": "Alice"},
//	    {"user_id": 2, "name": "Bob"},
//	    {"user_id": 3, "name": "Charlie"},
//	})
//	orders := frame.NewDataFrameFromRecords([]map[string]any{
//	    {"customer_id": 1, "product": "Laptop"},
//	})
//	result, err := users.Join(orders, frame.JoinOption{
//	    On:  []string{"user_id", "customer_id"},
//	    How: frame.LeftJoin,
//	})
//	// Result includes all 3 users; Bob and Charlie have nil for product
func (df *DataFrame) Join(other *DataFrame, opt JoinOption) (*DataFrame, error) {
	if other == nil {
		return nil, errors.New("other DataFrame cannot be nil")
	}
	if len(opt.On) == 0 {
		return nil, errors.New("join requires at least one column")
	}
	if len(opt.On) > 2 {
		return nil, errors.New("join on more than 2 columns is not supported yet")
	}

	// Determine left and right join columns
	leftCol := opt.On[0]
	rightCol := leftCol
	if len(opt.On) == 2 {
		rightCol = opt.On[1]
	}

	// Validate columns exist
	if !df.hasColumn(leftCol) {
		return nil, fmt.Errorf("column %q not found in left DataFrame", leftCol)
	}
	if !other.hasColumn(rightCol) {
		return nil, fmt.Errorf("column %q not found in right DataFrame", rightCol)
	}

	// Build index for right DataFrame, mapping key -> (row index, row data)
	type indexedRow struct {
		index int
		row   map[string]any
	}
	rightIndex := make(map[any][]indexedRow)
	for i, row := range other.rows {
		key := row[rightCol]
		rightIndex[key] = append(rightIndex[key], indexedRow{index: i, row: row})
	}

	// Build result columns (left columns + right columns excluding join column)
	rightColsToAdd := make([]string, 0, len(other.columns))
	rightColsOriginal := make([]string, 0, len(other.columns))
	for _, col := range other.columns {
		if col != rightCol {
			rightColsOriginal = append(rightColsOriginal, col)
			// Handle column name conflicts by prefixing with "right_"
			finalCol := col
			if df.hasColumn(col) {
				finalCol = "right_" + col
			}
			rightColsToAdd = append(rightColsToAdd, finalCol)
		}
	}

	resultColumns := make([]string, 0, len(df.columns)+len(rightColsToAdd))
	resultColumns = append(resultColumns, df.columns...)
	resultColumns = append(resultColumns, rightColsToAdd...)

	var resultRows []map[string]any

	// Track which right row indices have been matched (for outer join)
	rightMatched := make(map[int]bool)

	// Process left rows
	for _, leftRow := range df.rows {
		leftKey := leftRow[leftCol]
		indexedRows, found := rightIndex[leftKey]

		if found {
			// Create joined rows and mark right rows as matched
			for _, ir := range indexedRows {
				rightMatched[ir.index] = true
				newRow := copyRow(leftRow)
				for i, col := range rightColsOriginal {
					newRow[rightColsToAdd[i]] = ir.row[col]
				}
				resultRows = append(resultRows, newRow)
			}
		} else if opt.How == LeftJoin || opt.How == OuterJoin {
			// No match, but include left row with nil for right columns
			newRow := copyRow(leftRow)
			for _, col := range rightColsToAdd {
				newRow[col] = nil
			}
			resultRows = append(resultRows, newRow)
		}
	}

	// For right join and outer join, add unmatched right rows
	if opt.How == RightJoin || opt.How == OuterJoin {
		for i, rightRow := range other.rows {
			if !rightMatched[i] {
				newRow := make(map[string]any, len(resultColumns))
				// Set left columns to nil
				for _, col := range df.columns {
					if col == leftCol {
						newRow[col] = rightRow[rightCol]
					} else {
						newRow[col] = nil
					}
				}
				// Set right columns
				for j, col := range rightColsOriginal {
					newRow[rightColsToAdd[j]] = rightRow[col]
				}
				resultRows = append(resultRows, newRow)
			}
		}
	}

	return &DataFrame{
		columns: resultColumns,
		rows:    resultRows,
	}, nil
}

// hasColumn checks if the DataFrame has a column with the given name.
func (df *DataFrame) hasColumn(name string) bool {
	for _, col := range df.columns {
		if col == name {
			return true
		}
	}
	return false
}

// Concat concatenates multiple DataFrames vertically (row-wise).
// This is useful for combining data from multiple sources with the same schema.
//
// Requirements:
//   - All DataFrames must have exactly the same columns in the same order
//   - If columns differ, use ConcatAll instead for flexible concatenation
//
// Returns an error if:
//   - Any DataFrame is nil
//   - Columns don't match (different names or different order)
//
// Example - Combining monthly data:
//
//	jan := frame.NewDataFrameFromRecords([]map[string]any{
//	    {"month": "Jan", "sales": 100},
//	})
//	feb := frame.NewDataFrameFromRecords([]map[string]any{
//	    {"month": "Feb", "sales": 150},
//	})
//	mar := frame.NewDataFrameFromRecords([]map[string]any{
//	    {"month": "Mar", "sales": 200},
//	})
//	quarterly, err := jan.Concat(feb, mar)
//	// Result: 3 rows with all monthly data
//
// Example - Combining data from multiple CSV files:
//
//	df1, _ := frame.NewDataFrameFromPath("data_2024_01.csv")
//	df2, _ := frame.NewDataFrameFromPath("data_2024_02.csv")
//	combined, err := df1.Concat(df2)
func (df *DataFrame) Concat(others ...*DataFrame) (*DataFrame, error) {
	if len(others) == 0 {
		return df.clone(), nil
	}

	// Validate all DataFrames have the same columns
	for i, other := range others {
		if other == nil {
			return nil, fmt.Errorf("DataFrame at index %d is nil", i+1)
		}
		if !slices.Equal(df.columns, other.columns) {
			return nil, fmt.Errorf("DataFrame at index %d has different columns: expected %v, got %v",
				i+1, df.columns, other.columns)
		}
	}

	// Calculate total rows
	totalRows := len(df.rows)
	for _, other := range others {
		totalRows += len(other.rows)
	}

	// Build result
	resultColumns := make([]string, len(df.columns))
	copy(resultColumns, df.columns)

	resultRows := make([]map[string]any, 0, totalRows)

	// Add rows from first DataFrame
	for _, row := range df.rows {
		resultRows = append(resultRows, copyRow(row))
	}

	// Add rows from other DataFrames
	for _, other := range others {
		for _, row := range other.rows {
			resultRows = append(resultRows, copyRow(row))
		}
	}

	return &DataFrame{
		columns: resultColumns,
		rows:    resultRows,
	}, nil
}

// ConcatAll concatenates multiple DataFrames vertically, automatically handling
// different column sets by taking the union of all columns.
// This is a standalone function (not a method) that accepts any number of DataFrames.
//
// Column Handling:
//   - Columns from all DataFrames are collected into a union set
//   - Columns are sorted alphabetically for deterministic output
//   - Missing values in rows are set to nil
//
// Nil DataFrames are silently skipped, making this safe for optional data.
//
// Use Cases:
//   - Combining data from different sources with overlapping schemas
//   - Merging datasets that evolved over time with different columns
//   - Appending new data with additional fields to existing data
//
// Example - Combining data with different schemas:
//
//	users := frame.NewDataFrameFromRecords([]map[string]any{
//	    {"name": "Alice", "age": 30},
//	})
//	contacts := frame.NewDataFrameFromRecords([]map[string]any{
//	    {"name": "Bob", "email": "bob@example.com"},
//	})
//	result, err := frame.ConcatAll(users, contacts)
//	// Result columns: ["age", "email", "name"] (sorted alphabetically)
//	// Alice has nil for email, Bob has nil for age
//
// Example - Combining CSV and TSV data:
//
//	csv, _ := frame.NewDataFrameFromPath("users.csv")
//	tsv, _ := frame.NewDataFrameFromPath("extra_info.tsv")
//	combined, err := frame.ConcatAll(csv, tsv)
func ConcatAll(frames ...*DataFrame) (*DataFrame, error) {
	if len(frames) == 0 {
		return &DataFrame{
			columns: []string{},
			rows:    []map[string]any{},
		}, nil
	}

	// Collect all unique columns
	columnSet := make(map[string]struct{})
	for _, df := range frames {
		if df == nil {
			continue
		}
		for _, col := range df.columns {
			columnSet[col] = struct{}{}
		}
	}

	// Sort columns for deterministic order
	allColumns := make([]string, 0, len(columnSet))
	for col := range columnSet {
		allColumns = append(allColumns, col)
	}
	slices.Sort(allColumns)

	// Calculate total rows
	totalRows := 0
	for _, df := range frames {
		if df != nil {
			totalRows += len(df.rows)
		}
	}

	// Build result rows
	resultRows := make([]map[string]any, 0, totalRows)
	for _, df := range frames {
		if df == nil {
			continue
		}
		for _, row := range df.rows {
			newRow := make(map[string]any, len(allColumns))
			for _, col := range allColumns {
				if val, exists := row[col]; exists {
					newRow[col] = val
				} else {
					newRow[col] = nil
				}
			}
			resultRows = append(resultRows, newRow)
		}
	}

	return &DataFrame{
		columns: allColumns,
		rows:    resultRows,
	}, nil
}

// SortOrder specifies the order for sorting.
type SortOrder int

const (
	// Ascending sorts values from smallest to largest.
	Ascending SortOrder = iota
	// Descending sorts values from largest to smallest.
	Descending
)

// SortOption specifies options for the Sort operation.
type SortOption struct {
	// Column is the column name to sort by.
	Column string
	// Order specifies ascending or descending sort order.
	Order SortOrder
}

// Sort returns a new DataFrame sorted by the specified column.
// Supports sorting by string, int64, and float64 values.
// Nil values are placed at the end regardless of sort order.
//
// Example:
//
//	sorted := df.Sort("age", frame.Ascending)
func (df *DataFrame) Sort(column string, order SortOrder) (*DataFrame, error) {
	if !df.hasColumn(column) {
		return nil, fmt.Errorf("column %q not found", column)
	}

	// Clone rows for sorting
	sortedRows := make([]map[string]any, len(df.rows))
	for i, row := range df.rows {
		sortedRows[i] = copyRow(row)
	}

	// Sort using Go's slices.SortFunc
	slices.SortStableFunc(sortedRows, func(a, b map[string]any) int {
		aVal := a[column]
		bVal := b[column]

		// Handle nil values - always sort to end
		if aVal == nil && bVal == nil {
			return 0
		}
		if aVal == nil {
			return 1
		}
		if bVal == nil {
			return -1
		}

		result := compareValues(aVal, bVal)
		if order == Descending {
			result = -result
		}
		return result
	})

	columns := make([]string, len(df.columns))
	copy(columns, df.columns)

	return &DataFrame{
		columns: columns,
		rows:    sortedRows,
	}, nil
}

// SortBy returns a new DataFrame sorted by multiple columns.
// Columns are sorted in the order specified (first column has highest priority).
//
// Example:
//
//	sorted, err := df.SortBy(
//	    frame.SortOption{Column: "category", Order: frame.Ascending},
//	    frame.SortOption{Column: "price", Order: frame.Descending},
//	)
func (df *DataFrame) SortBy(options ...SortOption) (*DataFrame, error) {
	if len(options) == 0 {
		return df.clone(), nil
	}

	// Validate all columns exist
	for _, opt := range options {
		if !df.hasColumn(opt.Column) {
			return nil, fmt.Errorf("column %q not found", opt.Column)
		}
	}

	// Clone rows for sorting
	sortedRows := make([]map[string]any, len(df.rows))
	for i, row := range df.rows {
		sortedRows[i] = copyRow(row)
	}

	// Sort using multiple columns
	slices.SortFunc(sortedRows, func(a, b map[string]any) int {
		for _, opt := range options {
			aVal := a[opt.Column]
			bVal := b[opt.Column]

			// Handle nil values - always sort to end
			if aVal == nil && bVal == nil {
				continue
			}
			if aVal == nil {
				return 1
			}
			if bVal == nil {
				return -1
			}

			result := compareValues(aVal, bVal)
			if opt.Order == Descending {
				result = -result
			}
			if result != 0 {
				return result
			}
		}
		return 0
	})

	columns := make([]string, len(df.columns))
	copy(columns, df.columns)

	return &DataFrame{
		columns: columns,
		rows:    sortedRows,
	}, nil
}

// compareValues compares two values and returns -1, 0, or 1.
// Supports string, int64, float64 comparisons.
func compareValues(a, b any) int {
	switch aTyped := a.(type) {
	case string:
		if bTyped, ok := b.(string); ok {
			return cmp.Compare(aTyped, bTyped)
		}
	case int64:
		switch bTyped := b.(type) {
		case int64:
			return cmp.Compare(aTyped, bTyped)
		case float64:
			return cmp.Compare(float64(aTyped), bTyped)
		}
	case float64:
		switch bTyped := b.(type) {
		case float64:
			return cmp.Compare(aTyped, bTyped)
		case int64:
			return cmp.Compare(aTyped, float64(bTyped))
		}
	case int:
		switch bTyped := b.(type) {
		case int:
			return cmp.Compare(aTyped, bTyped)
		case int64:
			return cmp.Compare(int64(aTyped), bTyped)
		case float64:
			return cmp.Compare(float64(aTyped), bTyped)
		}
	}
	// Fallback: compare string representations
	return cmp.Compare(fmt.Sprintf("%v", a), fmt.Sprintf("%v", b))
}

// Distinct returns a new DataFrame with duplicate rows removed.
// Two rows are considered duplicates if all their column values are equal.
//
// Example:
//
//	unique := df.Distinct()
func (df *DataFrame) Distinct() *DataFrame {
	return df.DistinctBy(df.columns...)
}

// DistinctBy returns a new DataFrame with duplicate rows removed based on
// the specified columns only.
//
// Example:
//
//	unique := df.DistinctBy("name", "email")
func (df *DataFrame) DistinctBy(columns ...string) *DataFrame {
	if len(columns) == 0 {
		return df.clone()
	}

	seen := make(map[string]struct{})
	var uniqueRows []map[string]any

	for _, row := range df.rows {
		key := makeRowKey(row, columns)
		if _, exists := seen[key]; !exists {
			seen[key] = struct{}{}
			uniqueRows = append(uniqueRows, copyRow(row))
		}
	}

	resultColumns := make([]string, len(df.columns))
	copy(resultColumns, df.columns)

	return &DataFrame{
		columns: resultColumns,
		rows:    uniqueRows,
	}
}

// makeRowKey creates a string key from row values for the specified columns.
func makeRowKey(row map[string]any, columns []string) string {
	parts := make([]string, len(columns))
	for i, col := range columns {
		parts[i] = fmt.Sprintf("%v", row[col])
	}
	// Use null byte as separator to avoid collisions
	return strings.Join(parts, "\x00")
}

// Head returns a new DataFrame with the first n rows.
// If n is greater than the number of rows, all rows are returned.
// If n is negative, returns an empty DataFrame.
//
// Example:
//
//	first10 := df.Head(10)
func (df *DataFrame) Head(n int) *DataFrame {
	if n <= 0 {
		return &DataFrame{
			columns: append([]string{}, df.columns...),
			rows:    []map[string]any{},
		}
	}
	if n > len(df.rows) {
		n = len(df.rows)
	}

	rows := make([]map[string]any, n)
	for i := range n {
		rows[i] = copyRow(df.rows[i])
	}

	columns := make([]string, len(df.columns))
	copy(columns, df.columns)

	return &DataFrame{
		columns: columns,
		rows:    rows,
	}
}

// Tail returns a new DataFrame with the last n rows.
// If n is greater than the number of rows, all rows are returned.
// If n is negative, returns an empty DataFrame.
//
// Example:
//
//	last10 := df.Tail(10)
func (df *DataFrame) Tail(n int) *DataFrame {
	if n <= 0 {
		return &DataFrame{
			columns: append([]string{}, df.columns...),
			rows:    []map[string]any{},
		}
	}
	if n > len(df.rows) {
		n = len(df.rows)
	}

	start := len(df.rows) - n
	rows := make([]map[string]any, n)
	for i := range n {
		rows[i] = copyRow(df.rows[start+i])
	}

	columns := make([]string, len(df.columns))
	copy(columns, df.columns)

	return &DataFrame{
		columns: columns,
		rows:    rows,
	}
}

// Limit is an alias for Head. Returns a new DataFrame with the first n rows.
//
// Example:
//
//	limited := df.Limit(100)
func (df *DataFrame) Limit(n int) *DataFrame {
	return df.Head(n)
}

// Drop returns a new DataFrame with the specified columns removed.
// Columns that do not exist are silently ignored.
//
// Example:
//
//	dropped := df.Drop("temp_col", "debug_col")
func (df *DataFrame) Drop(columns ...string) *DataFrame {
	if len(columns) == 0 {
		return df.clone()
	}

	// Create set of columns to drop
	dropSet := make(map[string]struct{}, len(columns))
	for _, col := range columns {
		dropSet[col] = struct{}{}
	}

	// Build new column list
	var newColumns []string
	for _, col := range df.columns {
		if _, drop := dropSet[col]; !drop {
			newColumns = append(newColumns, col)
		}
	}

	// Build new rows with only remaining columns
	newRows := make([]map[string]any, len(df.rows))
	for i, row := range df.rows {
		newRow := make(map[string]any, len(newColumns))
		for _, col := range newColumns {
			newRow[col] = row[col]
		}
		newRows[i] = newRow
	}

	return &DataFrame{
		columns: newColumns,
		rows:    newRows,
	}
}

// Rename returns a new DataFrame with the specified column renamed.
// Returns an error if the old column does not exist or if the new column name
// already exists.
//
// Example:
//
//	renamed, err := df.Rename("old_name", "new_name")
func (df *DataFrame) Rename(oldName, newName string) (*DataFrame, error) {
	if oldName == newName {
		return df.clone(), nil
	}

	if !df.hasColumn(oldName) {
		return nil, fmt.Errorf("column %q not found", oldName)
	}
	if df.hasColumn(newName) {
		return nil, fmt.Errorf("column %q already exists", newName)
	}

	// Build new column list
	newColumns := make([]string, len(df.columns))
	for i, col := range df.columns {
		if col == oldName {
			newColumns[i] = newName
		} else {
			newColumns[i] = col
		}
	}

	// Build new rows with renamed column
	newRows := make([]map[string]any, len(df.rows))
	for i, row := range df.rows {
		newRow := make(map[string]any, len(row))
		for k, v := range row {
			if k == oldName {
				newRow[newName] = v
			} else {
				newRow[k] = v
			}
		}
		newRows[i] = newRow
	}

	return &DataFrame{
		columns: newColumns,
		rows:    newRows,
	}, nil
}

// RenameColumns returns a new DataFrame with multiple columns renamed.
// The renames map specifies old name -> new name mappings.
// Returns an error if any old column does not exist or if any new name conflicts.
//
// Example:
//
//	renamed, err := df.RenameColumns(map[string]string{
//	    "col1": "column_one",
//	    "col2": "column_two",
//	})
func (df *DataFrame) RenameColumns(renames map[string]string) (*DataFrame, error) {
	if len(renames) == 0 {
		return df.clone(), nil
	}

	// Validate all old columns exist and no conflicts
	newNameSet := make(map[string]struct{})
	for oldName, newName := range renames {
		if !df.hasColumn(oldName) {
			return nil, fmt.Errorf("column %q not found", oldName)
		}
		if _, exists := newNameSet[newName]; exists {
			return nil, fmt.Errorf("duplicate new column name %q", newName)
		}
		newNameSet[newName] = struct{}{}
	}

	// Check new names don't conflict with existing columns (that aren't being renamed)
	for _, col := range df.columns {
		if _, beingRenamed := renames[col]; beingRenamed {
			continue
		}
		if _, conflicts := newNameSet[col]; conflicts {
			return nil, fmt.Errorf("column %q already exists", col)
		}
	}

	// Build new column list
	newColumns := make([]string, len(df.columns))
	for i, col := range df.columns {
		if newName, ok := renames[col]; ok {
			newColumns[i] = newName
		} else {
			newColumns[i] = col
		}
	}

	// Build new rows with renamed columns
	newRows := make([]map[string]any, len(df.rows))
	for i, row := range df.rows {
		newRow := make(map[string]any, len(row))
		for k, v := range row {
			if newName, ok := renames[k]; ok {
				newRow[newName] = v
			} else {
				newRow[k] = v
			}
		}
		newRows[i] = newRow
	}

	return &DataFrame{
		columns: newColumns,
		rows:    newRows,
	}, nil
}

// DropNA returns a new DataFrame with rows containing missing values removed.
// By default, removes rows where any column has a nil value or an empty string.
//
// Example:
//
//	cleaned := df.DropNA()
func (df *DataFrame) DropNA() *DataFrame {
	return df.DropNASubset(df.columns...)
}

// DropNASubset returns a new DataFrame with rows removed where any of the
// specified columns have missing values.
// Missing values are treated as nil or the empty string.
//
// Example:
//
//	cleaned := df.DropNASubset("required_field1", "required_field2")
func (df *DataFrame) DropNASubset(columns ...string) *DataFrame {
	if len(columns) == 0 {
		return df.clone()
	}

	var validRows []map[string]any
	for _, row := range df.rows {
		hasNA := false
		for _, col := range columns {
			if val, exists := row[col]; !exists || val == nil || val == "" {
				hasNA = true
				break
			}
		}
		if !hasNA {
			validRows = append(validRows, copyRow(row))
		}
	}

	resultColumns := make([]string, len(df.columns))
	copy(resultColumns, df.columns)

	return &DataFrame{
		columns: resultColumns,
		rows:    validRows,
	}
}

// FillNA returns a new DataFrame with nil values replaced by the specified value.
//
// Example:
//
//	filled := df.FillNA(0)  // Replace all nil with 0
func (df *DataFrame) FillNA(value any) *DataFrame {
	newRows := make([]map[string]any, len(df.rows))
	for i, row := range df.rows {
		newRow := make(map[string]any, len(df.columns))
		for _, col := range df.columns {
			if val, exists := row[col]; !exists || val == nil {
				newRow[col] = value
			} else {
				newRow[col] = val
			}
		}
		newRows[i] = newRow
	}

	columns := make([]string, len(df.columns))
	copy(columns, df.columns)

	return &DataFrame{
		columns: columns,
		rows:    newRows,
	}
}

// FillNAByColumn returns a new DataFrame with nil values replaced by column-specific values.
// Columns not in the map retain their nil values.
//
// Example:
//
//	filled := df.FillNAByColumn(map[string]any{
//	    "age":    0,
//	    "name":   "Unknown",
//	    "active": false,
//	})
func (df *DataFrame) FillNAByColumn(values map[string]any) *DataFrame {
	if len(values) == 0 {
		return df.clone()
	}

	newRows := make([]map[string]any, len(df.rows))
	for i, row := range df.rows {
		newRow := make(map[string]any, len(df.columns))
		for _, col := range df.columns {
			val := row[col]
			if val == nil {
				if fillValue, ok := values[col]; ok {
					newRow[col] = fillValue
				} else {
					newRow[col] = nil
				}
			} else {
				newRow[col] = val
			}
		}
		newRows[i] = newRow
	}

	columns := make([]string, len(df.columns))
	copy(columns, df.columns)

	return &DataFrame{
		columns: columns,
		rows:    newRows,
	}
}
