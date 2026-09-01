package prep

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/nao1215/filesql/internal/reader"
	"github.com/nao1215/filesql/internal/writer"
	"github.com/nao1215/filesql/parser"
)

// Processor handles preprocessing and validation of file data
type Processor struct {
	fileType         parser.FileType
	strictTagParsing bool
	validRowsOnly    bool
}

// Option configures a Processor.
type Option func(*Processor)

// WithStrictTagParsing enables strict tag parsing mode.
// When enabled, invalid tag arguments (e.g., "eq=abc" where a number is expected)
// return an error during Process() instead of being silently ignored.
//
// Example:
//
//	processor := prep.NewProcessor(parser.CSV, prep.WithStrictTagParsing())
func WithStrictTagParsing() Option {
	return func(p *Processor) {
		p.strictTagParsing = true
	}
}

// WithValidRowsOnly configures the Processor to include only valid rows
// in the output io.Reader and struct slice. Rows that fail validation are
// excluded from the output but still counted in ProcessResult.RowCount
// and reported in ProcessResult.Errors.
//
// Example:
//
//	processor := prep.NewProcessor(parser.CSV, prep.WithValidRowsOnly())
//	reader, result, err := processor.Process(input, &records)
//	// reader contains only rows that passed all validations
//	// result.RowCount includes all rows, result.ValidRowCount has valid count
func WithValidRowsOnly() Option {
	return func(p *Processor) {
		p.validRowsOnly = true
	}
}

// NewProcessor creates a new Processor for the specified file type.
// Options can be provided to configure behavior such as strict tag parsing
// and output filtering.
//
// Example:
//
//	processor := prep.NewProcessor(parser.CSV)
//	var records []MyRecord
//	reader, result, err := processor.Process(input, &records)
//
//	// With options:
//	processor := prep.NewProcessor(parser.CSV,
//	    prep.WithStrictTagParsing(),
//	    prep.WithValidRowsOnly(),
//	)
func NewProcessor(fileType parser.FileType, opts ...Option) *Processor {
	p := &Processor{
		fileType: fileType,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Process reads from the input reader, applies preprocessing and validation,
// populates the struct slice, and returns an io.Reader with preprocessed data.
//
// The format the reader serves follows the format that was read, and is not
// always the same one:
//   - CSV input → CSV output
//   - TSV input → TSV output (tab-delimited)
//   - LTSV input → LTSV output (label:value format)
//   - JSON input → JSONL output (one JSON value per line)
//   - JSONL input → JSONL output (one JSON value per line)
//   - XLSX input → CSV output (tabular data)
//   - Parquet input → CSV output (tabular data)
//
// ProcessResult.OriginalFormat is the format that was read and
// ProcessResult.OutputFormat is the format of the bytes the reader serves. The
// returned io.Reader can be passed directly to filesql.AddReader, under the
// format the result reports rather than the format that was read:
//
//	reader, result, err := processor.Process(input, &records)
//	var format filesql.FileType
//	switch result.OutputFormat {
//	case prep.FileTypeTSV:
//		format = filesql.FileTypeTSV
//	case prep.FileTypeLTSV:
//		format = filesql.FileTypeLTSV
//	case prep.FileTypeJSONL:
//		format = filesql.FileTypeJSONL
//	default:
//		format = filesql.FileTypeCSV
//	}
//	db.AddReader(reader, "table", format)
//
// Those four are all OutputFormat can be. Naming the input format instead reads
// a JSON file as one JSON document rather than as the JSONL the reader serves,
// and an XLSX or Parquet file as a workbook or a Parquet file rather than as
// the CSV it serves, so the load fails with an error about the bytes rather
// than about the format.
// The reader also satisfies io.Seeker, so it can be rewound with
// Seek(0, io.SeekStart) and read again.
//
// Example:
//
//	type User struct {
//	    Name  string `prep:"trim" validate:"required"`
//	    Email string `prep:"trim,lowercase" validate:"email"`
//	    Age   string `validate:"numeric,min=0,max=150"`
//	}
//
//	csvData := "name,email,age\n  John  ,JOHN@EXAMPLE.COM,30\n"
//	processor := prep.NewProcessor(parser.CSV)
//	var users []User
//	reader, result, err := processor.Process(strings.NewReader(csvData), &users)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Processed %d rows, %d valid\n", result.RowCount, result.ValidRowCount)
func (p *Processor) Process(input io.Reader, structSlicePointer any) (io.Reader, *ProcessResult, error) {
	headers, records, result, err := p.processRecords(input, structSlicePointer)
	if err != nil {
		return nil, nil, err
	}

	// Select output records
	outputRecords := records
	if p.validRowsOnly {
		outputRecords = result.validRecords
	}

	reader, err := p.buildOutput(headers, outputRecords)
	if err != nil {
		return nil, nil, err
	}

	result.validRecords = nil // release; no longer needed
	return reader, result, nil
}

// ProcessToWriter works like Process but writes the preprocessed output
// directly to w instead of buffering it in memory. This is useful for
// large datasets where holding the full output buffer is undesirable.
//
// Example:
//
//	var buf bytes.Buffer
//	result, err := processor.ProcessToWriter(input, &records, &buf)
func (p *Processor) ProcessToWriter(input io.Reader, structSlicePointer any, w io.Writer) (*ProcessResult, error) {
	if w == nil || isNilInterface(w) {
		return nil, ErrNilWriter
	}

	headers, records, result, err := p.processRecords(input, structSlicePointer)
	if err != nil {
		return nil, err
	}

	outputRecords := records
	if p.validRowsOnly {
		outputRecords = result.validRecords
	}

	// Wrap w with a countingWriter so we can detect whether the output format
	// wrote any bytes at all (writeJSONL skips empty records).
	cw := &countingWriter{w: w}
	if err := p.writeOutput(cw, headers, outputRecords); err != nil {
		return nil, fmt.Errorf("failed to write output: %w", err)
	}

	if err := p.refuseEmptyOutput(cw.n == 0); err != nil {
		return nil, err
	}

	result.validRecords = nil
	return result, nil
}

// countingWriter wraps an io.Writer and counts total bytes written.
type countingWriter struct {
	w io.Writer
	n int64
}

// Write implements io.Writer.
func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.n += int64(n)
	return n, err
}

// isNilInterface reports whether v is an interface holding a typed nil pointer.
// This catches cases like: var w io.Writer = (*bytes.Buffer)(nil)
func isNilInterface(v any) bool {
	rv := reflect.ValueOf(v)
	return rv.Kind() == reflect.Pointer && rv.IsNil()
}

// processRecords is the shared core of Process and ProcessToWriter.
// It parses the input, applies preprocessing and validation, populates
// the struct slice, and returns the processed headers, records, structInfo
// and result. The caller is responsible for writing the output.
func (p *Processor) processRecords(input io.Reader, structSlicePointer any) (
	[]string, [][]string, *ProcessResult, error,
) {
	// Get struct type and parse tags
	structType, err := getStructType(structSlicePointer)
	if err != nil {
		return nil, nil, nil, err
	}

	cachedInfo, err := cachedParseStructType(structType, p.strictTagParsing)
	if err != nil {
		return nil, nil, nil, err
	}

	// Copy fields so we can safely mutate ColumnIndex without racing
	// against concurrent callers that share the cached structInfo.
	fields := make([]fieldInfo, len(cachedInfo.Fields))
	copy(fields, cachedInfo.Fields)
	sInfo := &structInfo{Fields: fields}

	// Parse the file using the parser package
	tableData, err := parser.Parse(input, p.fileType)
	if err != nil {
		return nil, nil, nil, wrapParseError(err)
	}

	headers := tableData.Headers
	records := tableData.Records

	// Build header name to column index map (first occurrence wins for
	// duplicates).
	//
	// The key is folded, and so is the lookup below, because a header and a
	// field name are the same column when they differ only in case: SQLite
	// compares the identifiers filesql creates from these headers that way, and
	// the loader refuses two headers that differ only in case as duplicates for
	// the same reason. Comparing as written meant a struct field "Name", whose
	// column name is derived as "name", matched no column at all in a file whose
	// header says "Name" -- which is what a spreadsheet writes.
	//
	// It is the loader's fold rather than strings.ToLower, and for the same
	// reason: SQLite's stops at ASCII, so a file whose headers are "ä" and "Ä"
	// is two columns there. Folding the whole of Unicode made them one key here,
	// and a field naming the second one read and wrote the first.
	headerToColIdx := make(map[string]int, len(headers))
	for i, h := range headers {
		key := reader.ASCIIFold(h)
		if _, exists := headerToColIdx[key]; !exists {
			headerToColIdx[key] = i
		}
	}

	// Resolve column indices for each field based on column name. A field that
	// matches nothing is the caller's mistake rather than a row with a missing
	// value, and saying so here is the only place it can be told apart: once the
	// rows start, an unmatched field is indistinguishable from an empty cell.
	//
	// Every unmatched field is collected before failing, since a struct written
	// against the wrong shape usually has more than one, and one error listing
	// them beats making the caller fix them one run at a time. A struct covering
	// a subset of the columns is not a mistake and is not caught: extra columns
	// are ordinary, extra fields are not.
	var unmatched []string
	for i := range sInfo.Fields {
		fi := &sInfo.Fields[i]
		colIdx, ok := headerToColIdx[reader.ASCIIFold(fi.ColumnName)]
		if !ok {
			// A field carrying prep:"default=..." is meant to work without a
			// column: the default is where its value comes from. Only a field
			// with no other way to get one is a mistake.
			if !fi.Preprocessors.hasDefault() {
				unmatched = append(unmatched, fmt.Sprintf("%s (column %q)", fi.Name, fi.ColumnName))
			}
			continue
		}
		fi.ColumnIndex = colIdx
	}
	if len(unmatched) > 0 {
		return nil, nil, nil, fmt.Errorf("%w: %s; the input has %v",
			ErrUnknownColumn, strings.Join(unmatched, ", "), headers)
	}

	// Process records: apply preprocessing and validation
	// Pre-allocate errors slice with estimated capacity (assume ~10% error rate)
	estimatedErrors := max(len(records)/10, 16)
	result := &ProcessResult{
		Columns:        headers,
		OriginalFormat: p.fileType,
		OutputFormat:   p.outputFormat(),
		Errors:         make([]error, 0, estimatedErrors),
	}
	structSliceValue := reflect.ValueOf(structSlicePointer).Elem()

	// Always reset slice length so that reusing the same slice does not
	// carry over stale elements from a previous Process call.
	structSliceValue.SetLen(0)

	// Pre-allocate the struct slice to avoid repeated growth
	if structSliceValue.Cap() < len(records) {
		newSlice := reflect.MakeSlice(structSliceValue.Type(), 0, len(records))
		structSliceValue.Set(newSlice)
	}

	headerLen := len(headers)
	isJSONFormat := p.fileType == parser.JSON || p.fileType == parser.JSONL

	// jsonDataColumn is the column name the parser package uses for JSON/JSONL data.
	// Each JSON element is stored as a raw JSON string in this single column.
	const jsonDataColumn = "data"

	// When validRowsOnly is enabled, collect only valid records for output
	if p.validRowsOnly {
		result.validRecords = make([][]string, 0, len(records))
	}

	// One buffer holds the target values of whichever cross-field validator is
	// running, so a file with cross-field tags does not allocate a slice per
	// validator per row.
	targetScratch := make([]string, 0, maxCrossFieldTargets(sInfo))

	// The values each unique column has already seen, allocated per run. It
	// cannot live anywhere longer-lived: sInfo comes from a process-wide cache
	// keyed by (type, strict), so a seen set parked there would carry values
	// from one file into the next and race between concurrent processors
	// sharing one struct type.
	uniqueSeen := newUniqueColumns(sInfo)

	// Process records in-place to avoid unnecessary allocations
	for rowIdx := range records {
		record := records[rowIdx]
		rowNum := rowIdx + 1 // 1-based row number (excluding header)
		result.RowCount++

		// Pad short rows with empty strings only if needed
		if len(record) < headerLen {
			padded := make([]string, headerLen)
			copy(padded, record)
			records[rowIdx] = padded
			record = padded
		}

		structValue := reflect.New(structType).Elem()

		// First pass: preprocessing and single-field validation.
		// processRow returns fieldValues mapping each field name to its
		// preprocessed string value (used for cross-field validation).
		fieldValues := make(map[string]string, len(sInfo.Fields))
		rowHasError, err := p.processRow(record, rowNum, sInfo, structValue, result, isJSONFormat, jsonDataColumn, fieldValues, uniqueSeen)
		if err != nil {
			return nil, nil, nil, err
		}

		// Second pass: cross-field validation using processed field values
		if p.applyCrossFieldValidation(rowNum, sInfo, fieldValues, result, targetScratch) {
			rowHasError = true
		}

		if !rowHasError {
			result.ValidRowCount++
			if p.validRowsOnly {
				result.validRecords = append(result.validRecords, record)
			}
			structSliceValue.Set(reflect.Append(structSliceValue, structValue))
		} else if !p.validRowsOnly {
			structSliceValue.Set(reflect.Append(structSliceValue, structValue))
		}
	}

	return headers, records, result, nil
}

// processRow applies preprocessing and single-field validation to one row.
// It populates fieldValues with each field's preprocessed string value so
// that cross-field validators always see the correct value regardless of
// the field's Go type.
// It returns true if the row has any errors, and a non-nil error for fatal
// conditions (e.g., JSON corruption after preprocessing).
func (p *Processor) processRow(
	record []string,
	rowNum int,
	structInfo *structInfo,
	structValue reflect.Value,
	result *ProcessResult,
	isJSONFormat bool,
	jsonDataColumn string,
	fieldValues map[string]string,
	uniqueSeen []map[string]int,
) (bool, error) {
	rowHasError := false

	for fieldIdx := range structInfo.Fields {
		fieldInfo := &structInfo.Fields[fieldIdx]
		colIdx := fieldInfo.ColumnIndex

		// Get value: empty string if column not found or out of range
		value := ""
		if colIdx >= 0 && colIdx < len(record) {
			value = record[colIdx]
		}

		colName := fieldInfo.ColumnName

		// Apply preprocessing and update record in-place
		processedValue := fieldInfo.Preprocessors.Process(value)
		if colIdx >= 0 && colIdx < len(record) {
			record[colIdx] = processedValue
		}

		// Store the preprocessed string value for cross-field validation.
		// This avoids using reflect.Value.String() which returns diagnostic
		// strings (e.g. "<int Value>") for non-string types.
		fieldValues[fieldInfo.Name] = processedValue

		// For JSON/JSONL formats, verify the "data" column integrity after preprocessing.
		// Only the "data" column contains JSON values; other struct fields may map to
		// non-existent columns and receive default/preprocessed non-JSON values, so
		// checking all fields would cause false positives.
		if isJSONFormat && colName == jsonDataColumn {
			if processedValue != "" && !json.Valid([]byte(processedValue)) {
				// Prep tags (e.g. truncate, replace) destroyed the JSON structure.
				// This is a hard error: invalid JSON lines in JSONL output cause
				// downstream parsers to fail entirely.
				return false, fmt.Errorf("row %d, column %q: %w: %s",
					rowNum, colName, ErrInvalidJSONAfterPrep, truncateForError(processedValue, 100))
			} else if value != "" && processedValue == "" {
				// Preprocessing emptied the JSON data (e.g. nullify).
				// The row will be skipped in JSONL output, so record a PrepError
				// to keep ValidRowCount consistent with actual output line count.
				result.Errors = append(result.Errors, newPrepError(
					rowNum, colName, fieldInfo.Name, "empty_json_data",
					"JSON data is empty after preprocessing (original: "+truncateForError(value, 100)+")",
				))
				rowHasError = true
			}
		}

		// Apply validation
		if tag, msg := fieldInfo.Validators.Validate(processedValue); msg != "" {
			result.Errors = append(result.Errors, newValidationError(
				rowNum, colName, fieldInfo.Name, processedValue, tag, msg,
			))
			rowHasError = true
		}

		// A unique column refuses a value an earlier row already carried. An
		// empty cell is a missing value rather than a value, so two of them are
		// two absences and neither is a duplicate.
		if seen := uniqueColumn(uniqueSeen, fieldIdx); seen != nil && processedValue != "" {
			if firstRow, duplicate := seen[processedValue]; duplicate {
				result.Errors = append(result.Errors, newValidationError(
					rowNum, colName, fieldInfo.Name, processedValue, uniqueTagValue,
					fmt.Sprintf("value %q already appeared in row %d", processedValue, firstRow),
				))
				rowHasError = true
			} else {
				seen[processedValue] = rowNum
			}
		}

		// Set struct field value (use field index, not column index)
		if err := setFieldValue(structValue.Field(fieldInfo.Index), processedValue); err != nil {
			result.Errors = append(result.Errors, newPrepError(
				rowNum, colName, fieldInfo.Name, "type_conversion",
				fmt.Sprintf("failed to convert value %q: %v", processedValue, err),
			))
			rowHasError = true
		}
	}

	return rowHasError, nil
}

// newUniqueColumns allocates one seen set per column tagged unique, indexed the
// way structInfo.Fields is, with nil for every other column. It is called once
// per Process call, which is what keeps one run's values out of the next.
//
// The map needs no lock because a run walks its rows in order on one goroutine.
// If that ever changes, this map is the first thing it breaks.
func newUniqueColumns(structInfo *structInfo) []map[string]int {
	var columns []map[string]int
	for i := range structInfo.Fields {
		if !structInfo.Fields[i].Unique {
			continue
		}
		if columns == nil {
			columns = make([]map[string]int, len(structInfo.Fields))
		}
		columns[i] = make(map[string]int)
	}
	// A struct with no unique column gets no slice, so it pays nothing for the
	// tag it does not use.
	return columns
}

// uniqueColumn returns the seen set of one column, or nil when the struct has
// no unique column at all and newUniqueColumns therefore built no slice.
func uniqueColumn(columns []map[string]int, fieldIdx int) map[string]int {
	if columns == nil {
		return nil
	}
	return columns[fieldIdx]
}

// applyCrossFieldValidation runs cross-field validators for one row.
// fieldValues maps struct field names to their preprocessed values, so
// cross-field validators always see the final values regardless of whether
// the column existed in the original input.
// It returns true if any cross-field validation error was found.
func (p *Processor) applyCrossFieldValidation(
	rowNum int,
	structInfo *structInfo,
	fieldValues map[string]string,
	result *ProcessResult,
	targetScratch []string,
) bool {
	hasError := false

	for _, fieldInfo := range structInfo.Fields {
		if len(fieldInfo.CrossFieldValidators) == 0 {
			continue
		}

		srcValue := fieldValues[fieldInfo.Name]
		colName := fieldInfo.ColumnName

		for _, crossValidator := range fieldInfo.CrossFieldValidators {
			targetValues, missing := resolveTargets(crossValidator, fieldValues, targetScratch)
			if missing != "" {
				result.Errors = append(result.Errors, newValidationError(
					rowNum, colName, fieldInfo.Name, srcValue,
					crossValidator.Name(),
					fmt.Sprintf("target field %s not found", missing),
				))
				hasError = true
				continue
			}

			if skipCrossField(crossValidator, srcValue, targetValues) {
				continue
			}

			if msg := crossValidator.Validate(srcValue, targetValues); msg != "" {
				result.Errors = append(result.Errors, newValidationError(
					rowNum, colName, fieldInfo.Name, srcValue,
					crossValidator.Name(), msg,
				))
				hasError = true
			}
		}
	}

	return hasError
}

// resolveTargets reads the row's value for each field the validator names. It
// returns the name of the first field the struct does not have, so a misspelled
// field name is reported rather than compared against nothing.
func resolveTargets(cv crossFieldValidator, fieldValues map[string]string, buf []string) ([]string, string) {
	values := buf[:0]
	for _, name := range cv.TargetFields() {
		value, ok := fieldValues[name]
		if !ok {
			return nil, name
		}
		values = append(values, value)
	}
	return values, ""
}

// maxCrossFieldTargets returns the longest list of fields any cross-field
// validator in the struct names, which is how large one row's buffer has to be.
func maxCrossFieldTargets(structInfo *structInfo) int {
	longest := 0
	for _, fieldInfo := range structInfo.Fields {
		for _, cv := range fieldInfo.CrossFieldValidators {
			if n := len(cv.TargetFields()); n > longest {
				longest = n
			}
		}
	}
	return longest
}

// skipCrossField reports whether a comparison has nothing to say about this
// row. An empty cell passes every validator but required, the rule the
// single-field validators already follow, so a comparison is skipped as soon as
// either side is missing; comparing a value against a value that is not there
// has no answer. The tags that decide whether an empty cell is allowed are the
// exception, since an empty cell is the only row they have anything to say
// about.
func skipCrossField(cv crossFieldValidator, srcValue string, targetValues []string) bool {
	if cv.decidesPresence() {
		return false
	}
	if srcValue == "" {
		return true
	}
	return slices.Contains(targetValues, "")
}

// buildOutput generates the output io.Reader from the given records.
func (p *Processor) buildOutput(headers []string, outputRecords [][]string) (io.Reader, error) {
	// Pre-allocate buffer capacity based on estimated output size to reduce allocations
	var outputBuf bytes.Buffer
	estimatedSize := p.estimateOutputSize(headers, outputRecords)
	outputBuf.Grow(estimatedSize)
	if err := p.writeOutput(&outputBuf, headers, outputRecords); err != nil {
		return nil, fmt.Errorf("failed to write output: %w", err)
	}

	if err := p.refuseEmptyOutput(outputBuf.Len() == 0); err != nil {
		return nil, err
	}

	return newStream(outputBuf.Bytes()), nil
}

// refuseEmptyOutput turns an output with no bytes into an error, for the
// formats where that leaves nothing to read. JSONL and LTSV write no header
// line, so every row is written entirely within itself and a file with no rows
// is an empty stream: whatever reads it next has no columns to make a table
// from. CSV and TSV write a header, so the same drop leaves a stream that still
// names the columns, and that is a table with no rows rather than nothing.
func (p *Processor) refuseEmptyOutput(empty bool) error {
	format := p.outputFormat()
	if !empty || (format != parser.JSONL && format != parser.LTSV) {
		return nil
	}
	return fmt.Errorf("%s %w", format, ErrEmptyOutput)
}

// outputFormat returns the actual output format for the stream.
// CSV, TSV, and LTSV preserve their format.
// JSON and JSONL are output as JSONL (one JSON value per line).
// XLSX and Parquet are converted to CSV.
func (p *Processor) outputFormat() parser.FileType {
	switch p.fileType {
	case parser.CSV, parser.TSV, parser.LTSV:
		return p.fileType
	case parser.JSON, parser.JSONL:
		return parser.JSONL
	default:
		// XLSX, Parquet output as CSV
		return parser.CSV
	}
}

// estimateOutputSize estimates the output buffer size based on headers and records.
// This helps reduce buffer reallocations during output generation.
func (p *Processor) estimateOutputSize(headers []string, records [][]string) int {
	// Estimate average field length (including delimiter and quotes)
	const avgFieldLen = 20
	const lineOverhead = 2 // newline characters

	headerSize := len(headers) * avgFieldLen
	recordSize := len(records) * (len(headers)*avgFieldLen + lineOverhead)

	return headerSize + recordSize
}

// writeOutput writes the processed data back in the original format.
//
// Output format by input type:
//   - CSV → CSV (comma-delimited)
//   - TSV → TSV (tab-delimited)
//   - LTSV → LTSV (label:value pairs, tab-separated)
//   - JSON → JSONL (one JSON value per line)
//   - JSONL → JSONL (one JSON value per line)
//   - XLSX → CSV (tabular data as comma-delimited)
//   - Parquet → CSV (tabular data as comma-delimited)
func (p *Processor) writeOutput(w io.Writer, headers []string, records [][]string) error {
	format, ok := outputFormats[p.fileType]
	if !ok {
		// CSV, XLSX and Parquet all come out as CSV: the two that are not text
		// have no text form of their own, and a tabular one is what they hold.
		format = writer.FormatCSV
	}

	out := writer.New(w, format, writer.Options{})
	if err := out.Header(headers); err != nil {
		return err
	}
	for _, record := range records {
		if err := out.Record(record); err != nil {
			return err
		}
	}
	return out.Flush()
}

// outputFormats names the format each input type is written back as. A type
// that is not here is written as CSV.
//
//nolint:gochecknoglobals // constant-like lookup table
var outputFormats = map[parser.FileType]writer.Format{
	parser.TSV:   writer.FormatTSV,
	parser.LTSV:  writer.FormatLTSV,
	parser.JSON:  writer.FormatJSONL,
	parser.JSONL: writer.FormatJSONL,
}

// truncateForError truncates a string for inclusion in error messages.
// It truncates on rune boundaries to avoid splitting multi-byte characters.
func truncateForError(s string, maxLen int) string {
	runes := []rune(s)
	if len(runes) <= maxLen {
		return s
	}
	return string(runes[:maxLen]) + "..."
}

// setFieldValue sets a struct field value from a string
func setFieldValue(field reflect.Value, value string) error {
	if !field.CanSet() {
		return nil
	}

	switch field.Kind() {
	case reflect.String:
		field.SetString(value)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if value == "" {
			field.SetInt(0)
			return nil
		}
		intVal, err := strconv.ParseInt(value, 10, field.Type().Bits())
		if err != nil {
			return err
		}
		field.SetInt(intVal)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if value == "" {
			field.SetUint(0)
			return nil
		}
		uintVal, err := strconv.ParseUint(value, 10, field.Type().Bits())
		if err != nil {
			return err
		}
		field.SetUint(uintVal)
	case reflect.Float32, reflect.Float64:
		if value == "" {
			field.SetFloat(0)
			return nil
		}
		floatVal, err := strconv.ParseFloat(value, field.Type().Bits())
		if err != nil {
			return err
		}
		field.SetFloat(floatVal)
	case reflect.Bool:
		if value == "" {
			field.SetBool(false)
			return nil
		}
		boolVal, err := strconv.ParseBool(value)
		if err != nil {
			return err
		}
		field.SetBool(boolVal)
	default:
		return fmt.Errorf("unsupported field type: %s", field.Kind())
	}
	return nil
}
