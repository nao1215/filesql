package filesql

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	pqfile "github.com/apache/arrow/go/v18/parquet/file"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	"github.com/nao1215/filesql/parser"
	"github.com/xuri/excelize/v2"
)

// handleCloseError is a helper function to handle close errors consistently
func handleCloseError(closeFunc func() error) func() {
	return func() {
		if closeErr := closeFunc(); closeErr != nil {
			// In the future, this could be enhanced with proper logging
			_ = closeErr
		}
	}
}

// newStreamingParser creates a new streaming parser. The malformed-row policy
// defaults to MalformedRowStop (the zero value); callers that need another
// policy set the field after construction.
func newStreamingParser(fileType FileType, compression CompressionType, tableName string, chunkSize int) *streamingParser {
	return &streamingParser{
		fileType:    fileType,
		compression: compression,
		tableName:   tableName,
		chunkSize:   newChunkSize(chunkSize),
		memoryPool:  newMemoryPool(1024 * 1024), // 1MB default max buffer size
		memoryLimit: newMemoryLimit(512),        // 512MB default memory limit
	}
}

// parseFromReader parses data from io.Reader and returns a table using streaming approach
func (p *streamingParser) parseFromReader(reader io.Reader) (*table, error) {
	var decompressedReader io.Reader
	var closeFunc func() error
	var err error

	// Handle compression
	decompressedReader, closeFunc, err = p.createDecompressedReader(reader)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to create decompressed reader: %w", ErrCompression, err)
	}
	if closeFunc != nil {
		defer handleCloseError(closeFunc)
	}

	// Parse based on base file type
	baseType := p.fileType
	if isTextBaseType(baseType) {
		decompressedReader = decodeTextReader(decompressedReader)
	}
	switch baseType {
	case FileTypeCSV:
		return p.parseCSVStream(decompressedReader)
	case FileTypeTSV:
		return p.parseTSVStream(decompressedReader)
	case FileTypeLTSV:
		return p.parseLTSVStream(decompressedReader)
	case FileTypeParquet:
		return p.parseParquetStream(decompressedReader)
	case FileTypeXLSX:
		return p.parseXLSXStream(decompressedReader)
	case FileTypeJSON:
		return p.parseJSONStream(decompressedReader)
	case FileTypeJSONL:
		return p.parseJSONLStream(decompressedReader)
	default:
		return nil, ErrUnsupportedFormat
	}
}

// createDecompressedReader wraps reader with the codec the source was declared
// to use. The per-format switch this replaced was a second implementation of
// CompressionHandler.CreateReader, reached through the fused FileType.
//
// CompressionNone returns the reader unchanged with a no-op close function,
// following CreateReader's convention that the close function is never nil.
func (p *streamingParser) createDecompressedReader(reader io.Reader) (io.Reader, func() error, error) {
	return NewCompressionHandler(p.compression).CreateReader(reader)
}

// parseDelimitedStream parses CSV or TSV data from reader using streaming approach
func (p *streamingParser) parseDelimitedStream(reader io.Reader, delimiter rune, fileTypeName string) (*table, error) {
	csvReader := csv.NewReader(parser.NormalizeLineEndings(reader))
	csvReader.Comma = delimiter
	// Accept a variable field count so a ragged row is handled by the configured
	// malformed-row policy instead of aborting the whole read.
	csvReader.FieldsPerRecord = -1
	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: failed to read %s: %w", ErrParsing, fileTypeName, err)
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("%w: empty %s data", ErrEmptyData, fileTypeName)
	}

	header := newHeader(records[0])
	// Check for duplicate column names
	if err := validateColumnNames(records[0]); err != nil {
		return nil, err
	}

	tablerecords := make([]record, 0, len(records)-1)
	for i := 1; i < len(records); i++ {
		p.totalRows++
		record, skip, err := reconcileFieldCount(records[i], len(header), i, p.malformedRowPolicy)
		if err != nil {
			return nil, err
		}
		if skip {
			p.skippedRows++
			continue
		}
		tablerecords = append(tablerecords, newRecord(record))
	}

	return newTable(p.tableName, header, tablerecords), nil
}

// parseCSVStream parses CSV data from reader using streaming approach
func (p *streamingParser) parseCSVStream(reader io.Reader) (*table, error) {
	return p.parseDelimitedStream(reader, csvDelimiter, "CSV")
}

// parseTSVStream parses TSV data from reader using streaming approach
func (p *streamingParser) parseTSVStream(reader io.Reader) (*table, error) {
	return p.parseDelimitedStream(reader, tsvDelimiter, "TSV")
}

// parseLTSVStream parses LTSV data from reader using streaming approach
// labelOrder collects LTSV labels, keeping each one once and in the order it was
// first seen. LTSV has no header line, so the columns can only be the labels the
// records carry; the set has to be built while reading, and the order has to be
// remembered rather than recovered from the set afterwards.
type labelOrder struct {
	seen  map[string]bool
	order []string
}

func newLabelOrder() *labelOrder {
	return &labelOrder{seen: make(map[string]bool)}
}

// add records a label, ignoring one already seen.
func (l *labelOrder) add(name string) {
	if l.seen[name] {
		return
	}
	l.seen[name] = true
	l.order = append(l.order, name)
}

// names returns the labels in the order they were first seen.
func (l *labelOrder) names() []string {
	return l.order
}

// len returns how many distinct labels were seen.
func (l *labelOrder) len() int {
	return len(l.order)
}

func (p *streamingParser) parseLTSVStream(reader io.Reader) (*table, error) {
	content, err := io.ReadAll(parser.NormalizeLineEndings(reader))
	if err != nil {
		return nil, fmt.Errorf("%w: failed to read LTSV: %w", ErrParsing, err)
	}

	lines := strings.Split(string(content), "\n")
	if len(lines) == 0 {
		return nil, fmt.Errorf("%w: empty LTSV data", ErrEmptyData)
	}

	// The columns are the labels in the order they first appear. Reading them out
	// of a map instead drew a fresh order on every load, because Go randomizes map
	// iteration: the same file answered SELECT * as "id,name" one run and
	// "name,id" the next, and its dump was unstable with it.
	labels := newLabelOrder()
	var records []map[string]string

	for _, line := range lines {
		// Only the line terminator is removed. TrimSpace took the trailing spaces
		// of the last field with it, so a value ending in a space lost it.
		line = strings.TrimRight(line, "\r")
		if line == "" {
			continue
		}

		recordMap := make(map[string]string)
		for pair := range strings.SplitSeq(line, "\t") {
			kv := strings.SplitN(pair, ":", 2)
			if len(kv) == 2 {
				key := strings.TrimSpace(kv[0])
				// The value is the bytes up to the next tab or newline. Trimming it lost
				// whitespace the writer had written and CSV would have kept, so the same
				// data read from two formats disagreed. The label is trimmed because a
				// space around one is malformed either way.
				value := kv[1]
				// A label repeated within the same record cannot be two distinct
				// columns; keeping the last value would silently drop the earlier
				// one, so reject it. Ref nao1215/sqly#467.
				if _, dup := recordMap[key]; dup {
					return nil, fmt.Errorf("%w: duplicate column name %q in LTSV record", ErrParsing, key)
				}
				recordMap[key] = value
				labels.add(key)
			}
		}
		if len(recordMap) > 0 {
			records = append(records, recordMap)
		}
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("%w: no valid LTSV records found", ErrEmptyData)
	}

	header := header(labels.names())

	tablerecords := make([]record, 0, len(records))
	for _, recordMap := range records {
		var row record
		for _, key := range header {
			if val, exists := recordMap[key]; exists {
				row = append(row, val)
			} else {
				row = append(row, "")
			}
		}
		tablerecords = append(tablerecords, row)
	}

	return newTable(p.tableName, header, tablerecords), nil
}

// ProcessInChunks processes data from io.Reader in chunks and calls processor for each chunk
// This provides true streaming with memory-efficient chunk-based processing
func (p *streamingParser) ProcessInChunks(reader io.Reader, processor chunkProcessor) error {
	var decompressedReader io.Reader
	var closeFunc func() error
	var err error

	// Handle compression
	decompressedReader, closeFunc, err = p.createDecompressedReader(reader)
	if err != nil {
		return fmt.Errorf("%w: failed to create decompressed reader: %w", ErrCompression, err)
	}
	if closeFunc != nil {
		defer handleCloseError(closeFunc)
	}

	// Parse based on base file type
	baseType := p.fileType
	if isTextBaseType(baseType) {
		decompressedReader = decodeTextReader(decompressedReader)
	}
	switch baseType {
	case FileTypeCSV:
		return p.processCSVInChunks(decompressedReader, processor)
	case FileTypeTSV:
		return p.processTSVInChunks(decompressedReader, processor)
	case FileTypeLTSV:
		return p.processLTSVInChunks(decompressedReader, processor)
	case FileTypeParquet:
		return p.processParquetInChunks(decompressedReader, processor)
	case FileTypeXLSX:
		return p.processXLSXInChunks(decompressedReader, processor)
	case FileTypeJSON:
		return p.processJSONInChunks(decompressedReader, processor)
	case FileTypeJSONL:
		return p.processJSONLInChunks(decompressedReader, processor)
	default:
		return fmt.Errorf("%w: unsupported file type for chunked processing", ErrUnsupportedFormat)
	}
}

// processDelimitedInChunks processes CSV or TSV data in chunks based on delimiter
func (p *streamingParser) processDelimitedInChunks(reader io.Reader, processor chunkProcessor, delimiter rune, fileTypeName string) error {
	csvReader := csv.NewReader(parser.NormalizeLineEndings(reader))
	if delimiter != csvDelimiter {
		csvReader.Comma = delimiter
	}
	// Accept a variable field count from the reader so a ragged row is handled by
	// the configured malformed-row policy instead of aborting the whole read.
	csvReader.FieldsPerRecord = -1

	// Read header first
	headerrecord, err := csvReader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("%w: empty %s data", ErrEmptyData, fileTypeName)
		}
		return fmt.Errorf("%w: failed to read %s header: %w", ErrParsing, fileTypeName, err)
	}

	// Validate header for duplicates
	if err := validateColumnNames(headerrecord); err != nil {
		return err
	}

	header := newHeader(headerrecord)
	var columnInfo columnInfoList
	var columnValues [][]string

	// Read records in chunks
	var chunkrecords []record
	chunkSize := p.chunkSize.Int()
	if chunkSize <= 0 {
		chunkSize = DefaultRowsPerChunk
	}

	rowNum := 0
	for {
		record, err := csvReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("%w: failed to read %s record: %w", ErrParsing, fileTypeName, err)
		}
		rowNum++

		p.totalRows++
		record, skip, err := reconcileFieldCount(record, len(header), rowNum, p.malformedRowPolicy)
		if err != nil {
			return err
		}
		if skip {
			p.skippedRows++
			continue
		}

		chunkrecords = append(chunkrecords, newRecord(record))

		// Collect values for type inference (only on first chunk)
		if len(columnInfo) == 0 {
			if len(columnValues) == 0 {
				columnValues = make([][]string, len(header))
			}
			for i, val := range record {
				if i < len(columnValues) {
					columnValues[i] = append(columnValues[i], val)
				}
			}
		}

		// Process chunk when it reaches the target size
		if len(chunkrecords) >= chunkSize {
			// Infer column types on first chunk. A later chunk does not
			// re-infer, but it can still hold a value a numeric column would
			// damage, so it is asked. See promoteForRecords.
			if len(columnInfo) != 0 {
				columnInfo.promoteForRecords(chunkrecords)
			} else {
				columnInfo = newColumnInfoListFromValues(header, columnValues)
			}

			chunk := &tableChunk{
				tableName:  p.tableName,
				headers:    header,
				records:    chunkrecords,
				columnInfo: columnInfo,
			}

			if err := processor(chunk); err != nil {
				return fmt.Errorf("chunk processor error: %w", err)
			}

			// Reset for next chunk
			chunkrecords = nil
			columnValues = nil // Don't collect values after first chunk
		}
	}

	// Process remaining records
	if len(chunkrecords) > 0 {
		// Infer column types if we haven't yet (small dataset)
		if len(columnInfo) != 0 {
			columnInfo.promoteForRecords(chunkrecords)
		} else {
			columnInfo = newColumnInfoListFromValues(header, columnValues)
		}

		chunk := &tableChunk{
			tableName:  p.tableName,
			headers:    header,
			records:    chunkrecords,
			columnInfo: columnInfo,
		}

		if err := processor(chunk); err != nil {
			return fmt.Errorf("chunk processor error: %w", err)
		}
	} else if len(columnInfo) == 0 {
		// Handle header-only files: create empty chunk with header information
		// This ensures table is created with correct column names even when no data records exist
		columnInfo = newColumnInfoListFromValues(header, columnValues)

		chunk := &tableChunk{
			tableName:  p.tableName,
			headers:    header,
			records:    nil, // Empty records for header-only file
			columnInfo: columnInfo,
		}

		if err := processor(chunk); err != nil {
			return fmt.Errorf("chunk processor error: %w", err)
		}
	}

	return nil
}

// processCSVInChunks processes CSV data in chunks
func (p *streamingParser) processCSVInChunks(reader io.Reader, processor chunkProcessor) error {
	return p.processDelimitedInChunks(reader, processor, csvDelimiter, "CSV")
}

// processTSVInChunks processes TSV data in chunks
func (p *streamingParser) processTSVInChunks(reader io.Reader, processor chunkProcessor) error {
	return p.processDelimitedInChunks(reader, processor, tsvDelimiter, "TSV")
}

// processLTSVInChunks processes LTSV data in chunks
func (p *streamingParser) processLTSVInChunks(reader io.Reader, processor chunkProcessor) error {
	// For LTSV, we need to read line by line
	content, err := io.ReadAll(parser.NormalizeLineEndings(reader))
	if err != nil {
		return fmt.Errorf("%w: failed to read LTSV: %w", ErrParsing, err)
	}

	lines := strings.Split(string(content), "\n")
	if len(lines) == 0 {
		return fmt.Errorf("%w: empty LTSV data", ErrEmptyData)
	}

	// First pass: collect the labels in the order they first appear. See
	// parseLTSVStream for why the order cannot come out of a map.
	labels := newLabelOrder()
	for _, line := range lines {
		// Only the line terminator is removed. TrimSpace took the trailing spaces
		// of the last field with it, so a value ending in a space lost it.
		line = strings.TrimRight(line, "\r")
		if line == "" {
			continue
		}

		for pair := range strings.SplitSeq(line, "\t") {
			kv := strings.SplitN(pair, ":", 2)
			if len(kv) == 2 {
				labels.add(strings.TrimSpace(kv[0]))
			}
		}
	}

	if labels.len() == 0 {
		return fmt.Errorf("%w: no valid LTSV keys found", ErrEmptyData)
	}

	header := header(labels.names())

	// Second pass: process records in chunks
	chunkrecords := make([]record, 0) // Pre-allocate slice
	var columnValues [][]string
	var columnInfo columnInfoList

	chunkSize := p.chunkSize.Int()
	if chunkSize <= 0 {
		chunkSize = DefaultRowsPerChunk
	}

	for _, line := range lines {
		// Only the line terminator is removed. TrimSpace took the trailing spaces
		// of the last field with it, so a value ending in a space lost it.
		line = strings.TrimRight(line, "\r")
		if line == "" {
			continue
		}

		recordMap := make(map[string]string)
		for pair := range strings.SplitSeq(line, "\t") {
			kv := strings.SplitN(pair, ":", 2)
			if len(kv) == 2 {
				key := strings.TrimSpace(kv[0])
				// The value is the bytes up to the next tab or newline. Trimming it lost
				// whitespace the writer had written and CSV would have kept, so the same
				// data read from two formats disagreed. The label is trimmed because a
				// space around one is malformed either way.
				value := kv[1]
				// A label repeated within the same record cannot be two distinct
				// columns; keeping the last value would silently drop the earlier
				// one, so reject it. Ref nao1215/sqly#467.
				if _, dup := recordMap[key]; dup {
					return fmt.Errorf("%w: duplicate column name %q in LTSV record", ErrParsing, key)
				}
				recordMap[key] = value
			}
		}

		if len(recordMap) == 0 {
			continue
		}

		var row record
		for _, key := range header {
			if val, exists := recordMap[key]; exists {
				row = append(row, val)
			} else {
				row = append(row, "")
			}
		}
		chunkrecords = append(chunkrecords, row)

		// Collect values for type inference (only on first chunk)
		if len(columnInfo) == 0 {
			if len(columnValues) == 0 {
				columnValues = make([][]string, len(header))
			}
			for i, val := range row {
				if i < len(columnValues) {
					columnValues[i] = append(columnValues[i], val)
				}
			}
		}

		// Process chunk when it reaches the target size
		if len(chunkrecords) >= chunkSize {
			// Infer column types on first chunk. A later chunk does not
			// re-infer, but it can still hold a value a numeric column would
			// damage, so it is asked. See promoteForRecords.
			if len(columnInfo) != 0 {
				columnInfo.promoteForRecords(chunkrecords)
			} else {
				columnInfo = newColumnInfoListFromValues(header, columnValues)
			}

			chunk := &tableChunk{
				tableName:  p.tableName,
				headers:    header,
				records:    chunkrecords,
				columnInfo: columnInfo,
			}

			if err := processor(chunk); err != nil {
				return fmt.Errorf("chunk processor error: %w", err)
			}

			// Reset for next chunk
			chunkrecords = nil
			columnValues = nil
		}
	}

	// Process remaining records
	if len(chunkrecords) > 0 {
		// Infer column types if we haven't yet
		if len(columnInfo) != 0 {
			columnInfo.promoteForRecords(chunkrecords)
		} else {
			columnInfo = newColumnInfoListFromValues(header, columnValues)
		}

		chunk := &tableChunk{
			tableName:  p.tableName,
			headers:    header,
			records:    chunkrecords,
			columnInfo: columnInfo,
		}

		if err := processor(chunk); err != nil {
			return fmt.Errorf("chunk processor error: %w", err)
		}
	}

	return nil
}

// parseParquetStream parses Parquet data from reader using streaming approach
func (p *streamingParser) parseParquetStream(reader io.Reader) (*table, error) {
	// Read all data into memory (Parquet requires random access)
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read parquet data: %w", err)
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("%w: empty parquet file", ErrEmptyData)
	}

	// Create a bytes reader for the parquet data
	bytesReader := &bytesReaderAt{data: data}

	// Create parquet file reader from bytes
	pqReader, err := pqfile.NewParquetReader(bytesReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader from bytes: %w", err)
	}
	defer pqReader.Close()

	// Create arrow file reader
	arrowReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create arrow reader: %w", err)
	}

	// Read all record batches using the table reader approach
	ctx := context.Background()
	table, err := arrowReader.ReadTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read table: %w", err)
	}
	defer table.Release()

	if table.NumRows() == 0 {
		return nil, fmt.Errorf("%w: no records found in parquet stream", ErrEmptyData)
	}

	// Initialize header from table schema
	schema := table.Schema()
	headerSlice := make(header, schema.NumFields())
	for i, field := range schema.Fields() {
		headerSlice[i] = field.Name
	}

	// Read data by converting table to record batches
	tableReader := array.NewTableReader(table, 0)
	defer tableReader.Release()

	var allRecords []record
	for tableReader.Next() {
		batch := tableReader.Record()

		// Convert each row in the batch
		numRows := batch.NumRows()
		for i := range numRows {
			row := make(record, batch.NumCols())
			for j, col := range batch.Columns() {
				value := extractValueFromArrowArray(col, i)
				row[j] = value
			}
			allRecords = append(allRecords, row)
		}
	}

	if err := tableReader.Err(); err != nil {
		return nil, fmt.Errorf("error reading table records: %w", err)
	}

	t := newTable(p.tableName, headerSlice, allRecords)
	// The schema outranks what the values look like: a DOUBLE column holding
	// whole numbers is still REAL, and a STRING column of digits is still TEXT.
	t.columnInfo = arrowColumnInfoList(schema)
	return t, nil
}

// arrowColumnInfoList maps a Parquet file's Arrow schema onto SQLite column
// types. Parquet states the type of every column, so an import has nothing to
// guess: reading the schema is both cheaper and more faithful than inferring
// from the rendered values, which cannot tell a DOUBLE that happens to hold
// whole numbers from an INT64, nor a STRING of digits from either.
//
// The type declared here has to agree with what extractValueFromArrowArray
// renders, because SQLite applies the column's affinity to that string. A
// mismatch is worse than TEXT: it would store a value the column claims not to
// hold.
func arrowColumnInfoList(schema *arrow.Schema) columnInfoList {
	fields := schema.Fields()
	columns := make(columnInfoList, len(fields))
	for i, field := range fields {
		columns[i] = columnInfo{Name: field.Name, Type: arrowColumnType(field.Type)}
	}
	return columns
}

// arrowColumnType is the SQLite type for one Arrow type. Anything not named
// here stays TEXT, which is the safe answer: an unrecognized type is rendered by
// extractValueFromArrowArray's default branch, and its shape is not known.
func arrowColumnType(dt arrow.DataType) columnType {
	switch dt.ID() {
	// Booleans render as 1 and 0, and the temporal types render as the raw
	// count they store (days, milliseconds, or ticks since the epoch), so all of
	// them reach SQLite as integers.
	case arrow.BOOL,
		arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.DATE32, arrow.DATE64, arrow.TIMESTAMP:
		return columnTypeInteger
	case arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64:
		return columnTypeReal
	default:
		return columnTypeText
	}
}

// processParquetInChunks processes Parquet data in chunks
func (p *streamingParser) processParquetInChunks(reader io.Reader, processor chunkProcessor) error {
	// Read all data into memory (Parquet requires random access)
	data, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read parquet data: %w", err)
	}

	if len(data) == 0 {
		return fmt.Errorf("%w: empty parquet file", ErrEmptyData)
	}

	// Create a bytes reader for the parquet data
	bytesReader := &bytesReaderAt{data: data}

	// Create parquet file reader from bytes
	pqReader, err := pqfile.NewParquetReader(bytesReader)
	if err != nil {
		return fmt.Errorf("failed to create parquet reader from bytes: %w", err)
	}
	defer pqReader.Close()

	// Create arrow file reader
	arrowReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{}, nil)
	if err != nil {
		return fmt.Errorf("failed to create arrow reader: %w", err)
	}

	// Read table to get schema and prepare for chunked reading
	ctx := context.Background()
	table, err := arrowReader.ReadTable(ctx)
	if err != nil {
		return fmt.Errorf("failed to read table: %w", err)
	}
	defer table.Release()

	// Initialize header from table schema
	schema := table.Schema()
	headerSlice := make(header, schema.NumFields())
	for i, field := range schema.Fields() {
		headerSlice[i] = field.Name
	}

	columnInfoList := arrowColumnInfoList(schema)

	// Handle header-only Parquet files (schema exists but no data rows)
	if table.NumRows() == 0 {
		chunk := &tableChunk{
			tableName:  p.tableName,
			headers:    headerSlice,
			records:    nil, // Empty records for header-only file
			columnInfo: columnInfoList,
		}

		if err := processor(chunk); err != nil {
			return fmt.Errorf("chunk processor error: %w", err)
		}
		return nil
	}

	// Process data in chunks using batch reader
	chunkSize := p.chunkSize.Int()
	if chunkSize <= 0 {
		chunkSize = DefaultRowsPerChunk
	}

	tableReader := array.NewTableReader(table, int64(chunkSize))
	defer tableReader.Release()

	for tableReader.Next() {
		batch := tableReader.Record()

		var chunkRecords []record
		var chunkNulls [][]bool
		numRows := batch.NumRows()
		for i := range numRows {
			row := make(record, batch.NumCols())
			nullRow := make([]bool, batch.NumCols())
			for j, col := range batch.Columns() {
				if col.IsNull(int(i)) {
					nullRow[j] = true
					continue
				}
				row[j] = extractValueFromArrowArray(col, i)
			}
			chunkRecords = append(chunkRecords, row)
			chunkNulls = append(chunkNulls, nullRow)
		}

		if len(chunkRecords) > 0 {
			chunk := &tableChunk{
				tableName:  p.tableName,
				headers:    headerSlice,
				records:    chunkRecords,
				columnInfo: columnInfoList,
				nulls:      chunkNulls,
			}

			if err := processor(chunk); err != nil {
				return fmt.Errorf("chunk processor error: %w", err)
			}
		}
	}

	if err := tableReader.Err(); err != nil {
		return fmt.Errorf("error reading table records: %w", err)
	}

	return nil
}

// parseXLSXStream parses XLSX data from reader using memory-optimized streaming approach
// Note: XLSX requires loading entire file into memory due to ZIP format limitations
// For multiple sheets, only the first sheet is processed (streaming parser limitation)
// Use Open/OpenContext for full multi-sheet support with 1-sheet-1-table structure
func (p *streamingParser) parseXLSXStream(reader io.Reader) (*table, error) {
	// Check memory limits before processing
	if p.memoryLimit != nil && p.memoryLimit.checkMemoryUsage() == memoryStatusExceeded {
		return nil, p.memoryLimit.createMemoryError("XLSX parsing")
	}

	// Open XLSX directly from the reader (excelize will buffer as needed)
	xlsxFile, err := excelize.OpenReader(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to open XLSX file: %w", err)
	}
	defer func() {
		_ = xlsxFile.Close() // Ignore close error
	}()

	// Get the sheet names the policy admits
	sheetNames, _, err := selectExcelSheets(xlsxFile, p.excelSheetPolicy)
	if err != nil {
		return nil, err
	}
	if len(sheetNames) == 0 {
		return nil, noExcelSheetsError(xlsxFile, p.excelSheetPolicy)
	}

	// With the streaming parser, we only process the first sheet the policy left
	sheetName := sheetNames[0]
	iter, err := xlsxFile.Rows(sheetName)
	if err != nil {
		return nil, fmt.Errorf("failed to open rows iterator for sheet %s: %w", sheetName, err)
	}
	defer iter.Close()

	var (
		headers header
		first   = true
	)

	// Use memory pool for record slice to reduce allocations
	records := p.memoryPool.getRecordSlice()
	originalRecords := records // Track original slice for proper pool return
	defer func() {
		// Always return the original slice to the pool, even if records grew
		p.memoryPool.putRecordSlice(originalRecords)
	}()

	for iter.Next() {
		// Check memory usage periodically (every 1000 records to reduce ReadMemStats overhead)
		// runtime.ReadMemStats can pause for milliseconds, so we check less frequently
		if p.memoryLimit != nil && len(records)%1000 == 0 {
			if status := p.memoryLimit.checkMemoryUsage(); status == memoryStatusExceeded {
				return nil, p.memoryLimit.createMemoryError("XLSX row processing")
			} else if status == memoryStatusWarning {
				// Force GC at warning threshold
				p.memoryPool.forceGC()
			}
		}

		row, err := iter.Columns()
		if err != nil {
			return nil, fmt.Errorf("failed to read row in sheet %s: %w", sheetName, err)
		}

		// Skip leading empty rows
		if first && len(row) == 0 {
			continue
		}
		if first {
			// Duplicate header check (parity with CSV/TSV)
			if err := validateColumnNames(row); err != nil {
				return nil, err
			}
			headers = newHeader(row)
			first = false
			continue
		}
		records = append(records, newRecord(row))
	}

	if len(headers) == 0 {
		return nil, fmt.Errorf("sheet %s is empty in XLSX file", sheetName)
	}

	return newTable(p.tableName, headers, records), nil
}

// processXLSXInChunks processes XLSX data in chunks with memory optimization
func (p *streamingParser) processXLSXInChunks(reader io.Reader, processor chunkProcessor) error {
	// Check memory limits before processing
	if p.memoryLimit != nil && p.memoryLimit.checkMemoryUsage() == memoryStatusExceeded {
		return p.memoryLimit.createMemoryError("XLSX chunk processing")
	}

	// Open XLSX file from reader
	xlsxFile, err := excelize.OpenReader(reader)
	if err != nil {
		return fmt.Errorf("failed to open XLSX file: %w", err)
	}
	defer func() {
		_ = xlsxFile.Close() // Ignore close error
	}()

	// Get the sheet names the policy admits
	sheetNames, _, err := selectExcelSheets(xlsxFile, p.excelSheetPolicy)
	if err != nil {
		return err
	}
	if len(sheetNames) == 0 {
		return noExcelSheetsError(xlsxFile, p.excelSheetPolicy)
	}

	// Process only the first admitted sheet (streaming parser limitation)
	sheetName := sheetNames[0]
	iter, err := xlsxFile.Rows(sheetName)
	if err != nil {
		return fmt.Errorf("failed to open rows iterator for sheet %s: %w", sheetName, err)
	}
	defer iter.Close()

	var (
		headers       header
		columnInfo    columnInfoList
		columnValues  [][]string
		first         = true
		chunkRecords  []record
		processedRows int
	)

	// Get base chunk size and adjust for memory limits
	chunkSize := p.chunkSize.Int()
	if chunkSize <= 0 {
		chunkSize = DefaultRowsPerChunk
	}

	// Adjust chunk size based on memory usage
	if p.memoryLimit != nil {
		if shouldReduce, newSize := p.memoryLimit.shouldReduceChunkSize(chunkSize); shouldReduce {
			chunkSize = newSize
			if chunkSize < 1 {
				chunkSize = 1
			}
		}
	}

	// Use memory pool for chunk records
	chunkRecords = p.memoryPool.getRecordSlice()
	originalChunkRecords := chunkRecords // Track original slice for proper pool return
	defer func() {
		// Always return the original slice to the pool, even if chunkRecords grew
		p.memoryPool.putRecordSlice(originalChunkRecords)
	}()

	for iter.Next() {
		// Check memory usage periodically (every 1000 rows to reduce ReadMemStats overhead)
		// runtime.ReadMemStats can pause for milliseconds, so we check less frequently
		if p.memoryLimit != nil && processedRows%1000 == 0 {
			if status := p.memoryLimit.checkMemoryUsage(); status == memoryStatusExceeded {
				return p.memoryLimit.createMemoryError("XLSX row processing")
			} else if status == memoryStatusWarning {
				// Force GC and reduce chunk size on memory pressure
				p.memoryPool.forceGC()
				runtime.GC()
				chunkSize = chunkSize / 2
				if chunkSize < 1 {
					chunkSize = 1
				}
			}
		}

		row, err := iter.Columns()
		if err != nil {
			return fmt.Errorf("failed to read row in sheet %s: %w", sheetName, err)
		}

		// Skip leading empty rows
		if first && len(row) == 0 {
			continue
		}

		if first {
			// Validate headers for duplicates
			if err := validateColumnNames(row); err != nil {
				return err
			}
			headers = newHeader(row)
			first = false
			continue
		}

		chunkRecords = append(chunkRecords, newRecord(row))
		processedRows++

		// Collect values for type inference (only on first chunk)
		if len(columnInfo) == 0 {
			if len(columnValues) == 0 {
				columnValues = make([][]string, len(headers))
			}
			for i, val := range row {
				if i < len(columnValues) {
					columnValues[i] = append(columnValues[i], val)
				}
			}
		}

		// Process chunk when it reaches the target size
		if len(chunkRecords) >= chunkSize {
			// Infer column types on first chunk. A later chunk does not
			// re-infer, but it can still hold a value a numeric column would
			// damage, so it is asked. See promoteForRecords.
			if len(columnInfo) != 0 {
				columnInfo.promoteForRecords(chunkRecords)
			} else {
				columnInfo = newColumnInfoListFromValues(headers, columnValues)
			}

			// Copy to decouple from the reused backing array
			chunkData := append([]record(nil), chunkRecords...)
			chunk := &tableChunk{
				tableName:  p.tableName,
				headers:    headers,
				records:    chunkData,
				columnInfo: columnInfo,
			}

			if err := processor(chunk); err != nil {
				return fmt.Errorf("chunk processor error: %w", err)
			}

			// Reset for next chunk, reuse memory pool slice
			chunkRecords = chunkRecords[:0] // Reset length but keep capacity
			columnValues = nil              // Don't collect values after first chunk
		}
	}

	// Process remaining records
	if len(chunkRecords) > 0 {
		// Infer column types if we haven't yet (small dataset)
		if len(columnInfo) != 0 {
			columnInfo.promoteForRecords(chunkRecords)
		} else {
			columnInfo = newColumnInfoListFromValues(headers, columnValues)
		}

		// Copy to decouple from the reused backing array
		chunkData := append([]record(nil), chunkRecords...)
		chunk := &tableChunk{
			tableName:  p.tableName,
			headers:    headers,
			records:    chunkData,
			columnInfo: columnInfo,
		}

		if err := processor(chunk); err != nil {
			return fmt.Errorf("chunk processor error: %w", err)
		}
	} else if len(columnInfo) == 0 && len(headers) > 0 {
		// Handle header-only XLSX files: create empty chunk with header information
		// This ensures table is created with correct column names even when no data records exist
		columnInfo = newColumnInfoListFromValues(headers, columnValues)

		chunk := &tableChunk{
			tableName:  p.tableName,
			headers:    headers,
			records:    nil, // Empty records for header-only file
			columnInfo: columnInfo,
		}

		if err := processor(chunk); err != nil {
			return fmt.Errorf("chunk processor error: %w", err)
		}
	} else if len(headers) == 0 {
		// Completely empty XLSX - no headers, no data
		return fmt.Errorf("sheet %s is empty in XLSX file", sheetName)
	}

	return nil
}

// jsonDataHeader is the column name for JSON data storage.
// JSON data is stored as raw JSON strings in a single TEXT column.
// Users can query fields using SQLite's json_extract() function.
//
// Example SQL:
//
//	SELECT json_extract(data, '$.name') AS name FROM my_json_table;
const jsonDataHeader = "data"

// parseJSONStream parses JSON data from reader and returns a table.
// Array root: each element becomes a row with raw JSON in the "data" column.
// Object root: single row with the entire object as raw JSON.
//
// This approach stores raw JSON and relies on SQLite's json_extract() for field access,
// making it robust against arbitrarily nested or complex JSON structures.
//
// Example usage with SQLite:
//
//	SELECT json_extract(data, '$.name') FROM my_json_table;
//	SELECT json_extract(data, '$.address.city') FROM my_json_table;
func (p *streamingParser) parseJSONStream(reader io.Reader) (*table, error) {
	content, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to read JSON: %w", ErrParsing, err)
	}

	trimmed := strings.TrimSpace(string(content))
	if trimmed == "" {
		return nil, fmt.Errorf("%w: empty JSON data", ErrEmptyData)
	}

	h := newHeader([]string{jsonDataHeader})
	colInfo := []columnInfo{newColumnInfoWithType(jsonDataHeader)}

	// Try to parse as array first
	var arr []json.RawMessage
	if err := json.Unmarshal([]byte(trimmed), &arr); err == nil {
		if len(arr) == 0 {
			return nil, fmt.Errorf("%w: empty JSON array", ErrEmptyData)
		}
		records := make([]record, 0, len(arr))
		for _, elem := range arr {
			records = append(records, newRecord([]string{string(elem)}))
		}
		t := newTable(p.tableName, h, records)
		t.columnInfo = colInfo
		return t, nil
	}

	// Try as single value (object, string, number, boolean, null)
	var obj json.RawMessage
	if err := json.Unmarshal([]byte(trimmed), &obj); err != nil {
		return nil, fmt.Errorf("%w: failed to parse JSON: %w", ErrInvalidData, err)
	}

	t := newTable(p.tableName, h, []record{newRecord([]string{string(obj)})})
	t.columnInfo = colInfo
	return t, nil
}

// parseJSONLStream parses JSON Lines data from reader and returns a table.
// Each non-empty line must be valid JSON and becomes a row with raw JSON in "data" column.
// Empty lines are silently skipped. There is no line size limit.
//
// JSONL (JSON Lines) format stores one JSON value per line, making it ideal for
// streaming and append-only log files. Each line is independently valid JSON.
//
// Example usage with SQLite:
//
//	SELECT json_extract(data, '$.status') FROM my_jsonl_table
//	WHERE json_extract(data, '$.code') = 200;
func (p *streamingParser) parseJSONLStream(reader io.Reader) (*table, error) {
	br := bufio.NewReader(reader)

	h := newHeader([]string{jsonDataHeader})
	colInfo := []columnInfo{newColumnInfoWithType(jsonDataHeader)}
	var records []record
	lineNum := 0

	for {
		rawLine, err := br.ReadBytes('\n')
		// Process whatever we got before checking the error.
		// ReadBytes returns data even when err == io.EOF.
		lineNum++
		line := strings.TrimSpace(string(rawLine))
		if line != "" {
			if !json.Valid([]byte(line)) {
				return nil, fmt.Errorf("%w: invalid JSON on line %d: %s",
					ErrInvalidData, lineNum, truncateLineForError(line, 100))
			}
			records = append(records, newRecord([]string{line}))
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("%w: failed to read JSONL: %w", ErrParsing, err)
		}
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("%w: empty JSONL data", ErrEmptyData)
	}

	t := newTable(p.tableName, h, records)
	t.columnInfo = colInfo
	return t, nil
}

// processJSONInChunks processes JSON data in chunks using a streaming decoder.
// For JSON arrays, elements are decoded one at a time from the stream without loading
// the entire array into memory, making it safe for large files.
// Single objects (non-array) are read fully and processed as a single chunk.
func (p *streamingParser) processJSONInChunks(reader io.Reader, processor chunkProcessor) error {
	// Use bufio.Reader to peek at the first non-whitespace byte and decide the strategy.
	br := bufio.NewReader(reader)
	h := newHeader([]string{jsonDataHeader})
	colInfo := []columnInfo{newColumnInfoWithType(jsonDataHeader)}
	isArray, err := peekJSONIsArray(br)
	if err != nil {
		return err
	}

	if isArray {
		// Stream array elements one by one via json.Decoder.
		dec := json.NewDecoder(br)
		// Consume the opening '['
		if _, err := dec.Token(); err != nil {
			return fmt.Errorf("%w: failed to parse JSON array: %w", ErrInvalidData, err)
		}
		return p.processJSONArrayChunks(dec, h, colInfo, processor)
	}

	// Non-array: read the whole value and process as a single-row chunk.
	content, err := io.ReadAll(br)
	if err != nil {
		return fmt.Errorf("%w: failed to read JSON: %w", ErrParsing, err)
	}

	trimmed := strings.TrimSpace(string(content))
	if trimmed == "" {
		return fmt.Errorf("%w: empty JSON data", ErrEmptyData)
	}

	var obj json.RawMessage
	if unmarshalErr := json.Unmarshal([]byte(trimmed), &obj); unmarshalErr != nil {
		return fmt.Errorf("%w: failed to parse JSON: %w", ErrInvalidData, unmarshalErr)
	}

	chunk := &tableChunk{
		tableName:  p.tableName,
		headers:    h,
		records:    []record{newRecord([]string{string(obj)})},
		columnInfo: colInfo,
	}
	return processor(chunk)
}

// peekJSONIsArray peeks at the reader to determine if the JSON value starts with '['.
// Leading whitespace is skipped. Returns an error if the reader is empty.
func peekJSONIsArray(br *bufio.Reader) (bool, error) {
	for {
		b, err := br.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return false, fmt.Errorf("%w: empty JSON data", ErrEmptyData)
			}
			return false, fmt.Errorf("%w: failed to read JSON: %w", ErrParsing, err)
		}
		// Skip whitespace
		if b == ' ' || b == '\t' || b == '\n' || b == '\r' {
			continue
		}
		// Put the byte back so the decoder / ReadAll can consume it
		if unreadErr := br.UnreadByte(); unreadErr != nil {
			return false, fmt.Errorf("%w: failed to read JSON: %w", ErrParsing, unreadErr)
		}
		return b == '[', nil
	}
}

// processJSONArrayChunks streams elements from an already-opened JSON array via decoder.
// The opening '[' must have already been consumed before calling this function.
func (p *streamingParser) processJSONArrayChunks(
	dec *json.Decoder, h header, colInfo []columnInfo, processor chunkProcessor,
) error {
	chunkSize := p.chunkSize.Int()
	if chunkSize <= 0 {
		chunkSize = DefaultRowsPerChunk
	}

	var chunkRecords []record
	totalRecords := 0

	for dec.More() {
		var elem json.RawMessage
		if err := dec.Decode(&elem); err != nil {
			return fmt.Errorf("%w: failed to decode JSON array element: %w", ErrParsing, err)
		}
		chunkRecords = append(chunkRecords, newRecord([]string{string(elem)}))
		totalRecords++

		if len(chunkRecords) >= chunkSize {
			chunk := &tableChunk{
				tableName:  p.tableName,
				headers:    h,
				records:    chunkRecords,
				columnInfo: colInfo,
			}
			if err := processor(chunk); err != nil {
				return fmt.Errorf("chunk processor error: %w", err)
			}
			chunkRecords = nil
		}
	}

	// Consume the closing ']'
	if _, err := dec.Token(); err != nil {
		return fmt.Errorf("%w: failed to read JSON array end: %w", ErrParsing, err)
	}

	// Reject trailing data after the array (e.g. "[1] garbage")
	if dec.More() {
		return fmt.Errorf("%w: unexpected data after JSON array", ErrInvalidData)
	}

	if totalRecords == 0 {
		return processor(&tableChunk{tableName: p.tableName, headers: h, columnInfo: colInfo})
	}

	// Process remaining records
	if len(chunkRecords) > 0 {
		chunk := &tableChunk{
			tableName:  p.tableName,
			headers:    h,
			records:    chunkRecords,
			columnInfo: colInfo,
		}
		if err := processor(chunk); err != nil {
			return fmt.Errorf("chunk processor error: %w", err)
		}
	}

	return nil
}

// processJSONLInChunks processes JSONL data in chunks with true streaming.
// Lines are read one by one and accumulated into chunks, calling the processor
// callback each time the chunk reaches the configured size.
func (p *streamingParser) processJSONLInChunks(reader io.Reader, processor chunkProcessor) error {
	br := bufio.NewReader(reader)

	h := newHeader([]string{jsonDataHeader})
	colInfo := []columnInfo{newColumnInfoWithType(jsonDataHeader)}

	chunkSize := p.chunkSize.Int()
	if chunkSize <= 0 {
		chunkSize = DefaultRowsPerChunk
	}

	var chunkRecords []record
	lineNum := 0
	totalRecords := 0

	for {
		rawLine, err := br.ReadBytes('\n')
		lineNum++
		line := strings.TrimSpace(string(rawLine))
		if line != "" {
			if !json.Valid([]byte(line)) {
				return fmt.Errorf("%w: invalid JSON on line %d: %s",
					ErrInvalidData, lineNum, truncateLineForError(line, 100))
			}
			chunkRecords = append(chunkRecords, newRecord([]string{line}))
			totalRecords++

			if len(chunkRecords) >= chunkSize {
				chunk := &tableChunk{
					tableName:  p.tableName,
					headers:    h,
					records:    chunkRecords,
					columnInfo: colInfo,
				}
				if err := processor(chunk); err != nil {
					return fmt.Errorf("chunk processor error: %w", err)
				}
				chunkRecords = nil
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("%w: failed to read JSONL: %w", ErrParsing, err)
		}
	}

	// Process remaining records
	if len(chunkRecords) > 0 {
		chunk := &tableChunk{
			tableName:  p.tableName,
			headers:    h,
			records:    chunkRecords,
			columnInfo: colInfo,
		}
		if err := processor(chunk); err != nil {
			return fmt.Errorf("chunk processor error: %w", err)
		}
	}

	if totalRecords == 0 {
		return fmt.Errorf("%w: empty JSONL data", ErrEmptyData)
	}

	return nil
}

// truncateLineForError truncates a string to maxLen characters for error messages.
func truncateLineForError(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
