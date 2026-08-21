package filesql

import (
	"bufio"
	"bytes"
	"context"
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

// closeQuietly closes what a decompression reader handed back, dropping the
// error: a close failure on a reader says nothing about the data already read
// from it, and the load either succeeded or has an error of its own to report.
//
// It performs the close rather than returning a function that does, because a
// helper of the second shape reads as if the defer closes and does not: two of
// the three callers wrote "defer handleCloseError(f)" and closed nothing, which
// is a shape the compiler cannot object to.
func closeQuietly(closeFunc func() error) {
	if closeFunc == nil {
		return
	}
	if closeErr := closeFunc(); closeErr != nil {
		// In the future, this could be enhanced with proper logging
		_ = closeErr
	}
}

// readArrowTable is pqarrow.FileReader.ReadTable done in the calling goroutine,
// with the panic a damaged file can raise turned into an error.
//
// A Parquet file this package did not write is untrusted input, and the reader
// panics on some of it rather than reporting: a corrupted page header reaches a
// nil dereference, and a row group that fails to read is cleaned up by releasing
// a column that was never built. ReadTable does the column reads in goroutines
// of its own, where a recover here cannot reach them and the process dies, so
// the same reads are done here instead -- one column at a time, in this
// goroutine, where the boundary can be held. A caller reading a file chosen by
// someone else cannot defend against a panic; every other malformed input in
// this package is an error, and this one is too now.
//
// The recover and this loop can go when the library stops panicking on its own
// error paths; ReadTable is the call this replaces.
func readArrowTable(ctx context.Context, arrowReader *pqarrow.FileReader) (tbl arrow.Table, err error) {
	defer func() {
		if r := recover(); r != nil {
			tbl = nil
			err = fmt.Errorf("parquet data is damaged: %v", r)
		}
	}()

	meta := arrowReader.ParquetReader().MetaData()
	columnIndices := make([]int, meta.Schema.NumColumns())
	for i := range columnIndices {
		columnIndices[i] = i
	}
	rowGroups := make([]int, arrowReader.ParquetReader().NumRowGroups())
	for i := range rowGroups {
		rowGroups[i] = i
	}

	// GetFieldReaders, the plural form, fans the per-field reads out into an
	// errgroup, and a panic in one of those goroutines is not this function's to
	// recover either. The singular form does the same work here.
	fieldIndices, err := arrowReader.Manifest.GetFieldIndices(columnIndices)
	if err != nil {
		return nil, err
	}
	includedLeaves := make(map[int]bool, len(columnIndices))
	for _, col := range columnIndices {
		includedLeaves[col] = true
	}
	// The readers are collected as they are built and released together, so a
	// failure part way through this loop does not leave the ones before it
	// unreleased.
	readers := make([]*pqarrow.ColumnReader, 0, len(fieldIndices))
	defer func() {
		for _, reader := range readers {
			reader.Release()
		}
	}()
	fields := make([]arrow.Field, 0, len(fieldIndices))
	for _, fieldIndex := range fieldIndices {
		reader, readerErr := arrowReader.GetFieldReader(ctx, fieldIndex, includedLeaves, rowGroups)
		if readerErr != nil {
			return nil, readerErr
		}
		if reader == nil || reader.Field() == nil {
			return nil, fmt.Errorf("parquet data is damaged: no reader for field %d", fieldIndex)
		}
		readers = append(readers, reader)
		fields = append(fields, *reader.Field())
	}
	schema := arrow.NewSchema(fields, arrowReader.Manifest.SchemaMeta)

	// The columns are appended as they are built, so the cleanup below releases
	// the ones that exist and never a zero value, whose Release dereferences a
	// chunk it does not have.
	columns := make([]arrow.Column, 0, len(readers))
	defer func() {
		// The columns are copied into the table, which owns them from then on;
		// on the way out with an error they are this function's to release.
		if err == nil {
			return
		}
		for i := range columns {
			columns[i].Release()
		}
	}()
	for i, reader := range readers {
		chunked, readErr := arrowReader.ReadColumn(rowGroups, reader)
		if readErr != nil {
			return nil, readErr
		}
		columns = append(columns, *arrow.NewColumn(schema.Field(i), chunked))
		chunked.Release()
	}

	var rows int
	if len(columns) > 0 {
		rows = columns[0].Len()
	}
	return array.NewTable(schema, columns, int64(rows)), nil
}

// parquetMagic is the four bytes a Parquet file begins and ends with. The
// reader underneath checks only the trailing one, so a file that ends "PAR1" and
// begins with anything is read into its metadata, where damaged input has
// reached a panic and an allocation that does not stop.
var parquetMagic = []byte("PAR1") //nolint:gochecknoglobals // constant-like

// errNotParquet reports bytes that do not begin the way the format says.
func errNotParquet(head []byte) error {
	return fmt.Errorf("not a parquet file: it begins %q rather than %q", head, parquetMagic)
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

// createDecompressedReader wraps reader with the codec the source was declared
// to use. The per-format switch this replaced was a second implementation of
// CompressionHandler.CreateReader, reached through the fused FileType.
//
// CompressionNone returns the reader unchanged with a no-op close function,
// following CreateReader's convention that the close function is never nil.
func (p *streamingParser) createDecompressedReader(reader io.Reader) (io.Reader, func() error, error) {
	return NewCompressionHandler(p.compression).CreateReader(reader)
}

// delimitedReader reads the records of a delimited file. CSV and TSV need
// different readers because the formats differ on what a quote means: CSV
// escapes with it, TSV has no escape at all.
type delimitedReader interface {
	Read() ([]string, error)
	ReadAll() ([][]string, error)
}

// newDelimitedReader picks the reader delimiter names, over normalized line
// endings so a carriage-return terminated file is read as lines.
func newDelimitedReader(reader io.Reader, delimiter rune) delimitedReader {
	normalized := parser.NormalizeLineEndings(reader)
	if delimiter == tsvDelimiter {
		return parser.NewTSVReader(normalized)
	}

	csvReader := parser.NewCSVReader(normalized)
	csvReader.Comma = delimiter
	// Accept a variable field count so a ragged row is handled by the configured
	// malformed-row policy instead of aborting the whole read.
	csvReader.FieldsPerRecord = -1
	return csvReader
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

// add records a label, ignoring one already seen. Two labels differing only in
// case are one label, the way they are within a single record and the way
// SQLite compares the column names it ends up holding: keeping them apart made
// "id" and "ID" two columns, and the table SQLite was then asked to create was
// refused with an error naming neither the file nor the rule. The spelling kept
// is the one that named the column first.
func (l *labelOrder) add(name string) {
	folded := ltsvLabelKey(name)
	if l.seen[folded] {
		return
	}
	l.seen[folded] = true
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

// ProcessInChunks reads the input in chunks, handing each to processor, and
// returns the columns with the type every row read requires. The types come
// last because they are not known until the last row has been read: a column
// is declared once, after the whole input has been seen, so where a chunk
// boundary falls cannot change what a table holds.
func (p *streamingParser) ProcessInChunks(reader io.Reader, processor chunkProcessor) (columnInfoList, error) {
	decompressedReader, closeFunc, err := p.createDecompressedReader(reader)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to create decompressed reader: %w", ErrCompression, err)
	}
	defer closeQuietly(closeFunc)

	if isTextBaseType(p.fileType) {
		decompressedReader = decodeTextReader(decompressedReader)
	}
	switch p.fileType {
	case FileTypeCSV:
		return p.processDelimitedInChunks(decompressedReader, processor, csvDelimiter, "CSV")
	case FileTypeTSV:
		return p.processDelimitedInChunks(decompressedReader, processor, tsvDelimiter, "TSV")
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
		return nil, fmt.Errorf("%w: unsupported file type for chunked processing", ErrUnsupportedFormat)
	}
}

// emitChunk hands one chunk of rows to the processor, typed by every row read
// so far. nulls marks the cells that are SQL NULL rather than text, and is nil
// for a format without nulls.
func (p *streamingParser) emitChunk(processor chunkProcessor, headers header, records []record, types columnInfoList, nulls [][]bool) error {
	chunk := &tableChunk{
		tableName: p.tableName,
		headers:   headers,
		records:   records,
		types:     types,
		nulls:     nulls,
	}
	if err := processor(chunk); err != nil {
		return fmt.Errorf("chunk processor error: %w", err)
	}
	return nil
}

// processDelimitedInChunks reads CSV or TSV rows in chunks.
func (p *streamingParser) processDelimitedInChunks(reader io.Reader, processor chunkProcessor, delimiter rune, fileTypeName string) (columnInfoList, error) {
	recordReader := newDelimitedReader(reader, delimiter)

	headerrecord, err := recordReader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("%w: empty %s data", ErrEmptyData, fileTypeName)
		}
		return nil, fmt.Errorf("%w: failed to read %s header: %w", ErrParsing, fileTypeName, err)
	}
	if err := validateColumnNames(headerrecord); err != nil {
		return nil, err
	}

	header := newHeader(headerrecord)
	evidence := newColumnEvidenceList(len(header))
	chunkSize := p.chunkSize.Int()

	var chunkrecords []record
	emitted := false
	rowNum := 0
	for {
		record, err := recordReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("%w: failed to read %s record: %w", ErrParsing, fileTypeName, err)
		}
		rowNum++
		p.totalRows++

		record, skip, err := reconcileFieldCount(record, len(header), rowNum, p.malformedRowPolicy)
		if err != nil {
			return nil, err
		}
		if skip {
			p.skippedRows++
			continue
		}

		row := newRecord(record)
		evidence.addRecord(row)
		chunkrecords = append(chunkrecords, row)
		if len(chunkrecords) >= chunkSize {
			if err := p.emitChunk(processor, header, chunkrecords, evidence.columnInfos(header), nil); err != nil {
				return nil, err
			}
			chunkrecords = nil
			emitted = true
		}
	}

	// A file whose rows were all skipped, and one that is nothing but a header,
	// still emit an empty chunk so the table is created with the columns the
	// header names.
	if len(chunkrecords) > 0 || !emitted {
		if err := p.emitChunk(processor, header, chunkrecords, evidence.columnInfos(header), nil); err != nil {
			return nil, err
		}
	}
	return evidence.columnInfos(header), nil
}

// processLTSVInChunks processes LTSV data in chunks
func (p *streamingParser) processLTSVInChunks(reader io.Reader, processor chunkProcessor) (columnInfoList, error) {
	// For LTSV, we need to read line by line
	content, err := io.ReadAll(parser.NormalizeLineEndings(reader))
	if err != nil {
		return nil, fmt.Errorf("%w: failed to read LTSV: %w", ErrParsing, err)
	}

	lines := strings.Split(string(content), "\n")
	if len(lines) == 0 {
		return nil, fmt.Errorf("%w: empty LTSV data", ErrEmptyData)
	}

	// First pass: collect the labels in the order they first appear. A map
	// would lose the order, and the column order is the file's to decide.
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
		return nil, fmt.Errorf("%w: no valid LTSV keys found", ErrEmptyData)
	}

	header := header(labels.names())

	// Second pass: process records in chunks
	chunkrecords := make([]record, 0) // Pre-allocate slice
	evidence := newColumnEvidenceList(len(header))

	chunkSize := p.chunkSize.Int()

	for _, line := range lines {
		// Only the line terminator is removed. TrimSpace took the trailing spaces
		// of the last field with it, so a value ending in a space lost it.
		line = strings.TrimRight(line, "\r")
		if line == "" {
			continue
		}

		recordMap := make(map[string]string)
		// Labels are compared folded; see ltsvLabelKey. recordMap is keyed that
		// way so a record finds its value under the column the first record
		// named, whatever case this one wrote it in.
		seen := make(map[string]struct{})
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
				if _, dup := seen[ltsvLabelKey(key)]; dup {
					return nil, fmt.Errorf("%w: %q in LTSV record", errDuplicateColumnName, key)
				}
				seen[ltsvLabelKey(key)] = struct{}{}
				recordMap[ltsvLabelKey(key)] = value
			}
		}

		if len(recordMap) == 0 {
			continue
		}

		var row record
		for _, key := range header {
			if val, exists := recordMap[ltsvLabelKey(key)]; exists {
				row = append(row, val)
			} else {
				row = append(row, "")
			}
		}
		evidence.addRecord(row)
		chunkrecords = append(chunkrecords, row)

		// Process chunk when it reaches the target size
		if len(chunkrecords) >= chunkSize {
			if err := p.emitChunk(processor, header, chunkrecords, evidence.columnInfos(header), nil); err != nil {
				return nil, err
			}
			chunkrecords = nil
		}
	}

	// Process remaining records
	if len(chunkrecords) > 0 {
		if err := p.emitChunk(processor, header, chunkrecords, evidence.columnInfos(header), nil); err != nil {
			return nil, err
		}
	}
	return evidence.columnInfos(header), nil
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
func (p *streamingParser) processParquetInChunks(reader io.Reader, processor chunkProcessor) (columnInfoList, error) {
	// Read all data into memory (Parquet requires random access)
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read parquet data: %w", err)
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("%w: empty parquet file", ErrEmptyData)
	}
	if !bytes.HasPrefix(data, parquetMagic) {
		return nil, errNotParquet(data[:min(len(data), len(parquetMagic))])
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

	// Read table to get schema and prepare for chunked reading
	ctx := context.Background()
	table, err := readArrowTable(ctx, arrowReader)
	if err != nil {
		return nil, fmt.Errorf("failed to read table: %w", err)
	}
	defer table.Release()

	// Initialize header from table schema
	schema := table.Schema()
	headerSlice := make(header, schema.NumFields())
	for i, field := range schema.Fields() {
		headerSlice[i] = field.Name
	}

	// The schema outranks what the values look like: a DOUBLE column holding
	// whole numbers is still REAL, and a STRING column of digits is still TEXT.
	columns := arrowColumnInfoList(schema)

	// A file with a schema and no rows still names its columns.
	if table.NumRows() == 0 {
		if err := p.emitChunk(processor, headerSlice, nil, columns, nil); err != nil {
			return nil, err
		}
		return columns, nil
	}

	tableReader := array.NewTableReader(table, int64(p.chunkSize.Int()))
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
				if arrowCellIsNull(col, i) {
					nullRow[j] = true
					continue
				}
				row[j] = extractValueFromArrowArray(col, i)
			}
			chunkRecords = append(chunkRecords, row)
			chunkNulls = append(chunkNulls, nullRow)
		}

		if len(chunkRecords) > 0 {
			if err := p.emitChunk(processor, headerSlice, chunkRecords, columns, chunkNulls); err != nil {
				return nil, err
			}
		}
	}

	if err := tableReader.Err(); err != nil {
		return nil, fmt.Errorf("error reading table records: %w", err)
	}
	return columns, nil
}

// processXLSXInChunks processes XLSX data in chunks with memory optimization
func (p *streamingParser) processXLSXInChunks(reader io.Reader, processor chunkProcessor) (columnInfoList, error) {
	// Check memory limits before processing
	if p.memoryLimit != nil && p.memoryLimit.checkMemoryUsage() == memoryStatusExceeded {
		return nil, p.memoryLimit.createMemoryError("XLSX chunk processing")
	}

	// Open XLSX file from reader
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

	// Process only the first admitted sheet (streaming parser limitation)
	sheetName := sheetNames[0]
	iter, err := xlsxFile.Rows(sheetName)
	if err != nil {
		return nil, fmt.Errorf("failed to open rows iterator for sheet %s: %w", sheetName, err)
	}
	defer iter.Close()

	var (
		headers       header
		evidence      columnEvidenceList
		first         = true
		emitted       bool
		chunkRecords  []record
		processedRows int
	)

	// Get base chunk size and adjust for memory limits
	chunkSize := p.chunkSize.Int()

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
				return nil, p.memoryLimit.createMemoryError("XLSX row processing")
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
			return nil, fmt.Errorf("failed to read row in sheet %s: %w", sheetName, err)
		}

		// Skip leading empty rows
		if first && len(row) == 0 {
			continue
		}

		if first {
			// Validate headers for duplicates
			if err := validateColumnNames(row); err != nil {
				return nil, err
			}
			headers = newHeader(row)
			evidence = newColumnEvidenceList(len(headers))
			first = false
			continue
		}

		// A workbook stores no cell for a trailing empty one, so a row ending in
		// blanks arrives short and means what the padding says. More cells than
		// the header has means the opposite — there is data in a column the
		// header does not name — and the extra cells used to be dropped with no
		// error and no count to say it had happened.
		if len(row) > len(headers) {
			return nil, fmt.Errorf("%w: sheet %s row %d has %d cells where the header has %d",
				ErrParsing, sheetName, processedRows+2, len(row), len(headers))
		}

		cells := newRecord(row)
		evidence.addRecord(cells)
		chunkRecords = append(chunkRecords, cells)
		processedRows++

		// Process chunk when it reaches the target size
		if len(chunkRecords) >= chunkSize {
			// Copy to decouple from the reused backing array
			chunkData := append([]record(nil), chunkRecords...)
			if err := p.emitChunk(processor, headers, chunkData, evidence.columnInfos(headers), nil); err != nil {
				return nil, err
			}

			// Reset for next chunk, reuse memory pool slice
			chunkRecords = chunkRecords[:0] // Reset length but keep capacity
			emitted = true
		}
	}

	// Process remaining records
	if len(chunkRecords) > 0 {
		// Copy to decouple from the reused backing array
		chunkData := append([]record(nil), chunkRecords...)
		if err := p.emitChunk(processor, headers, chunkData, evidence.columnInfos(headers), nil); err != nil {
			return nil, err
		}
	} else if !emitted && len(headers) > 0 {
		// A sheet that is nothing but a header still names its columns.
		if err := p.emitChunk(processor, headers, nil, evidence.columnInfos(headers), nil); err != nil {
			return nil, err
		}
	} else if len(headers) == 0 {
		return nil, fmt.Errorf("sheet %s is empty in XLSX file", sheetName)
	}
	return evidence.columnInfos(headers), nil
}

// jsonDataHeader is the column name for JSON data storage.
// JSON data is stored as raw JSON strings in a single TEXT column.
// Users can query fields using SQLite's json_extract() function.
//
// Example SQL:
//
//	SELECT json_extract(data, '$.name') AS name FROM my_json_table;
const jsonDataHeader = "data"

// processJSONInChunks reads a JSON document into the single "data" column. An
// array is streamed element by element; any other value is one row.
func (p *streamingParser) processJSONInChunks(reader io.Reader, processor chunkProcessor) (columnInfoList, error) {
	br := bufio.NewReader(reader)
	h := newHeader([]string{jsonDataHeader})
	columns := columnInfoList{newJSONDataColumn()}

	isArray, err := peekJSONIsArray(br)
	if err != nil {
		if errors.Is(err, ErrEmptyData) {
			// An empty document is a table with no rows, not a failure.
			return columns, p.emitChunk(processor, h, nil, columns, nil)
		}
		return nil, err
	}

	if isArray {
		dec := json.NewDecoder(br)
		if _, err := dec.Token(); err != nil {
			return nil, fmt.Errorf("%w: failed to parse JSON array: %w", ErrInvalidData, err)
		}
		if err := p.processJSONArrayChunks(dec, h, columns, processor); err != nil {
			return nil, err
		}
		return columns, nil
	}

	content, err := io.ReadAll(br)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to read JSON: %w", ErrParsing, err)
	}
	var obj json.RawMessage
	if unmarshalErr := json.Unmarshal(bytes.TrimSpace(content), &obj); unmarshalErr != nil {
		return nil, fmt.Errorf("%w: failed to parse JSON: %w", ErrInvalidData, unmarshalErr)
	}
	if err := p.emitChunk(processor, h, []record{newRecord([]string{string(obj)})}, columns, nil); err != nil {
		return nil, err
	}
	return columns, nil
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

// processJSONArrayChunks streams the elements of an array whose opening
// bracket the decoder has already consumed.
func (p *streamingParser) processJSONArrayChunks(dec *json.Decoder, h header, columns columnInfoList, processor chunkProcessor) error {
	chunkSize := p.chunkSize.Int()
	var chunkRecords []record
	emitted := false
	for dec.More() {
		var elem json.RawMessage
		if err := dec.Decode(&elem); err != nil {
			return fmt.Errorf("%w: failed to decode JSON array element: %w", ErrParsing, err)
		}
		chunkRecords = append(chunkRecords, newRecord([]string{string(elem)}))
		if len(chunkRecords) >= chunkSize {
			if err := p.emitChunk(processor, h, chunkRecords, columns, nil); err != nil {
				return err
			}
			chunkRecords = nil
			emitted = true
		}
	}

	// Consume the closing bracket, then refuse anything after it ("[1] garbage").
	if _, err := dec.Token(); err != nil {
		return fmt.Errorf("%w: failed to read JSON array end: %w", ErrParsing, err)
	}
	if dec.More() {
		return fmt.Errorf("%w: unexpected data after JSON array", ErrInvalidData)
	}

	// An empty array still makes its table.
	if len(chunkRecords) > 0 || !emitted {
		return p.emitChunk(processor, h, chunkRecords, columns, nil)
	}
	return nil
}

// processJSONLInChunks reads one JSON value per line into the "data" column.
func (p *streamingParser) processJSONLInChunks(reader io.Reader, processor chunkProcessor) (columnInfoList, error) {
	br := bufio.NewReader(reader)
	h := newHeader([]string{jsonDataHeader})
	columns := columnInfoList{newJSONDataColumn()}
	chunkSize := p.chunkSize.Int()

	var chunkRecords []record
	emitted := false
	lineNum := 0
	for {
		rawLine, err := br.ReadBytes('\n')
		lineNum++
		line := strings.TrimSpace(string(rawLine))
		if line != "" {
			if !json.Valid([]byte(line)) {
				return nil, fmt.Errorf("%w: invalid JSON on line %d: %s",
					ErrInvalidData, lineNum, truncateLineForError(line, 100))
			}
			chunkRecords = append(chunkRecords, newRecord([]string{line}))
			if len(chunkRecords) >= chunkSize {
				if err := p.emitChunk(processor, h, chunkRecords, columns, nil); err != nil {
					return nil, err
				}
				chunkRecords = nil
				emitted = true
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("%w: failed to read JSONL: %w", ErrParsing, err)
		}
	}

	// An input with no lines is a table with no rows.
	if len(chunkRecords) > 0 || !emitted {
		if err := p.emitChunk(processor, h, chunkRecords, columns, nil); err != nil {
			return nil, err
		}
	}
	return columns, nil
}

// truncateLineForError truncates a string to maxLen characters for error messages.
func truncateLineForError(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
