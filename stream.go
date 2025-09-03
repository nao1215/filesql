package filesql

import (
	"compress/bzip2"
	"compress/gzip"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"

	"github.com/apache/arrow/go/v18/arrow/array"
	pqfile "github.com/apache/arrow/go/v18/parquet/file"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"
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

// newStreamingParser creates a new streaming parser
func newStreamingParser(fileType FileType, tableName string, chunkSize int) *streamingParser {
	return &streamingParser{
		fileType:    fileType,
		tableName:   tableName,
		chunkSize:   NewChunkSize(chunkSize),
		memoryPool:  NewMemoryPool(1024 * 1024), // 1MB default max buffer size
		memoryLimit: NewMemoryLimit(512),        // 512MB default memory limit
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
		return nil, fmt.Errorf("failed to create decompressed reader: %w", err)
	}
	if closeFunc != nil {
		defer handleCloseError(closeFunc)
	}

	// Parse based on base file type
	baseType := p.fileType.baseType()
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
	default:
		return nil, errors.New("unsupported file type")
	}
}

// createDecompressedReader creates appropriate reader based on compression type
func (p *streamingParser) createDecompressedReader(reader io.Reader) (io.Reader, func() error, error) {
	switch p.fileType {
	case FileTypeCSVGZ, FileTypeTSVGZ, FileTypeLTSVGZ, FileTypeXLSXGZ:
		gzReader, err := gzip.NewReader(reader)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		return gzReader, gzReader.Close, nil

	case FileTypeCSVBZ2, FileTypeTSVBZ2, FileTypeLTSVBZ2, FileTypeXLSXBZ2:
		bz2Reader := bzip2.NewReader(reader)
		return bz2Reader, nil, nil

	case FileTypeCSVXZ, FileTypeTSVXZ, FileTypeLTSVXZ, FileTypeXLSXXZ:
		xzReader, err := xz.NewReader(reader)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create xz reader: %w", err)
		}
		return xzReader, nil, nil

	case FileTypeCSVZSTD, FileTypeTSVZSTD, FileTypeLTSVZSTD, FileTypeXLSXZSTD:
		decoder, err := zstd.NewReader(reader)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create zstd reader: %w", err)
		}
		return decoder, func() error { decoder.Close(); return nil }, nil

	default:
		// No compression
		return reader, nil, nil
	}
}

// parseDelimitedStream parses CSV or TSV data from reader using streaming approach
func (p *streamingParser) parseDelimitedStream(reader io.Reader, delimiter rune, fileTypeName string) (*table, error) {
	csvReader := csv.NewReader(reader)
	csvReader.Comma = delimiter
	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", fileTypeName, err)
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("empty %s data", fileTypeName)
	}

	header := newHeader(records[0])
	// Check for duplicate column names
	if err := validateColumnNames(records[0]); err != nil {
		return nil, err
	}

	tablerecords := make([]Record, 0, len(records)-1)
	for i := 1; i < len(records); i++ {
		tablerecords = append(tablerecords, newRecord(records[i]))
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
func (p *streamingParser) parseLTSVStream(reader io.Reader) (*table, error) {
	content, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read LTSV: %w", err)
	}

	lines := strings.Split(string(content), "\n")
	if len(lines) == 0 {
		return nil, errors.New("empty LTSV data")
	}

	headerMap := make(map[string]bool)
	var records []map[string]string

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		recordMap := make(map[string]string)
		for pair := range strings.SplitSeq(line, "\t") {
			kv := strings.SplitN(pair, ":", 2)
			if len(kv) == 2 {
				key := strings.TrimSpace(kv[0])
				value := strings.TrimSpace(kv[1])
				recordMap[key] = value
				headerMap[key] = true
			}
		}
		if len(recordMap) > 0 {
			records = append(records, recordMap)
		}
	}

	if len(records) == 0 {
		return nil, errors.New("no valid LTSV records found")
	}

	var header header
	for key := range headerMap {
		header = append(header, key)
	}

	tablerecords := make([]Record, 0, len(records))
	for _, recordMap := range records {
		var row Record
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
		return fmt.Errorf("failed to create decompressed reader: %w", err)
	}
	if closeFunc != nil {
		defer handleCloseError(closeFunc)
	}

	// Parse based on base file type
	baseType := p.fileType.baseType()
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
	default:
		return errors.New("unsupported file type for chunked processing")
	}
}

// processDelimitedInChunks processes CSV or TSV data in chunks based on delimiter
func (p *streamingParser) processDelimitedInChunks(reader io.Reader, processor chunkProcessor, delimiter rune, fileTypeName string) error {
	csvReader := csv.NewReader(reader)
	if delimiter != csvDelimiter {
		csvReader.Comma = delimiter
	}

	// Read header first
	headerrecord, err := csvReader.Read()
	if err != nil {
		if err == io.EOF {
			return fmt.Errorf("empty %s data", fileTypeName)
		}
		return fmt.Errorf("failed to read %s header: %w", fileTypeName, err)
	}

	// Validate header for duplicates
	if err := validateColumnNames(headerrecord); err != nil {
		return err
	}

	header := newHeader(headerrecord)
	var columnInfo columnInfoList
	var columnValues [][]string

	// Read records in chunks
	var chunkrecords []Record
	chunkSize := p.chunkSize.Int()
	if chunkSize <= 0 {
		chunkSize = DefaultRowsPerChunk
	}

	for {
		record, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read %s record: %w", fileTypeName, err)
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
			// Infer column types on first chunk
			if len(columnInfo) == 0 {
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
		if len(columnInfo) == 0 {
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
	content, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read LTSV: %w", err)
	}

	lines := strings.Split(string(content), "\n")
	if len(lines) == 0 {
		return errors.New("empty LTSV data")
	}

	headerMap := make(map[string]bool)

	// First pass: collect all possible keys
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		for pair := range strings.SplitSeq(line, "\t") {
			kv := strings.SplitN(pair, ":", 2)
			if len(kv) == 2 {
				key := strings.TrimSpace(kv[0])
				headerMap[key] = true
			}
		}
	}

	if len(headerMap) == 0 {
		return errors.New("no valid LTSV keys found")
	}

	var header header
	for key := range headerMap {
		header = append(header, key)
	}

	// Second pass: process records in chunks
	chunkrecords := make([]Record, 0) // Pre-allocate slice
	var columnValues [][]string
	var columnInfo columnInfoList

	chunkSize := p.chunkSize.Int()
	if chunkSize <= 0 {
		chunkSize = DefaultRowsPerChunk
	}

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		recordMap := make(map[string]string)
		for pair := range strings.SplitSeq(line, "\t") {
			kv := strings.SplitN(pair, ":", 2)
			if len(kv) == 2 {
				key := strings.TrimSpace(kv[0])
				value := strings.TrimSpace(kv[1])
				recordMap[key] = value
			}
		}

		if len(recordMap) == 0 {
			continue
		}

		var row Record
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
			// Infer column types on first chunk
			if len(columnInfo) == 0 {
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
		if len(columnInfo) == 0 {
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
		return nil, errors.New("empty parquet file")
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
		return nil, errors.New("no records found in parquet stream")
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

	var allRecords []Record
	for tableReader.Next() {
		batch := tableReader.Record()

		// Convert each row in the batch
		numRows := batch.NumRows()
		for i := range numRows {
			row := make(Record, batch.NumCols())
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

	return newTable(p.tableName, headerSlice, allRecords), nil
}

// processParquetInChunks processes Parquet data in chunks
func (p *streamingParser) processParquetInChunks(reader io.Reader, processor chunkProcessor) error {
	// Read all data into memory (Parquet requires random access)
	data, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read parquet data: %w", err)
	}

	if len(data) == 0 {
		return errors.New("empty parquet file")
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

	if table.NumRows() == 0 {
		return errors.New("no records found in parquet stream")
	}

	// Initialize header from table schema
	schema := table.Schema()
	headerSlice := make(header, schema.NumFields())
	for i, field := range schema.Fields() {
		headerSlice[i] = field.Name
	}

	// Infer column types from first batch
	columnInfoList := make(columnInfoList, len(headerSlice))
	for i, name := range headerSlice {
		// For Parquet files, we'll default to TEXT for simplicity in streaming
		// Real type inference could be done from Arrow schema
		columnInfoList[i] = newColumnInfoWithType(name, columnTypeText)
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

		var chunkRecords []Record
		numRows := batch.NumRows()
		for i := range numRows {
			row := make(Record, batch.NumCols())
			for j, col := range batch.Columns() {
				value := extractValueFromArrowArray(col, i)
				row[j] = value
			}
			chunkRecords = append(chunkRecords, row)
		}

		if len(chunkRecords) > 0 {
			chunk := &tableChunk{
				tableName:  p.tableName,
				headers:    headerSlice,
				records:    chunkRecords,
				columnInfo: columnInfoList,
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
	if p.memoryLimit != nil && p.memoryLimit.CheckMemoryUsage() == MemoryStatusExceeded {
		return nil, p.memoryLimit.CreateMemoryError("XLSX parsing")
	}

	// Open XLSX directly from the reader (excelize will buffer as needed)
	xlsxFile, err := excelize.OpenReader(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to open XLSX file: %w", err)
	}
	defer func() {
		_ = xlsxFile.Close() // Ignore close error
	}()

	// Get all sheet names
	sheetNames := xlsxFile.GetSheetList()
	if len(sheetNames) == 0 {
		return nil, errors.New("no sheets found in XLSX file")
	}

	// With the streaming parser, we only process the first sheet
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
	records := p.memoryPool.GetRecordSlice()
	originalRecords := records // Track original slice for proper pool return
	defer func() {
		// Always return the original slice to the pool, even if records grew
		p.memoryPool.PutRecordSlice(originalRecords)
	}()

	for iter.Next() {
		// Check memory usage periodically (every 1000 records to reduce ReadMemStats overhead)
		// runtime.ReadMemStats can pause for milliseconds, so we check less frequently
		if p.memoryLimit != nil && len(records)%1000 == 0 {
			if status := p.memoryLimit.CheckMemoryUsage(); status == MemoryStatusExceeded {
				return nil, p.memoryLimit.CreateMemoryError("XLSX row processing")
			} else if status == MemoryStatusWarning {
				// Force GC at warning threshold
				p.memoryPool.ForceGC()
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
	if p.memoryLimit != nil && p.memoryLimit.CheckMemoryUsage() == MemoryStatusExceeded {
		return p.memoryLimit.CreateMemoryError("XLSX chunk processing")
	}

	// Open XLSX file from reader
	xlsxFile, err := excelize.OpenReader(reader)
	if err != nil {
		return fmt.Errorf("failed to open XLSX file: %w", err)
	}
	defer func() {
		_ = xlsxFile.Close() // Ignore close error
	}()

	// Get all sheet names
	sheetNames := xlsxFile.GetSheetList()
	if len(sheetNames) == 0 {
		return errors.New("no sheets found in XLSX file")
	}

	// Process only the first sheet (streaming parser limitation)
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
		chunkRecords  []Record
		processedRows int
	)

	// Get base chunk size and adjust for memory limits
	chunkSize := p.chunkSize.Int()
	if chunkSize <= 0 {
		chunkSize = DefaultRowsPerChunk
	}

	// Adjust chunk size based on memory usage
	if p.memoryLimit != nil {
		if shouldReduce, newSize := p.memoryLimit.ShouldReduceChunkSize(chunkSize); shouldReduce {
			chunkSize = newSize
			if chunkSize < 1 {
				chunkSize = 1
			}
		}
	}

	// Use memory pool for chunk records
	chunkRecords = p.memoryPool.GetRecordSlice()
	originalChunkRecords := chunkRecords // Track original slice for proper pool return
	defer func() {
		// Always return the original slice to the pool, even if chunkRecords grew
		p.memoryPool.PutRecordSlice(originalChunkRecords)
	}()

	for iter.Next() {
		// Check memory usage periodically (every 1000 rows to reduce ReadMemStats overhead)
		// runtime.ReadMemStats can pause for milliseconds, so we check less frequently
		if p.memoryLimit != nil && processedRows%1000 == 0 {
			if status := p.memoryLimit.CheckMemoryUsage(); status == MemoryStatusExceeded {
				return p.memoryLimit.CreateMemoryError("XLSX row processing")
			} else if status == MemoryStatusWarning {
				// Force GC and reduce chunk size on memory pressure
				p.memoryPool.ForceGC()
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
			// Infer column types on first chunk
			if len(columnInfo) == 0 {
				columnInfo = newColumnInfoListFromValues(headers, columnValues)
			}

			// Copy to decouple from the reused backing array
			chunkData := append([]Record(nil), chunkRecords...)
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
		if len(columnInfo) == 0 {
			columnInfo = newColumnInfoListFromValues(headers, columnValues)
		}

		// Copy to decouple from the reused backing array
		chunkData := append([]Record(nil), chunkRecords...)
		chunk := &tableChunk{
			tableName:  p.tableName,
			headers:    headers,
			records:    chunkData,
			columnInfo: columnInfo,
		}

		if err := processor(chunk); err != nil {
			return fmt.Errorf("chunk processor error: %w", err)
		}
	}

	return nil
}
