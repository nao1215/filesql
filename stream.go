package filesql

import (
	"bufio"
	"compress/bzip2"
	"compress/gzip"
	"compress/zlib"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"

	"github.com/apache/arrow/go/v18/arrow/array"
	pqfile "github.com/apache/arrow/go/v18/parquet/file"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
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
		return nil, fmt.Errorf("%w: failed to create decompressed reader: %s", ErrCompression, err.Error())
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
	case FileTypeJSON:
		return p.parseJSONStream(decompressedReader)
	case FileTypeJSONL:
		return p.parseJSONLStream(decompressedReader)
	default:
		return nil, ErrUnsupportedFormat
	}
}

// createDecompressedReader creates appropriate reader based on compression type
func (p *streamingParser) createDecompressedReader(reader io.Reader) (io.Reader, func() error, error) {
	switch p.fileType {
	case FileTypeCSVGZ, FileTypeTSVGZ, FileTypeLTSVGZ, FileTypeXLSXGZ, FileTypeParquetGZ,
		FileTypeJSONGZ, FileTypeJSONLGZ:
		gzReader, err := gzip.NewReader(reader)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: failed to create gzip reader: %s", ErrCompression, err.Error())
		}
		return gzReader, gzReader.Close, nil

	case FileTypeCSVBZ2, FileTypeTSVBZ2, FileTypeLTSVBZ2, FileTypeXLSXBZ2, FileTypeParquetBZ2,
		FileTypeJSONBZ2, FileTypeJSONLBZ2:
		bz2Reader := bzip2.NewReader(reader)
		return bz2Reader, nil, nil

	case FileTypeCSVXZ, FileTypeTSVXZ, FileTypeLTSVXZ, FileTypeXLSXXZ, FileTypeParquetXZ,
		FileTypeJSONXZ, FileTypeJSONLXZ:
		xzReader, err := xz.NewReader(reader)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: failed to create xz reader: %s", ErrCompression, err.Error())
		}
		return xzReader, nil, nil

	case FileTypeCSVZSTD, FileTypeTSVZSTD, FileTypeLTSVZSTD, FileTypeXLSXZSTD, FileTypeParquetZSTD,
		FileTypeJSONZSTD, FileTypeJSONLZSTD:
		decoder, err := zstd.NewReader(reader)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: failed to create zstd reader: %s", ErrCompression, err.Error())
		}
		return decoder, func() error { decoder.Close(); return nil }, nil

	case FileTypeCSVZLIB, FileTypeTSVZLIB, FileTypeLTSVZLIB, FileTypeXLSXZLIB, FileTypeParquetZLIB,
		FileTypeJSONZLIB, FileTypeJSONLZLIB:
		zlibReader, err := zlib.NewReader(reader)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: failed to create zlib reader: %s", ErrCompression, err.Error())
		}
		return zlibReader, zlibReader.Close, nil

	case FileTypeCSVSNAPPY, FileTypeTSVSNAPPY, FileTypeLTSVSNAPPY, FileTypeXLSXSNAPPY, FileTypeParquetSNAPPY,
		FileTypeJSONSNAPPY, FileTypeJSONLSNAPPY:
		snappyReader := snappy.NewReader(reader)
		return snappyReader, nil, nil

	case FileTypeCSVS2, FileTypeTSVS2, FileTypeLTSVS2, FileTypeXLSXS2, FileTypeParquetS2,
		FileTypeJSONS2, FileTypeJSONLS2:
		s2Reader := s2.NewReader(reader)
		return s2Reader, nil, nil

	case FileTypeCSVLZ4, FileTypeTSVLZ4, FileTypeLTSVLZ4, FileTypeXLSXLZ4, FileTypeParquetLZ4,
		FileTypeJSONLZ4, FileTypeJSONLLZ4:
		lz4Reader := lz4.NewReader(reader)
		return lz4Reader, nil, nil

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
		return nil, fmt.Errorf("%w: failed to read %s: %s", ErrParsing, fileTypeName, err.Error())
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("%w: empty %s data", ErrEmptyData, fileTypeName)
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
		return nil, fmt.Errorf("%w: failed to read LTSV: %s", ErrParsing, err.Error())
	}

	lines := strings.Split(string(content), "\n")
	if len(lines) == 0 {
		return nil, fmt.Errorf("%w: empty LTSV data", ErrEmptyData)
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
				// A label repeated within the same record cannot be two distinct
				// columns; keeping the last value would silently drop the earlier
				// one, so reject it. Ref nao1215/sqly#467.
				if _, dup := recordMap[key]; dup {
					return nil, fmt.Errorf("%w: duplicate column name %q in LTSV record", ErrParsing, key)
				}
				recordMap[key] = value
				headerMap[key] = true
			}
		}
		if len(recordMap) > 0 {
			records = append(records, recordMap)
		}
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("%w: no valid LTSV records found", ErrEmptyData)
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
		return fmt.Errorf("%w: failed to create decompressed reader: %s", ErrCompression, err.Error())
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
	csvReader := csv.NewReader(reader)
	if delimiter != csvDelimiter {
		csvReader.Comma = delimiter
	}

	// Read header first
	headerrecord, err := csvReader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("%w: empty %s data", ErrEmptyData, fileTypeName)
		}
		return fmt.Errorf("%w: failed to read %s header: %s", ErrParsing, fileTypeName, err.Error())
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
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("%w: failed to read %s record: %s", ErrParsing, fileTypeName, err.Error())
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
	content, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("%w: failed to read LTSV: %s", ErrParsing, err.Error())
	}

	lines := strings.Split(string(content), "\n")
	if len(lines) == 0 {
		return fmt.Errorf("%w: empty LTSV data", ErrEmptyData)
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
		return fmt.Errorf("%w: no valid LTSV keys found", ErrEmptyData)
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

	// Infer column types from first batch
	columnInfoList := make(columnInfoList, len(headerSlice))
	for i, name := range headerSlice {
		// For Parquet files, we'll default to TEXT for simplicity in streaming
		// Real type inference could be done from Arrow schema
		columnInfoList[i] = newColumnInfoWithType(name)
	}

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
		return nil, fmt.Errorf("%w: no sheets found in XLSX file", ErrEmptyData)
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
		return fmt.Errorf("%w: no sheets found in XLSX file", ErrEmptyData)
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
		return nil, fmt.Errorf("%w: failed to read JSON: %s", ErrParsing, err.Error())
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
		records := make([]Record, 0, len(arr))
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
		return nil, fmt.Errorf("%w: failed to parse JSON: %s", ErrInvalidData, err.Error())
	}

	t := newTable(p.tableName, h, []Record{newRecord([]string{string(obj)})})
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
	var records []Record
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
			return nil, fmt.Errorf("%w: failed to read JSONL: %s", ErrParsing, err.Error())
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
	isArray, err := peekJSONIsArray(br)
	if err != nil {
		return err
	}

	h := newHeader([]string{jsonDataHeader})
	colInfo := []columnInfo{newColumnInfoWithType(jsonDataHeader)}

	if isArray {
		// Stream array elements one by one via json.Decoder.
		dec := json.NewDecoder(br)
		// Consume the opening '['
		if _, err := dec.Token(); err != nil {
			return fmt.Errorf("%w: failed to parse JSON array: %s", ErrInvalidData, err.Error())
		}
		return p.processJSONArrayChunks(dec, h, colInfo, processor)
	}

	// Non-array: read the whole value and process as a single-row chunk.
	content, err := io.ReadAll(br)
	if err != nil {
		return fmt.Errorf("%w: failed to read JSON: %s", ErrParsing, err.Error())
	}

	trimmed := strings.TrimSpace(string(content))
	if trimmed == "" {
		return fmt.Errorf("%w: empty JSON data", ErrEmptyData)
	}

	var obj json.RawMessage
	if unmarshalErr := json.Unmarshal([]byte(trimmed), &obj); unmarshalErr != nil {
		return fmt.Errorf("%w: failed to parse JSON: %s", ErrInvalidData, unmarshalErr.Error())
	}

	chunk := &tableChunk{
		tableName:  p.tableName,
		headers:    h,
		records:    []Record{newRecord([]string{string(obj)})},
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
			return false, fmt.Errorf("%w: failed to read JSON: %s", ErrParsing, err.Error())
		}
		// Skip whitespace
		if b == ' ' || b == '\t' || b == '\n' || b == '\r' {
			continue
		}
		// Put the byte back so the decoder / ReadAll can consume it
		if unreadErr := br.UnreadByte(); unreadErr != nil {
			return false, fmt.Errorf("%w: failed to read JSON: %s", ErrParsing, unreadErr.Error())
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

	var chunkRecords []Record
	totalRecords := 0

	for dec.More() {
		var elem json.RawMessage
		if err := dec.Decode(&elem); err != nil {
			return fmt.Errorf("%w: failed to decode JSON array element: %s", ErrParsing, err.Error())
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
		return fmt.Errorf("%w: failed to read JSON array end: %s", ErrParsing, err.Error())
	}

	// Reject trailing data after the array (e.g. "[1] garbage")
	if dec.More() {
		return fmt.Errorf("%w: unexpected data after JSON array", ErrInvalidData)
	}

	if totalRecords == 0 {
		return fmt.Errorf("%w: empty JSON array", ErrEmptyData)
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

	var chunkRecords []Record
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
			return fmt.Errorf("%w: failed to read JSONL: %s", ErrParsing, err.Error())
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
