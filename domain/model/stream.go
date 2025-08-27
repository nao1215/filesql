package model

import (
	"compress/bzip2"
	"compress/gzip"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"
)

// NewStreamingParser creates a new streaming parser
func NewStreamingParser(fileType FileType, tableName string, chunkSize int) *StreamingParser {
	return &StreamingParser{
		fileType:  fileType,
		tableName: tableName,
		chunkSize: chunkSize,
	}
}

// ParseFromReader parses data from io.Reader and returns a Table using streaming approach
func (p *StreamingParser) ParseFromReader(reader io.Reader) (*Table, error) {
	var decompressedReader io.Reader
	var closeFunc func() error
	var err error

	// Handle compression
	decompressedReader, closeFunc, err = p.createDecompressedReader(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to create decompressed reader: %w", err)
	}
	if closeFunc != nil {
		defer func() {
			if closeErr := closeFunc(); closeErr != nil {
				// TODO: Add proper logging for close errors
				_ = closeErr
			}
		}()
	}

	// Parse based on base file type
	baseType := p.fileType.BaseType()
	switch baseType {
	case FileTypeCSV:
		return p.parseCSVStream(decompressedReader)
	case FileTypeTSV:
		return p.parseTSVStream(decompressedReader)
	case FileTypeLTSV:
		return p.parseLTSVStream(decompressedReader)
	default:
		return nil, errors.New("unsupported file type")
	}
}

// createDecompressedReader creates appropriate reader based on compression type
func (p *StreamingParser) createDecompressedReader(reader io.Reader) (io.Reader, func() error, error) {
	switch p.fileType {
	case FileTypeCSVGZ, FileTypeTSVGZ, FileTypeLTSVGZ:
		gzReader, err := gzip.NewReader(reader)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		return gzReader, gzReader.Close, nil

	case FileTypeCSVBZ2, FileTypeTSVBZ2, FileTypeLTSVBZ2:
		bz2Reader := bzip2.NewReader(reader)
		return bz2Reader, nil, nil

	case FileTypeCSVXZ, FileTypeTSVXZ, FileTypeLTSVXZ:
		xzReader, err := xz.NewReader(reader)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create xz reader: %w", err)
		}
		return xzReader, nil, nil

	case FileTypeCSVZSTD, FileTypeTSVZSTD, FileTypeLTSVZSTD:
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

// parseCSVStream parses CSV data from reader using streaming approach
func (p *StreamingParser) parseCSVStream(reader io.Reader) (*Table, error) {
	csvReader := csv.NewReader(reader)
	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV: %w", err)
	}

	if len(records) == 0 {
		return nil, errors.New("empty CSV data")
	}

	header := NewHeader(records[0])
	// Check for duplicate column names
	columnsSeen := make(map[string]bool)
	for _, col := range records[0] {
		if columnsSeen[col] {
			return nil, fmt.Errorf("%w: %s", ErrDuplicateColumnName, col)
		}
		columnsSeen[col] = true
	}

	tableRecords := make([]Record, 0, len(records)-1)
	for i := 1; i < len(records); i++ {
		tableRecords = append(tableRecords, NewRecord(records[i]))
	}

	return NewTable(p.tableName, header, tableRecords), nil
}

// parseTSVStream parses TSV data from reader using streaming approach
func (p *StreamingParser) parseTSVStream(reader io.Reader) (*Table, error) {
	csvReader := csv.NewReader(reader)
	csvReader.Comma = '\t'
	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read TSV: %w", err)
	}

	if len(records) == 0 {
		return nil, errors.New("empty TSV data")
	}

	header := NewHeader(records[0])
	// Check for duplicate column names
	columnsSeen := make(map[string]bool)
	for _, col := range records[0] {
		if columnsSeen[col] {
			return nil, fmt.Errorf("%w: %s", ErrDuplicateColumnName, col)
		}
		columnsSeen[col] = true
	}

	tableRecords := make([]Record, 0, len(records)-1)
	for i := 1; i < len(records); i++ {
		tableRecords = append(tableRecords, NewRecord(records[i]))
	}

	return NewTable(p.tableName, header, tableRecords), nil
}

// parseLTSVStream parses LTSV data from reader using streaming approach
func (p *StreamingParser) parseLTSVStream(reader io.Reader) (*Table, error) {
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

		record := make(map[string]string)
		pairs := strings.Split(line, "\t")
		for _, pair := range pairs {
			kv := strings.SplitN(pair, ":", 2)
			if len(kv) == 2 {
				key := strings.TrimSpace(kv[0])
				value := strings.TrimSpace(kv[1])
				record[key] = value
				headerMap[key] = true
			}
		}
		if len(record) > 0 {
			records = append(records, record)
		}
	}

	if len(records) == 0 {
		return nil, errors.New("no valid LTSV records found")
	}

	var header Header
	for key := range headerMap {
		header = append(header, key)
	}

	tableRecords := make([]Record, 0, len(records))
	for _, record := range records {
		var row Record
		for _, key := range header {
			if val, exists := record[key]; exists {
				row = append(row, val)
			} else {
				row = append(row, "")
			}
		}
		tableRecords = append(tableRecords, row)
	}

	return NewTable(p.tableName, header, tableRecords), nil
}

// ProcessInChunks processes data from io.Reader in chunks and calls processor for each chunk
// This provides true streaming with memory-efficient chunk-based processing
func (p *StreamingParser) ProcessInChunks(reader io.Reader, processor ChunkProcessor) error {
	var decompressedReader io.Reader
	var closeFunc func() error
	var err error

	// Handle compression
	decompressedReader, closeFunc, err = p.createDecompressedReader(reader)
	if err != nil {
		return fmt.Errorf("failed to create decompressed reader: %w", err)
	}
	if closeFunc != nil {
		defer func() {
			if closeErr := closeFunc(); closeErr != nil {
				// TODO: Add proper logging for close errors
				_ = closeErr
			}
		}()
	}

	// Parse based on base file type
	baseType := p.fileType.BaseType()
	switch baseType {
	case FileTypeCSV:
		return p.processCSVInChunks(decompressedReader, processor)
	case FileTypeTSV:
		return p.processTSVInChunks(decompressedReader, processor)
	case FileTypeLTSV:
		return p.processLTSVInChunks(decompressedReader, processor)
	default:
		return errors.New("unsupported file type for chunked processing")
	}
}

// processCSVInChunks processes CSV data in chunks
func (p *StreamingParser) processCSVInChunks(reader io.Reader, processor ChunkProcessor) error {
	csvReader := csv.NewReader(reader)

	// Read header first
	headerRecord, err := csvReader.Read()
	if err != nil {
		if err == io.EOF {
			return errors.New("empty CSV data")
		}
		return fmt.Errorf("failed to read CSV header: %w", err)
	}

	// Validate header for duplicates
	columnsSeen := make(map[string]bool)
	for _, col := range headerRecord {
		if columnsSeen[col] {
			return fmt.Errorf("%w: %s", ErrDuplicateColumnName, col)
		}
		columnsSeen[col] = true
	}

	header := NewHeader(headerRecord)
	var columnInfo []ColumnInfo
	var columnValues [][]string

	// Read records in chunks
	var chunkRecords []Record
	chunkSize := p.chunkSize
	if chunkSize <= 0 {
		chunkSize = 1000 // Default chunk size
	}

	for {
		record, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read CSV record: %w", err)
		}

		chunkRecords = append(chunkRecords, NewRecord(record))

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
		if len(chunkRecords) >= chunkSize {
			// Infer column types on first chunk
			if len(columnInfo) == 0 {
				columnInfo = p.inferColumnInfoFromValues(header, columnValues)
			}

			chunk := &TableChunk{
				tableName:  p.tableName,
				headers:    header,
				records:    chunkRecords,
				columnInfo: columnInfo,
			}

			if err := processor(chunk); err != nil {
				return fmt.Errorf("chunk processor error: %w", err)
			}

			// Reset for next chunk
			chunkRecords = nil
			columnValues = nil // Don't collect values after first chunk
		}
	}

	// Process remaining records
	if len(chunkRecords) > 0 {
		// Infer column types if we haven't yet (small dataset)
		if len(columnInfo) == 0 {
			columnInfo = p.inferColumnInfoFromValues(header, columnValues)
		}

		chunk := &TableChunk{
			tableName:  p.tableName,
			headers:    header,
			records:    chunkRecords,
			columnInfo: columnInfo,
		}

		if err := processor(chunk); err != nil {
			return fmt.Errorf("chunk processor error: %w", err)
		}
	}

	return nil
}

// processTSVInChunks processes TSV data in chunks
func (p *StreamingParser) processTSVInChunks(reader io.Reader, processor ChunkProcessor) error {
	csvReader := csv.NewReader(reader)
	csvReader.Comma = '\t'

	// Read header first
	headerRecord, err := csvReader.Read()
	if err != nil {
		if err == io.EOF {
			return errors.New("empty TSV data")
		}
		return fmt.Errorf("failed to read TSV header: %w", err)
	}

	// Validate header for duplicates
	columnsSeen := make(map[string]bool)
	for _, col := range headerRecord {
		if columnsSeen[col] {
			return fmt.Errorf("%w: %s", ErrDuplicateColumnName, col)
		}
		columnsSeen[col] = true
	}

	header := NewHeader(headerRecord)
	var columnInfo []ColumnInfo
	var columnValues [][]string

	// Read records in chunks
	var chunkRecords []Record
	chunkSize := p.chunkSize
	if chunkSize <= 0 {
		chunkSize = 1000
	}

	for {
		record, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read TSV record: %w", err)
		}

		chunkRecords = append(chunkRecords, NewRecord(record))

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
		if len(chunkRecords) >= chunkSize {
			// Infer column types on first chunk
			if len(columnInfo) == 0 {
				columnInfo = p.inferColumnInfoFromValues(header, columnValues)
			}

			chunk := &TableChunk{
				tableName:  p.tableName,
				headers:    header,
				records:    chunkRecords,
				columnInfo: columnInfo,
			}

			if err := processor(chunk); err != nil {
				return fmt.Errorf("chunk processor error: %w", err)
			}

			// Reset for next chunk
			chunkRecords = nil
			columnValues = nil
		}
	}

	// Process remaining records
	if len(chunkRecords) > 0 {
		// Infer column types if we haven't yet
		if len(columnInfo) == 0 {
			columnInfo = p.inferColumnInfoFromValues(header, columnValues)
		}

		chunk := &TableChunk{
			tableName:  p.tableName,
			headers:    header,
			records:    chunkRecords,
			columnInfo: columnInfo,
		}

		if err := processor(chunk); err != nil {
			return fmt.Errorf("chunk processor error: %w", err)
		}
	}

	return nil
}

// processLTSVInChunks processes LTSV data in chunks
func (p *StreamingParser) processLTSVInChunks(reader io.Reader, processor ChunkProcessor) error {
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

		pairs := strings.Split(line, "\t")
		for _, pair := range pairs {
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

	var header Header
	for key := range headerMap {
		header = append(header, key)
	}

	// Second pass: process records in chunks
	chunkRecords := make([]Record, 0) // Pre-allocate slice
	var columnValues [][]string
	var columnInfo []ColumnInfo

	chunkSize := p.chunkSize
	if chunkSize <= 0 {
		chunkSize = 1000
	}

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		record := make(map[string]string)
		pairs := strings.Split(line, "\t")
		for _, pair := range pairs {
			kv := strings.SplitN(pair, ":", 2)
			if len(kv) == 2 {
				key := strings.TrimSpace(kv[0])
				value := strings.TrimSpace(kv[1])
				record[key] = value
			}
		}

		if len(record) == 0 {
			continue
		}

		var row Record
		for _, key := range header {
			if val, exists := record[key]; exists {
				row = append(row, val)
			} else {
				row = append(row, "")
			}
		}
		chunkRecords = append(chunkRecords, row)

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
		if len(chunkRecords) >= chunkSize {
			// Infer column types on first chunk
			if len(columnInfo) == 0 {
				columnInfo = p.inferColumnInfoFromValues(header, columnValues)
			}

			chunk := &TableChunk{
				tableName:  p.tableName,
				headers:    header,
				records:    chunkRecords,
				columnInfo: columnInfo,
			}

			if err := processor(chunk); err != nil {
				return fmt.Errorf("chunk processor error: %w", err)
			}

			// Reset for next chunk
			chunkRecords = nil
			columnValues = nil
		}
	}

	// Process remaining records
	if len(chunkRecords) > 0 {
		// Infer column types if we haven't yet
		if len(columnInfo) == 0 {
			columnInfo = p.inferColumnInfoFromValues(header, columnValues)
		}

		chunk := &TableChunk{
			tableName:  p.tableName,
			headers:    header,
			records:    chunkRecords,
			columnInfo: columnInfo,
		}

		if err := processor(chunk); err != nil {
			return fmt.Errorf("chunk processor error: %w", err)
		}
	}

	return nil
}

// inferColumnInfoFromValues creates column info from collected values
func (p *StreamingParser) inferColumnInfoFromValues(header Header, columnValues [][]string) []ColumnInfo {
	if len(columnValues) == 0 {
		// No data to infer from, use default TEXT type
		columnInfo := make([]ColumnInfo, len(header))
		for i, name := range header {
			columnInfo[i] = ColumnInfo{
				Name: name,
				Type: ColumnTypeText,
			}
		}
		return columnInfo
	}

	columnInfo := make([]ColumnInfo, len(header))
	for i, name := range header {
		var values []string
		if i < len(columnValues) {
			values = columnValues[i]
		}
		columnInfo[i] = ColumnInfo{
			Name: name,
			Type: InferColumnType(values),
		}
	}
	return columnInfo
}
