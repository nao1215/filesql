package filesql

import (
	"errors"
	"fmt"
	"io"

	"github.com/nao1215/filesql/internal/codec"
	"github.com/nao1215/filesql/internal/reader"
	"github.com/nao1215/filesql/internal/textin"
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

// readerFormats maps a format onto the reader package's name for it. Only
// formats the reader handles appear: ACH and Fedwire are loaded by their own
// paths, which build their tables rather than reading rows.
var readerFormats = map[FileType]reader.Format{ //nolint:gochecknoglobals // constant-like lookup table
	FileTypeCSV:     reader.FormatCSV,
	FileTypeTSV:     reader.FormatTSV,
	FileTypeLTSV:    reader.FormatLTSV,
	FileTypeParquet: reader.FormatParquet,
	FileTypeXLSX:    reader.FormatXLSX,
	FileTypeJSON:    reader.FormatJSON,
	FileTypeJSONL:   reader.FormatJSONL,
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
	}
}

// createDecompressedReader wraps reader with the codec the source was declared
// to use. The per-format switch this replaced was a second implementation of
// CompressionType.NewReader, reached through the fused FileType.
//
// CompressionNone returns the reader unchanged with a no-op close function,
// following the convention that the close function is never nil.
func (p *streamingParser) createDecompressedReader(source io.Reader) (io.Reader, func() error, error) {
	return newDecompressor(p.compression, source)
}

// ProcessInChunks reads the input in chunks, handing each to processor, and
// returns the columns with the type every row read requires. The types come
// last because they are not known until the last row has been read: a column is
// declared once, after the whole input has been seen, so where a chunk boundary
// falls cannot change what a table holds.
func (p *streamingParser) ProcessInChunks(source io.Reader, processor chunkProcessor) (columnInfoList, error) {
	format, supported := readerFormats[p.fileType]
	if !supported {
		return nil, fmt.Errorf("%w: unsupported file type for chunked processing", ErrUnsupportedFormat)
	}

	decompressed, closeFunc, err := p.createDecompressedReader(source)
	if err != nil {
		// The handler has already said which sentinel this is; adding it again
		// named one failure twice.
		return nil, err
	}
	defer closeQuietly(closeFunc)

	// A byte-order mark belongs to the encoding rather than to the text, and a
	// UTF-16 file has to be transcoded before anything splits it into fields.
	// Parquet and XLSX carry their own container and are read as bytes.
	if isTextBaseType(p.fileType) {
		decompressed = textin.Decode(decompressed)
	}

	read, err := reader.Read(decompressed, format, p.readOptions(), func(chunk *reader.Chunk) error {
		return processor(p.tableChunk(chunk))
	})
	p.totalRows += read.Total
	p.skippedRows += read.Skipped
	if err != nil {
		return nil, wrapReadError(err)
	}
	return columnInfos(read.Header, read.Types), nil
}

// readOptions are the settings this parser reads with.
func (p *streamingParser) readOptions() reader.Options {
	return reader.Options{
		ChunkSize:        p.chunkSize.Int(),
		Reconcile:        p.reconcile(),
		Unlabeled:        p.unlabeled(),
		ExcelSheetPolicy: p.excelSheetPolicy.internal(),
		// A load spells its values for the column they are stored in, so SQLite's
		// affinity converts each one back to what the file holds.
		Rendering: reader.RenderSQLite,
	}
}

// reconcile applies the malformed-row policy to a delimited record whose field
// count differs from the header's.
func (p *streamingParser) reconcile() reader.Reconcile {
	policy := p.malformedRowPolicy
	return func(record []string, want, rowNum int) ([]string, bool, error) {
		return reconcileFieldCount(record, want, rowNum, policy)
	}
}

// unlabeled applies the malformed-row policy to an LTSV record holding a field
// that names no label.
func (p *streamingParser) unlabeled() reader.Unlabeled {
	policy := p.malformedRowPolicy
	return func(fields []string, rowNum int) (bool, error) {
		return reconcileUnlabeledFields(fields, rowNum, policy)
	}
}

// tableChunk is one chunk of rows in the vocabulary the loader inserts with.
func (p *streamingParser) tableChunk(chunk *reader.Chunk) *tableChunk {
	records := make([]record, len(chunk.Records))
	for i, r := range chunk.Records {
		records[i] = newRecord(r)
	}
	return &tableChunk{
		tableName: p.tableName,
		headers:   newHeader(chunk.Header),
		records:   records,
		types:     columnInfos(chunk.Header, chunk.Types),
		nulls:     chunk.Nulls,
	}
}

// wrapReadError gives a failed read the sentinel a caller matches it by. The
// reader names no sentinel of its own, so that every package reading through it
// reports a fault in its own vocabulary; this is filesql's.
func wrapReadError(err error) error {
	var readErr *reader.Error
	if err == nil || !errors.As(err, &readErr) {
		return err
	}
	// A cause that already carries one of this package's sentinels is already
	// framed. The text decoder sits in front of the reader and reports a byte
	// that is not UTF-8 with filesql's own error, so framing it again named the
	// package twice about one byte. ErrParsing still reaches a caller through
	// the ParseError the load wraps this in. Any other sentinel of this
	// package's that comes to be produced below the reader belongs here too.
	if errors.Is(err, ErrInvalidUTF8) {
		return err
	}
	// A stream that failed to decompress is a fault in the file's compression
	// rather than in its format, whichever of the two the decoder happened to
	// notice it in: three of the codecs read their header when the reader is
	// built and the rest fail on the first read, which lands inside the format
	// reader and used to be reported as a bad CSV header.
	if errors.Is(err, codec.ErrDecompress) {
		return fmt.Errorf("%w: %w", ErrCompression, err)
	}
	switch readErr.Kind {
	case reader.KindEmpty:
		return fmt.Errorf("%w: %w", ErrEmptyData, err)
	case reader.KindInvalidData:
		return fmt.Errorf("%w: %w", ErrInvalidData, err)
	case reader.KindDuplicateColumn:
		return fmt.Errorf("%w: %w", ErrDuplicateColumn, err)
	case reader.KindUnsupported:
		return fmt.Errorf("%w: %w", ErrUnsupportedFormat, err)
	default:
		return fmt.Errorf("%w: %w", ErrParsing, err)
	}
}
