package prep

import (
	"bytes"
	"io"

	"github.com/nao1215/filesql/parser"
)

// Stream represents a preprocessed data stream with format information.
// It implements io.Reader and provides metadata about the file format.
type Stream interface {
	io.Reader
	// Format returns the actual output format of the stream data.
	// For CSV/TSV/LTSV input, this matches the input format.
	// For JSON/JSONL input, this returns JSONL since the output is JSONL-formatted.
	// For XLSX/Parquet input, this returns CSV since the output is CSV-formatted.
	Format() parser.FileType
	// OriginalFormat returns the original input file type including compression
	OriginalFormat() parser.FileType
}

// stream implements the Stream interface
type stream struct {
	reader         *bytes.Reader
	format         parser.FileType
	originalFormat parser.FileType
}

// newStream creates a new Stream from data and format information.
// outputFormat is the actual format of the data in the stream.
// originalFormat is the format of the input file.
func newStream(data []byte, outputFormat parser.FileType, originalFormat parser.FileType) *stream {
	return &stream{
		reader:         bytes.NewReader(data),
		format:         outputFormat,
		originalFormat: originalFormat,
	}
}

// Read implements io.Reader
func (s *stream) Read(p []byte) (n int, err error) {
	return s.reader.Read(p)
}

// Format returns the actual output format of the stream data.
// For CSV/TSV/LTSV input, this matches the input format.
// For JSON/JSONL input, this returns JSONL since the output is JSONL-formatted.
// For XLSX/Parquet input, this returns CSV since the output is CSV-formatted.
func (s *stream) Format() parser.FileType {
	return s.format
}

// OriginalFormat returns the original file type including compression info
func (s *stream) OriginalFormat() parser.FileType {
	return s.originalFormat
}

// Seek implements io.Seeker for rewinding the stream
func (s *stream) Seek(offset int64, whence int) (int64, error) {
	return s.reader.Seek(offset, whence)
}

// Len returns the number of bytes of the unread portion of the stream
func (s *stream) Len() int {
	return s.reader.Len()
}
