package prep

import (
	"bytes"
	"io"
)

// stream holds preprocessed data and serves it as a rewindable reader.
type stream struct {
	reader *bytes.Reader
}

// newStream creates a new stream over the preprocessed data.
func newStream(data []byte) *stream {
	return &stream{reader: bytes.NewReader(data)}
}

// Read implements io.Reader.
func (s *stream) Read(p []byte) (n int, err error) {
	return s.reader.Read(p)
}

// Seek implements io.Seeker, so a caller can rewind the preprocessed data.
func (s *stream) Seek(offset int64, whence int) (int64, error) {
	return s.reader.Seek(offset, whence)
}

var (
	_ io.Reader = (*stream)(nil)
	_ io.Seeker = (*stream)(nil)
)
