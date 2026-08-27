package prep

import (
	"io"
	"testing"
)

func TestStream_Read(t *testing.T) {
	t.Parallel()

	data := []byte("hello, world")
	s := newStream(data)

	// Read all data
	result, err := io.ReadAll(s)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if string(result) != string(data) {
		t.Errorf("Read() = %q, want %q", result, data)
	}
}

func TestStream_Seek(t *testing.T) {
	t.Parallel()

	data := []byte("hello, world")
	s := newStream(data)

	// Read all
	if _, err := io.ReadAll(s); err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	// Seek to beginning
	pos, err := s.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatalf("Seek() error = %v", err)
	}
	if pos != 0 {
		t.Errorf("Seek() pos = %d, want 0", pos)
	}

	// Read again
	result, err := io.ReadAll(s)
	if err != nil {
		t.Fatalf("ReadAll() after Seek error = %v", err)
	}
	if string(result) != string(data) {
		t.Errorf("After Seek, Read() = %q, want %q", result, data)
	}
}
