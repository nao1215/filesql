//nolint:errcheck,gosec // Test helpers build headers by hand; the widths are fixed and the cleanup errors are not the subject
package codec

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"runtime"
	"testing"
)

// xzStream builds a minimal xz stream whose LZMA2 filter property asks for the
// dictionary the prop byte encodes, optionally with the compressed and
// uncompressed size fields that every CLI level writes between the block flags
// and the filter list. The sizes here are one byte each, where a real file's are
// multi-byte integers that depend on its contents.
func xzStream(prop byte, withSizes bool) []byte {
	var b bytes.Buffer
	b.Write([]byte{0xFD, '7', 'z', 'X', 'Z', 0x00})
	flags := []byte{0x00, 0x00}
	b.Write(flags)
	_ = binary.Write(&b, binary.LittleEndian, crc32.ChecksumIEEE(flags))

	fields := []byte{0x00, 0x00}
	if withSizes {
		fields[1] = 0xC0                    // both size fields present, one filter
		fields = append(fields, 0x14, 0x10) // the two sizes, one byte each
	}
	fields = append(fields, lzma2FilterID, 0x01, prop)
	total := len(fields) + 4 // the CRC counts toward Block Header Size
	for total%4 != 0 {
		fields = append(fields, 0x00)
		total = len(fields) + 4
	}
	fields[0] = byte(total/4 - 1)
	b.Write(fields)
	_ = binary.Write(&b, binary.LittleEndian, crc32.ChecksumIEEE(fields))
	b.Write([]byte{0x01, 0x00, 0x00, 'A'})
	return b.Bytes()
}

// zstdFrame builds a minimal zstd frame whose window descriptor asks for the
// window the exponent and mantissa select.
func zstdFrame(exponent, mantissa byte) []byte {
	body := []byte("id\n1\n")
	b := []byte{0x28, 0xB5, 0x2F, 0xFD, 0x00, exponent<<3 | mantissa}
	v := uint32(1) | (uint32(len(body)) << 3)
	b = append(b, byte(v), byte(v>>8), byte(v>>16))
	return append(b, body...)
}

func TestLZMA2DictionarySize(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		block []byte
		want  uint64
		ok    bool
	}{
		{
			name:  "no size fields, which is what this package's own writer emits",
			block: []byte{0x02, 0x00, lzma2FilterID, 0x01, 22},
			want:  8 << 20,
			ok:    true,
		},
		{
			name:  "both size fields present, which is what the CLI emits",
			block: []byte{0x04, 0xC0, 0x8E, 0x04, 0xC0, 0x8D, 0xB7, 0x01, lzma2FilterID, 0x01, 28},
			want:  64 << 20,
			ok:    true,
		},
		{
			name:  "the format's maximum",
			block: []byte{0x01, 0x00, lzma2FilterID, 0x01, 40},
			want:  4 << 30,
			ok:    true,
		},
		{
			name:  "the format's minimum",
			block: []byte{0x01, 0x00, lzma2FilterID, 0x01, 0},
			want:  4 << 10,
			ok:    true,
		},
		{
			name:  "a property past what the format defines is not ours to read",
			block: []byte{0x01, 0x00, lzma2FilterID, 0x01, 41},
		},
		{
			name:  "a filter that is not LZMA2 carries no dictionary",
			block: []byte{0x01, 0x00, 0x03, 0x01, 40},
		},
		{
			name:  "a property list of the wrong length is not ours to read",
			block: []byte{0x01, 0x00, lzma2FilterID, 0x02, 40, 0x00},
		},
		{
			name:  "a header that ends inside its own filter list",
			block: []byte{0x01, 0x00, lzma2FilterID, 0x04, 40},
		},
		{
			name:  "a header too short to hold flags",
			block: []byte{0x01},
		},
		{
			name:  "two filters, LZMA2 last",
			block: []byte{0x02, 0x01, 0x03, 0x01, 0x00, lzma2FilterID, 0x01, 22},
			want:  8 << 20,
			ok:    true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, ok := lzma2DictionarySize(tc.block)
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
			if ok && got != tc.want {
				t.Errorf("dictionary = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestXZVarint(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		in    []byte
		want  uint64
		width int
	}{
		{name: "one byte", in: []byte{0x14}, want: 0x14, width: 1},
		{name: "two bytes", in: []byte{0x80, 0x01}, want: 128, width: 2},
		{name: "three bytes", in: []byte{0xFF, 0xFF, 0x03}, want: 65535, width: 3},
		{name: "stops at the first byte without the continuation bit", in: []byte{0x01, 0xFF}, want: 1, width: 1},
		{name: "runs past the buffer", in: []byte{0x80, 0x80}},
		{name: "empty", in: nil},
		{name: "past the nine bytes the format allows", in: bytes.Repeat([]byte{0x80}, 12)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, width := xzVarint(tc.in)
			if width != tc.width {
				t.Fatalf("width = %d, want %d", width, tc.width)
			}
			if width > 0 && got != tc.want {
				t.Errorf("value = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestNewReaderBoundsWhatAStreamMayDeclare pins that the working memory a
// stream costs is bounded by this package's limit rather than by the number the
// stream asserts. The oracle is bytes allocated, so the test does not run in
// parallel.
func TestNewReaderBoundsWhatAStreamMayDeclare(t *testing.T) {
	for _, tc := range []struct {
		name    string
		codec   Codec
		data    []byte
		refused bool
	}{
		{name: "xz 4 GiB dictionary", codec: XZ, data: xzStream(40, false), refused: true},
		{name: "xz 4 GiB dictionary with size fields", codec: XZ, data: xzStream(40, true), refused: true},
		{name: "xz 512 MiB dictionary", codec: XZ, data: xzStream(34, false), refused: true},
		{name: "xz 64 MiB dictionary, what xz -9 declares", codec: XZ, data: xzStream(28, true)},
		{name: "xz 8 MiB dictionary, what xz -6 declares", codec: XZ, data: xzStream(22, false)},
		{name: "zstd 512 MiB window", codec: ZSTD, data: zstdFrame(19, 0), refused: true},
		{name: "zstd 1.5 TiB window", codec: ZSTD, data: zstdFrame(30, 4), refused: true},
		{name: "zstd 128 MiB window, the limit itself", codec: ZSTD, data: zstdFrame(17, 0)},
		{name: "zstd 2 MiB window, what zstd -3 declares", codec: ZSTD, data: zstdFrame(11, 0)},
		// A file named for a codec that is not what it holds is refused for
		// what it is, not for a size it never declared: the bytes below would
		// read as a 4 GiB dictionary if the magic were not checked first.
		{name: "not an xz stream at all", codec: XZ, data: notXZButBlockHeaderShaped()},
		{name: "not a zstd frame at all", codec: ZSTD, data: []byte("not zstd at allZZ")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)

			reader, closeFn, err := tc.codec.NewReader(bytes.NewReader(tc.data))
			if err == nil {
				_, _ = io.Copy(io.Discard, reader)
				_ = closeFn()
			}
			runtime.ReadMemStats(&after)
			allocated := after.TotalAlloc - before.TotalAlloc

			if tc.refused {
				if !errors.Is(err, ErrDeclaredSizeTooLarge) {
					t.Fatalf("err = %v, want ErrDeclaredSizeTooLarge", err)
				}
				if allocated > 32<<20 {
					t.Errorf("refusing allocated %d MiB, which is paying for the size the stream asked for", allocated>>20)
				}
				return
			}
			if errors.Is(err, ErrDeclaredSizeTooLarge) {
				t.Fatalf("a size real files declare was refused: %v", err)
			}
		})
	}
}

// notXZButBlockHeaderShaped is twelve bytes that are not the xz magic, followed
// by bytes that do read as a block header declaring a 4 GiB dictionary. Without
// the magic check in front, such a file would be refused for a size it never
// declared rather than for not being an xz stream.
func notXZButBlockHeaderShaped() []byte {
	head := []byte("not an xz str") // thirteen bytes, so twelve plus a spare
	return append(head[:12], 0x01, 0x00, lzma2FilterID, 0x01, 40, 0x00, 0x00, 0x00)
}
