//nolint:errcheck // The target is what a decompressor does with hostile bytes; the read and the release are driven for their side effects, and their errors are the ordinary answer.
package codec

import (
	"bytes"
	"io"
	"runtime"
	"testing"
)

// FuzzCodecReader holds two properties over the decompressors, on any bytes at
// all: reading a stream never panics, and building the reader never allocates
// working memory out of proportion to the stream that asked for it.
//
// The second is the one this repository has been bitten by twice. An xz stream
// states its dictionary size in one byte and a zstd frame states its window
// size in one byte, and both libraries allocate what the byte asks for before
// reading any compressed data, so a few bytes bought gigabytes. Those two are
// now bounded by the codec package itself, and the bound here is what keeps a
// third codec, or a later version of one of these libraries, from reopening the
// same hole quietly: a stream is a header until it is read, and a header is
// small.
func FuzzCodecReader(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("not compressed at all"))
	f.Add(xzStream(40, false))
	f.Add(xzStream(22, false))
	f.Add(zstdFrame(19, 0))
	f.Add(zstdFrame(11, 0))
	for _, c := range []Codec{GZ, XZ, ZSTD, ZLIB, SNAPPY, S2, LZ4} {
		var out bytes.Buffer
		writer, closeFn, err := c.NewWriter(&out)
		if err != nil {
			f.Fatal(err)
		}
		if _, err := writer.Write([]byte("id,name\n1,alice\n")); err != nil {
			f.Fatal(err)
		}
		if err := closeFn(); err != nil {
			f.Fatal(err)
		}
		f.Add(out.Bytes())
	}

	// A header is small, so what a reader may hold before any data is read is
	// small too. The margin is wide enough for a decompressor's own buffers and
	// far below the sizes the two fixed bugs reached, which were 4 GiB and
	// 512 MiB from streams of 28 and 14 bytes.
	const maxSetupAlloc = 64 << 20

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<16 {
			t.Skip()
		}
		for _, c := range []Codec{GZ, BZ2, XZ, ZSTD, ZLIB, SNAPPY, S2, LZ4} {
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			reader, closeFn, err := c.NewReader(bytes.NewReader(data))
			runtime.ReadMemStats(&after)
			if allocated := after.TotalAlloc - before.TotalAlloc; allocated > maxSetupAlloc {
				t.Fatalf("%s: opening a %d-byte stream allocated %d MiB before reading any of it",
					c, len(data), allocated>>20)
			}
			if err != nil {
				continue
			}
			// Bounded, because a legitimate stream may expand without limit and
			// this target is about the reader, not about compression ratios.
			_, _ = io.Copy(io.Discard, io.LimitReader(reader, 1<<20))
			_ = closeFn()
		}
	})
}
