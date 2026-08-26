//nolint:errcheck // The target is what a decompressor does with hostile bytes; the read and the release are driven for their side effects, and their errors are the ordinary answer.
package codec

import (
	"bytes"
	"io"
	"runtime"
	"testing"
)

// maxSetupAlloc is what opening a stream may allocate before any of it has
// been read.
//
// It is what this package says a stream may cost, rather than a number of its
// own. A zstd frame declaring maxZstdWindow and an xz stream declaring
// maxXZDictionary are both accepted -- README states that ceiling as the
// intended price of a damaged file -- so a budget below either of them fails
// the target on a stream the package promises to read, which is a red nightly
// and no defect. Deriving it here is what keeps the two from drifting apart
// when a ceiling moves.
//
// The margin on top covers the decompressor's own buffers and the fact that
// this measurement is process-wide: klauspost/compress allocates the window in
// goroutines that outlive zstd.NewReader, so a previous iteration's work can
// land inside this one's reading.
const maxSetupAlloc = maxAcceptedDeclaration + 64<<20

// maxAcceptedDeclaration is the largest working memory any stream this package
// accepts may declare.
const maxAcceptedDeclaration = max(maxXZDictionary, maxZstdWindow)

// TestFuzzBudgetStillCatchesTheBugsItWasWrittenFor pins the budget between the
// two numbers that give it meaning: above every declaration this package
// accepts, so it cannot fail on a stream that is merely expensive, and below
// the smaller of the two failures it exists to catch, so it has not been
// widened into a tautology. Those were 4 GiB from a 28-byte xz stream and
// 512 MiB from a 14-byte zstd one. A ceiling raised past this range has to be
// argued for here rather than discovered by a nightly.
func TestFuzzBudgetStillCatchesTheBugsItWasWrittenFor(t *testing.T) {
	t.Parallel()

	const smallestBugCaught = 512 << 20
	if maxSetupAlloc <= maxAcceptedDeclaration {
		t.Errorf("budget %d MiB is not above the largest accepted declaration %d MiB",
			maxSetupAlloc>>20, maxAcceptedDeclaration>>20)
	}
	if maxSetupAlloc >= smallestBugCaught {
		t.Errorf("budget %d MiB would no longer catch the %d MiB allocation it was written for",
			maxSetupAlloc>>20, smallestBugCaught>>20)
	}
}

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
	// The two sides of the xz ceiling, which had no seed of its own. Property 32
	// encodes exactly maxXZDictionary and is accepted; 34 encodes twice that and
	// is refused. The accepted one is the expensive case worth opening every
	// run, the same way the zstd frame below is.
	f.Add(xzStream(32, false))
	f.Add(xzStream(34, false))
	f.Add(zstdFrame(19, 0))
	f.Add(zstdFrame(11, 0))
	// The two sides of the zstd ceiling. Exponent 17 selects 2^27, which is
	// exactly maxZstdWindow and is accepted; 18 selects twice that and is
	// refused. The accepted one is the most a zstd stream may ask this package
	// to spend, so it is the one worth opening every run.
	f.Add(zstdFrame(17, 0))
	f.Add(zstdFrame(18, 0))
	// A frame the fuzzer found: a valid header declaring a 64 MiB window
	// (descriptor 0x80 is exponent 16, so 2^26) in front of bytes that are not
	// compressed data. It is accepted, because the window is under the ceiling,
	// and opening it costs that window. Seeded so the most expensive stream
	// this package accepts is opened on every run rather than when the fuzzer
	// rediscovers it.
	f.Add([]byte("(\xb5/\xfd\x04\x80\x81\x00\x00id:name\n1,alice\n\x8c\xd7\xcf\xef"))
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
