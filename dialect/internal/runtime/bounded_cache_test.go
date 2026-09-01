package runtime

import (
	"errors"
	"strconv"
	"testing"
)

// TestBoundedCacheStopsGrowing pins the property the type exists for: it keeps
// what it can and then stops, rather than growing with the data. A pattern or a
// template can be a column, and an unbounded map keyed by data is a way to run
// out of memory on a large file.
func TestBoundedCacheStopsGrowing(t *testing.T) {
	t.Parallel()

	var cache boundedCache[int]
	derived := 0
	for i := range maxCachedDerivations * 3 {
		got := cache.value(strconv.Itoa(i), func() int {
			derived++
			return i
		})
		if got != i {
			t.Fatalf("value(%d) = %d", i, got)
		}
	}
	if size := cache.size(); size != maxCachedDerivations {
		t.Errorf("the cache holds %d entries, want the bound of %d", size, maxCachedDerivations)
	}
	if derived != maxCachedDerivations*3 {
		t.Errorf("derived %d times over %d distinct keys", derived, maxCachedDerivations*3)
	}

	// What it kept, it serves without deriving again.
	if got := cache.value("0", func() int { return -1 }); got != 0 {
		t.Errorf("a kept key was not served: got %d", got)
	}
	// What it did not keep, it derives again, and the answer is the
	// derivation's: correctness does not depend on being cached.
	if got := cache.value(strconv.Itoa(maxCachedDerivations*2), func() int { return 42 }); got != 42 {
		t.Errorf("a key past the bound was not derived again: got %d", got)
	}
}

// TestBoundedCacheDoesNotRememberAFailure pins that a derivation that fails
// costs its error every time rather than a map entry, which is what keeps a
// pattern that will not compile from filling the cache.
func TestBoundedCacheDoesNotRememberAFailure(t *testing.T) {
	t.Parallel()

	var cache boundedCache[int]
	failure := errors.New("cannot derive this")
	for range 3 {
		if _, err := cache.get("k", func() (int, error) { return 0, failure }); !errors.Is(err, failure) {
			t.Fatalf("err = %v, want the derivation's own", err)
		}
	}
	if size := cache.size(); size != 0 {
		t.Errorf("the cache holds %d entries after three failures", size)
	}

	if got, err := cache.get("k", func() (int, error) { return 7, nil }); err != nil || got != 7 {
		t.Errorf("get after a failure = %d, %v", got, err)
	}
	if size := cache.size(); size != 1 {
		t.Errorf("the cache holds %d entries after one success", size)
	}
}
