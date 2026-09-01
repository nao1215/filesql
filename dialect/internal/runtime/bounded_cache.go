package runtime

import (
	"sync"
	"sync/atomic"
)

// maxCachedDerivations bounds every cache in this package. A query holds a
// handful of distinct patterns and templates; a number this size is reached
// only by one that comes from the data, and then the cache stops growing and
// the work runs as it did before there was a cache.
const maxCachedDerivations = 256

// boundedCache remembers a value derived from a string, up to a bound.
//
// Deriving is most of what these calls cost -- compiling a regular expression,
// scanning a TO_CHAR template -- and the same string arrives once per row when
// it is written as a literal, which is what the cache is for. The bound is for
// the other case: a pattern or a format can be a column, and an unbounded map
// keyed by data is a way to run out of memory on a large file. Nothing evicts,
// because the point is to stop growing rather than to keep the most useful
// entries; past the bound every call derives, which is where the code started.
//
// It is one type rather than one per cache because the rule was written twice
// and remembered once: the compiled-pattern cache grew without a bound while
// the template cache beside it explained why it must not.
type boundedCache[V any] struct {
	entries sync.Map
	count   atomic.Int64
}

// get is the cached value for key, deriving it when it is not there. A derive
// that fails is not remembered, so a pattern that will not compile costs its
// error every time rather than a map entry.
func (c *boundedCache[V]) get(key string, derive func() (V, error)) (V, error) {
	if cached, ok := c.entries.Load(key); ok {
		return cached.(V), nil //nolint:forcetypeassert,errcheck // the map holds only this type
	}
	value, err := derive()
	if err != nil {
		var zero V
		return zero, err
	}
	if c.count.Load() < maxCachedDerivations {
		if _, loaded := c.entries.LoadOrStore(key, value); !loaded {
			c.count.Add(1)
		}
	}
	return value, nil
}

// value is get for a derivation that cannot fail, which is every caller but the
// one compiling a regular expression.
func (c *boundedCache[V]) value(key string, derive func() V) V {
	cached, _ := c.get(key, func() (V, error) { return derive(), nil }) //nolint:errcheck // derive returns no error
	return cached
}

// size is how many entries the cache holds, which is what a test asserts the
// bound with.
func (c *boundedCache[V]) size() int64 { return c.count.Load() }
