package lower_test

import (
	"testing"

	"github.com/nao1215/filesql/dialect/internal/dialects"
	"github.com/nao1215/filesql/dialect/internal/lower"
	"github.com/nao1215/filesql/dialect/internal/runtime"
)

// TestCastTargetsMatchTheHelpers holds the table lowering reads against the one
// the helpers convert by. The two are separate because the dependency runs one
// way -- lowering names helpers, and the helpers know nothing about lowering --
// and a target in one and not the other would fall through to SQLite's own
// CAST without anything saying so.
func TestCastTargetsMatchTheHelpers(t *testing.T) {
	t.Parallel()

	for _, d := range dialects.All() {
		if d == dialects.SQLite {
			continue
		}
		for _, name := range lower.CastTargets(d) {
			if !runtime.KnowsCastTarget(d, name) {
				t.Errorf("%s: lowering sends %s to a helper that does not convert it", d, name)
			}
		}
	}
}

// TestHelperNamesMatchTheRuntime holds the table of helpers lowering reads
// against the ones the runtime registers. The two are separate because the
// dependency runs one way, and a difference between them changes behavior with
// nothing saying so: a name in one and not the other changes what the SAFE
// prefix does, and an argument count in one and not the other decides whether a
// call is refused here under the caller's own function name or reaches the
// driver and fails there under the helper's.
func TestHelperNamesMatchTheRuntime(t *testing.T) {
	t.Parallel()

	registered := make(map[string]bool)
	for _, name := range runtime.RegisteredNames() {
		registered[name] = true
	}
	known := make(map[string]bool)
	for _, name := range lower.HelperNames() {
		known[name] = true
		if !registered[name] {
			t.Errorf("lowering knows %q, which the runtime does not register", name)
			continue
		}
		want, _ := runtime.RegisteredArity(name)
		got, _ := lower.HelperArity(name)
		if got != want {
			t.Errorf("lowering has %q taking %d arguments, the runtime registers it taking %d", name, got, want)
		}
	}
	for name := range registered {
		if !known[name] {
			t.Errorf("the runtime registers %q, which lowering does not know", name)
		}
	}
}
