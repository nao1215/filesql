#!/bin/sh
# Build and run the examples against the filesql checkout they sit in, then
# compare what each one prints with the "Expected Output" block of its README.
#
# The examples are separate Go modules pinned to a released filesql, so the
# root `go test ./...` never compiles them and a change that breaks one is
# invisible until someone opens the directory. This script wires them to the
# working tree through a temporary go.work instead of editing their go.mod, so
# a shrunk or renamed public symbol fails here while the checked-in go.mod
# keeps showing readers the released version they would actually import.
#
# Usage: examples/verify.sh [example-directory ...]   (default: all of them)
set -eu

root=$(cd "$(dirname "$0")/.." && pwd)
if [ "$#" -gt 0 ]; then
	targets=$*
else
	targets="basic multi-format sqlc gorm sqlx bun squirrel ent"
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
GOWORK="$work/go.work"
export GOWORK

status=0
for target in $targets; do
	dir="$root/examples/$target"
	if [ ! -d "$dir" ]; then
		echo "verify.sh: no such example: $target" >&2
		exit 1
	fi

	echo "==> $target"
	rm -f "$GOWORK" "$work/go.work.sum"
	(cd "$dir" && go work init "$root" .)
	(cd "$dir" && go vet ./...)

	# The block is the documentation users read before running anything, so it
	# is the golden file: no separate testdata to drift away from the README.
	awk '
		/^## Expected Output$/ { seen = 1; next }
		seen && /^```/ { if (inside) exit; inside = 1; next }
		inside { print }
	' "$dir/README.md" >"$work/expected"
	if ! grep -q '[^[:space:]]' "$work/expected"; then
		echo "verify.sh: $target/README.md has no Expected Output block" >&2
		status=1
		continue
	fi

	if ! (cd "$dir" && go run .) >"$work/actual"; then
		echo "verify.sh: $target failed to run" >&2
		status=1
		continue
	fi

	if ! diff -u "$work/expected" "$work/actual"; then
		echo "verify.sh: $target printed something its README does not document" >&2
		status=1
	fi
done

exit "$status"
