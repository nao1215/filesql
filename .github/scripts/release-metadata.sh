#!/usr/bin/env bash
#
# release-metadata.sh validates a release tag and extracts the changelog entry
# that belongs to it. It is the gate the release workflow runs before anything
# is published, and it is a script rather than inline YAML so that it can be
# run and tested outside GitHub Actions -- see release_metadata_test.go.
#
# Usage:
#   release-metadata.sh <tag> [changelog-path]
#
# On success it prints, one per line, on stdout:
#
#   tag=v1.2.3
#   version=1.2.3
#   prerelease=false
#   body_path=changelog_content.md
#
# which is the key=value form GitHub Actions reads from $GITHUB_OUTPUT, and it
# writes the release body to body_path. Set CHANGELOG_BODY_FILE to choose that
# path.
#
# It exits non-zero, printing why on stderr, when the tag is not a SemVer
# version or when the changelog has no entry for it. There is no fallback: a
# release used to go out with the body "No changelog entry found for version
# v1.2.3", which publishes the mistake instead of stopping for it, and by then
# the tag exists and the release is the artifact people see.
set -euo pipefail

readonly PROGRAM="release-metadata.sh"

fail() {
	printf '%s: %s\n' "$PROGRAM" "$1" >&2
	exit 1
}

tag="${1-}"
changelog="${2-CHANGELOG.md}"
body_path="${CHANGELOG_BODY_FILE-changelog_content.md}"

if [ -z "$tag" ]; then
	printf 'usage: %s <tag> [changelog-path]\n' "$PROGRAM" >&2
	exit 2
fi

# The published SemVer 2.0.0 grammar, with the leading "v" this project tags
# with. Anchored, so "v1.2" and "v1.2.3.4" are refused rather than half-read,
# and leading zeros are refused because SemVer says they are not numbers.
readonly SEMVER='^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-((0|[1-9][0-9]*|[0-9]*[a-zA-Z-][0-9a-zA-Z-]*)(\.(0|[1-9][0-9]*|[0-9]*[a-zA-Z-][0-9a-zA-Z-]*))*))?(\+([0-9a-zA-Z-]+(\.[0-9a-zA-Z-]+)*))?$'

if ! printf '%s' "$tag" | grep -Eq "$SEMVER"; then
	fail "tag \"$tag\" is not a SemVer version; want vMAJOR.MINOR.PATCH with an optional -prerelease and +build"
fi

version="${tag#v}"

# A release is a prerelease exactly when SemVer says it is: the tag carries a
# prerelease identifier. Matching the substrings "-alpha", "-beta" and "-rc" was
# what this decided by before, which called v1.0.0-pre.1 a full release -- and
# a full release is the one npm-style consumers and this project's own README
# point at, so the mistake ships the unfinished thing as the current one.
core="${version%%+*}"
if [ "${core#*-}" != "$core" ]; then
	prerelease=true
else
	prerelease=false
fi

[ -f "$changelog" ] || fail "changelog $changelog does not exist"

# changelog_body prints the body of the section whose heading names $1. A
# heading is normalized before it is compared -- its #s, its brackets, a leading
# "v" and a trailing " - 2026-08-26" date are removed -- so every form this file
# has used matches: "## [1.2.3] - 2026-08-26", "## 1.2.3", "### [v1.2.3]".
changelog_body() {
	awk -v want="$1" '
		function normalize(heading,    h) {
			h = heading
			sub(/^#+[ \t]*/, "", h)
			sub(/[ \t]*$/, "", h)
			sub(/[ \t]*-[ \t]*[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9].*$/, "", h)
			gsub(/[][]/, "", h)
			sub(/^v/, "", h)
			return h
		}
		/^#+[ \t]/ {
			match($0, /^#+/)
			level = RLENGTH
			# Only a heading at the same depth or shallower ends the
			# section. The "### Added" and "### Fixed" subheadings inside an
			# entry are part of it, and treating any heading as the end cut
			# every release note off after its first line.
			if (found) {
				if (level <= found_level) { exit }
			} else if (normalize($0) == want) {
				found = 1
				found_level = level
				next
			}
		}
		found { print }
	' "$changelog"
}

# Trim the blank lines a section is separated from its neighbors by, so a
# heading followed by nothing at all reads as empty rather than as a body of
# newlines.
trim_blank_lines() {
	sed -e '/./,$!d' | sed -e :a -e '/^\n*$/{$d;N;};/\n$/ba'
}

body="$(changelog_body "$version" | trim_blank_lines)"

# SemVer says build metadata takes no part in identifying a release, so a
# v1.2.3+build.7 tag is the changelog's 1.2.3 entry. The prerelease part does
# identify one, so it is never dropped.
if [ -z "$body" ] && [ "$core" != "$version" ]; then
	body="$(changelog_body "$core" | trim_blank_lines)"
fi

if [ -z "$body" ]; then
	fail "$changelog has no entry for $version; add one under a \"## [$version]\" heading before tagging"
fi

printf '%s\n' "$body" > "$body_path"

printf 'tag=%s\n' "$tag"
printf 'version=%s\n' "$version"
printf 'prerelease=%s\n' "$prerelease"
printf 'body_path=%s\n' "$body_path"
