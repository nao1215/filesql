package filesql

import (
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// releaseMetadataScript is the gate the release workflow runs before anything is
// published. It lives in .github/scripts rather than inline in the workflow so
// that it can be run here: a release workflow's own logic is otherwise only ever
// exercised by a tag, which is the one moment it must not be wrong.
const releaseMetadataScript = ".github/scripts/release-metadata.sh"

// runReleaseMetadata runs the script against a changelog written for the test
// and returns its key=value output as a map, its stderr, and whether it
// succeeded.
func runReleaseMetadata(t *testing.T, tag, changelog string) (map[string]string, string, bool) {
	t.Helper()

	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skipf("bash is not available: %v", err)
	}

	dir := t.TempDir()
	changelogPath := filepath.Join(dir, "CHANGELOG.md")
	if writeErr := os.WriteFile(changelogPath, []byte(changelog), 0o600); writeErr != nil {
		t.Fatalf("failed to write the changelog fixture: %v", writeErr)
	}
	bodyPath := filepath.Join(dir, "body.md")

	cmd := exec.CommandContext(t.Context(), bash, releaseMetadataScript, tag, changelogPath) //nolint:gosec // fixed, in-repo script path
	cmd.Env = append(os.Environ(), "CHANGELOG_BODY_FILE="+bodyPath)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, runErr := cmd.Output()

	values := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		key, value, found := strings.Cut(line, "=")
		if found {
			values[key] = value
		}
	}
	if runErr == nil {
		body, readErr := os.ReadFile(bodyPath) //nolint:gosec // path from t.TempDir()
		if readErr != nil {
			t.Fatalf("the script succeeded but wrote no body file: %v", readErr)
		}
		values["body"] = string(body)
	}
	return values, stderr.String(), runErr == nil
}

const releaseTestChangelog = `# Changelog

## [Unreleased]

- not released yet

## [1.2.3] - 2026-08-26

### Added

- a thing

### Fixed

- another thing

## [1.2.0-rc.1] - 2026-08-01

- a candidate

## [1.1.0] - 2026-07-01

## [1.0.0] - 2026-06-01

- the first one
`

// TestReleaseMetadata_RefusesATagThatIsNotSemVer pins the first gate. A tag is
// what starts the release, and a malformed one used to be carried straight
// through into the release name.
func TestReleaseMetadata_RefusesATagThatIsNotSemVer(t *testing.T) {
	t.Parallel()

	for _, tag := range []string{
		"v1.2",         // too few parts
		"v1.2.3.4",     // too many
		"1.2.3",        // this project tags with a leading v
		"v01.2.3",      // SemVer has no leading zeros
		"vX.Y.Z",       // not numbers
		"v1.0.0-",      // an empty prerelease identifier
		"v1.0.0-rc..1", // an empty identifier inside one
		"latest",
		"",
	} {
		t.Run(tag, func(t *testing.T) {
			t.Parallel()

			_, stderr, ok := runReleaseMetadata(t, tag, releaseTestChangelog)
			if ok {
				t.Fatalf("tag %q was accepted; a release must not start from it", tag)
			}
			if stderr == "" {
				t.Error("a refusal must say why on stderr")
			}
		})
	}
}

// TestReleaseMetadata_RefusesATagWithNoChangelogEntry pins the fallback that was
// removed. The workflow used to write "No changelog entry found for version
// v1.2.3" into the release body and publish it, which turns a missing entry into
// the release notes people read instead of into a failure somebody fixes.
func TestReleaseMetadata_RefusesATagWithNoChangelogEntry(t *testing.T) {
	t.Parallel()

	for _, tag := range []string{"v9.9.9", "v1.2.4", "v1.1.0"} {
		t.Run(tag, func(t *testing.T) {
			t.Parallel()

			values, stderr, ok := runReleaseMetadata(t, tag, releaseTestChangelog)
			if ok {
				t.Fatalf("tag %q was accepted with body %q; a release must not go out without notes", tag, values["body"])
			}
			if !strings.Contains(stderr, "no entry") {
				t.Errorf("stderr = %q, want it to say the changelog has no entry", stderr)
			}
			if strings.Contains(stderr, "No changelog entry found for version") {
				t.Error("the fallback message is back; a missing entry must fail rather than be published")
			}
		})
	}
}

// TestReleaseMetadata_ExtractsTheWholeEntry pins that an entry's subsections
// come with it. Ending the section at the next heading of any depth cut every
// release note off at its first "### Added".
func TestReleaseMetadata_ExtractsTheWholeEntry(t *testing.T) {
	t.Parallel()

	values, stderr, ok := runReleaseMetadata(t, "v1.2.3", releaseTestChangelog)
	if !ok {
		t.Fatalf("v1.2.3 must be accepted: %s", stderr)
	}
	if values["version"] != "1.2.3" {
		t.Errorf("version = %q, want 1.2.3", values["version"])
	}
	body := values["body"]
	for _, want := range []string{"### Added", "- a thing", "### Fixed", "- another thing"} {
		if !strings.Contains(body, want) {
			t.Errorf("body is missing %q:\n%s", want, body)
		}
	}
	for _, unwanted := range []string{"not released yet", "a candidate", "the first one"} {
		if strings.Contains(body, unwanted) {
			t.Errorf("body reached into a neighboring entry (%q):\n%s", unwanted, body)
		}
	}
}

// TestReleaseMetadata_MarksAPrereleaseBySemVer pins what decides the flag. It
// used to be whether the tag contained "-alpha", "-beta" or "-rc", so
// v1.0.0-pre.1 was published as the current release — and the current release
// is what pkg.go.dev and this project's own README point at.
func TestReleaseMetadata_MarksAPrereleaseBySemVer(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		tag  string
		want string
	}{
		{"v2.0.0", "false"},
		{"v2.0.0+build.7", "false"},
		{"v2.0.0-alpha", "true"},
		{"v2.0.0-alpha.1", "true"},
		{"v2.0.0-beta.2", "true"},
		{"v2.0.0-rc.1", "true"},
		{"v2.0.0-pre.1", "true"},
		{"v2.0.0-0", "true"},
	} {
		t.Run(tt.tag, func(t *testing.T) {
			t.Parallel()

			// Every tag under test needs an entry, since the changelog gate
			// runs first. Build metadata is not part of what identifies a
			// release, so that tag finds the plain 2.0.0 entry.
			changelog := "# Changelog\n\n## [2.0.0-alpha]\n\n- a\n\n## [2.0.0-alpha.1]\n\n- a1\n" +
				"\n## [2.0.0-beta.2]\n\n- b\n\n## [2.0.0-rc.1]\n\n- rc\n" +
				"\n## [2.0.0-pre.1]\n\n- pre\n\n## [2.0.0-0]\n\n- zero\n\n## [2.0.0] - 2026-09-01\n\n- final\n"

			values, stderr, ok := runReleaseMetadata(t, tt.tag, changelog)
			if !ok {
				t.Fatalf("tag %q must be accepted: %s", tt.tag, stderr)
			}
			if values["prerelease"] != tt.want {
				t.Errorf("prerelease = %q, want %q", values["prerelease"], tt.want)
			}
		})
	}
}

// TestReleaseMetadata_ReadsEveryHeadingFormThisFileHasUsed keeps the extraction
// working across the shapes CHANGELOG.md has carried, since a release is cut
// from whichever form the entry happens to be written in.
func TestReleaseMetadata_ReadsEveryHeadingFormThisFileHasUsed(t *testing.T) {
	t.Parallel()

	for _, heading := range []string{
		"## [3.1.0] - 2026-08-26",
		"## [3.1.0]",
		"## 3.1.0",
		"## [v3.1.0] - 2026-08-26",
		"### [3.1.0]",
	} {
		t.Run(heading, func(t *testing.T) {
			t.Parallel()

			values, stderr, ok := runReleaseMetadata(t, "v3.1.0", "# Changelog\n\n"+heading+"\n\n- the entry\n")
			if !ok {
				t.Fatalf("heading %q must be found: %s", heading, stderr)
			}
			if !strings.Contains(values["body"], "- the entry") {
				t.Errorf("body = %q, want the entry", values["body"])
			}
		})
	}
}

// TestReleaseMetadata_RefusesAnEmptyEntry covers the heading that is there with
// nothing under it, which is a release with no notes by another route.
func TestReleaseMetadata_RefusesAnEmptyEntry(t *testing.T) {
	t.Parallel()

	_, stderr, ok := runReleaseMetadata(t, "v4.0.0", "# Changelog\n\n## [4.0.0] - 2026-08-26\n\n## [3.0.0]\n\n- old\n")
	if ok {
		t.Fatal("an entry with nothing under its heading must be refused")
	}
	if !strings.Contains(stderr, "no entry") {
		t.Errorf("stderr = %q, want it to say the changelog has no entry", stderr)
	}
}

// TestReleaseWorkflowGatesTheRelease reads the workflow itself. The script above
// only matters if the workflow runs it, and the release step only comes after
// the checks if the file says so.
func TestReleaseWorkflowGatesTheRelease(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile(".github/workflows/release.yml")
	if err != nil {
		t.Fatalf("failed to read the release workflow: %v", err)
	}
	workflow := string(raw)

	for _, want := range []string{
		releaseMetadataScript,
		"go vet ./...",
		"go test",
		"golangci-lint",
		"examples/verify.sh",
		"softprops/action-gh-release",
	} {
		if !strings.Contains(workflow, want) {
			t.Errorf("the release workflow no longer runs %q", want)
		}
	}

	if strings.Contains(workflow, "No changelog entry found") {
		t.Error("the release workflow still falls back to a placeholder body")
	}
	if strings.Contains(workflow, "contains(steps.tag.outputs.TAG_NAME, '-alpha')") {
		t.Error("the release workflow still decides prerelease by substring")
	}

	release := strings.Index(workflow, "softprops/action-gh-release")
	for _, gate := range []string{releaseMetadataScript, "go vet ./...", "examples/verify.sh"} {
		if at := strings.Index(workflow, gate); at > release {
			t.Errorf("%q runs after the release is created", gate)
		}
	}

	// The checks and the release are two jobs so that the only token able to
	// write to the repository belongs to the job that does nothing but create
	// the release. Verification runs third-party actions and this module's own
	// tests, and a write-capable token in the same job would be reachable from
	// all of them.
	if !strings.Contains(workflow, "needs: verify") {
		t.Error("the release job no longer waits for the verification job")
	}
	if at := strings.Index(workflow, "contents: write"); at == -1 || at > release {
		t.Error("the release job must be the one that declares contents: write")
	}
	if verify := strings.Index(workflow, "verify:"); verify == -1 {
		t.Error("the verification job is gone")
	} else if strings.Contains(workflow[verify:strings.Index(workflow, "release:")], "contents: write") {
		t.Error("the verification job may not hold a token that can write to the repository")
	}
}

// TestWorkflowsNameOnlyPathsThatExist holds the CI workflows to the repository
// they run against. A workflow names package paths, scripts and config files in
// strings nothing compiles, so a package that moves leaves a job naming a
// directory that is gone -- which is what happened to the fuzz workflow when the
// parser package went under internal/, and what nobody saw for two releases
// because a scheduled job's failure notifies no pull request.
func TestWorkflowsNameOnlyPathsThatExist(t *testing.T) {
	t.Parallel()

	entries, err := os.ReadDir(".github/workflows")
	if err != nil {
		t.Fatalf("failed to read the workflows: %v", err)
	}

	// A path in a workflow is a word beginning "./" or naming a file in the
	// repository; these are the two shapes the workflows here use.
	pathWord := regexp.MustCompile(`(?:^|[\s'"=])(\./[A-Za-z0-9._/-]+|\.github/[A-Za-z0-9._/-]+)`)

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yml") {
			continue
		}
		name := entry.Name()
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			body, readErr := os.ReadFile(filepath.Join(".github/workflows", name)) //nolint:gosec // fixed, in-repo path
			if readErr != nil {
				t.Fatalf("failed to read %s: %v", name, readErr)
			}
			for _, match := range pathWord.FindAllStringSubmatch(string(body), -1) {
				path := strings.TrimRight(match[1], "/.,")
				if strings.ContainsAny(path, "*$") || strings.Contains(match[1], "...") {
					// A glob, a workflow expression, or Go's own "./..."
					// wildcard, none of which names one path.
					continue
				}
				if _, statErr := os.Stat(path); statErr != nil { //nolint:gosec // a path read out of an in-repo workflow, only stat-ed
					t.Errorf("%s names %q, which is not in the repository", name, path)
				}
			}
		})
	}
}

// TestFuzzWorkflowRunsEveryTarget holds the fuzz workflow's matrix to the fuzz
// targets the module has. `go test` walks a target's seed corpus and stops, so
// this workflow is the only thing that searches past the seeds: a target it
// does not name is a target nothing fuzzes, and an entry naming a package that
// does not hold its target is a job that fails every night.
func TestFuzzWorkflowRunsEveryTarget(t *testing.T) {
	t.Parallel()

	body, err := os.ReadFile(".github/workflows/fuzz.yml")
	if err != nil {
		t.Fatalf("failed to read the fuzz workflow: %v", err)
	}

	// The matrix entries are pairs of lines: "- package: X" then "target: Y".
	matrix := map[string]string{} // target -> package
	pkg := ""
	for line := range strings.Lines(string(body)) {
		trimmed := strings.TrimSpace(line)
		if rest, found := strings.CutPrefix(trimmed, "- package:"); found {
			pkg = strings.TrimSpace(rest)
			continue
		}
		if rest, found := strings.CutPrefix(trimmed, "target:"); found && pkg != "" {
			matrix[strings.TrimSpace(rest)] = pkg
		}
	}
	if len(matrix) == 0 {
		t.Fatal("the fuzz workflow's matrix could not be read")
	}

	// The targets the module holds, and the package directory of each.
	targets := map[string]string{}
	fuzzFunc := regexp.MustCompile(`(?m)^func (Fuzz[A-Za-z0-9_]*)\(`)
	walkErr := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() && (entry.Name() == ".git" || entry.Name() == "testdata" || entry.Name() == "examples") {
			return filepath.SkipDir
		}
		if entry.IsDir() || !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		body, readErr := os.ReadFile(path) //nolint:gosec // walked, in-repo path
		if readErr != nil {
			return readErr
		}
		for _, match := range fuzzFunc.FindAllStringSubmatch(string(body), -1) {
			targets[match[1]] = "./" + filepath.ToSlash(filepath.Dir(path))
		}
		return nil
	})
	if walkErr != nil {
		t.Fatalf("failed to walk the module: %v", walkErr)
	}

	for target, dir := range targets {
		named, found := matrix[target]
		if !found {
			t.Errorf("%s is a fuzz target in %s and the workflow does not run it", target, dir)
			continue
		}
		if named != dir {
			t.Errorf("the workflow runs %s in %s; it is in %s", target, named, dir)
		}
	}
	for target, dir := range matrix {
		if _, found := targets[target]; !found {
			t.Errorf("the workflow names %s in %s, and no package holds a target by that name", target, dir)
		}
	}
}
