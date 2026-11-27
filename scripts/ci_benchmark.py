"""Compare the module's benchmarks at two commits on the same machine.

Both commits are extracted with `git archive`, so neither the checkout nor the
worktree is touched. Each benchmarked package is built once per commit into a
test binary, and the binaries of the two commits run in turn, round after
round, so that a change in the runner's speed during the job lands on both
alike.

A benchmark fails the gate only when it is slower on both counts:

- the median of the head's samples is more than --max-ratio times the base's;
- even the head's fastest sample is slower than the base's median.

The second condition keeps one slow sample, or a noisy neighbor on a shared
runner, from failing a pull request that did not change the code it measures.
"""
import argparse
import json
import os
import re
import statistics
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BENCHSTAT = 'golang.org/x/perf/cmd/benchstat@v0.0.0-20261117185222-4febdb082b2c'
LINE = re.compile(r'^(Benchmark\S+?)(?:-\d+)?\s+\d+\s+([0-9.]+)\s+ns/op\b')
# The packages whose benchmarks the gate runs. The benchmarks in the root
# package and in dialect sit behind the "benchmark" build tag, because the
# root package's read 100,000 rows and would slow every `go test`.
PACKAGES = ['.', './dialect', './prep', './internal/parser/ach', './internal/parser/wire']


def parse(text, package='.'):
    """Return {benchmark name: [ns/op, ...]} from `go test -bench` output.

    A benchmark outside the root package is named after its package as well,
    since ach and wire both have a BenchmarkParseReader.
    """
    prefix = '' if package == '.' else package.removeprefix('./') + '.'
    samples = {}
    for line in text.splitlines():
        match = LINE.match(line)
        if match:
            samples.setdefault(prefix + match[1], []).append(float(match[2]))
    return samples


def gate(base, head, max_ratio):
    """Return one row per benchmark measured at both commits, worst first."""
    rows = []
    for name in sorted(base.keys() & head.keys()):
        base_median = statistics.median(base[name])
        head_median = statistics.median(head[name])
        ratio = head_median / base_median
        regressed = ratio > max_ratio and min(head[name]) > base_median
        rows.append({'benchmark': name, 'base_ns': base_median, 'head_ns': head_median,
                     'ratio': ratio, 'regressed': regressed})
    rows.sort(key=lambda row: -row['ratio'])
    return rows


def build(revision, directory):
    """Extract revision into directory and build a test binary per package.

    A package that does not exist at revision (a new one on the head, say) is
    left out; the gate compares only what both commits measure.
    """
    archive = subprocess.check_output(['git', 'archive', revision], cwd=ROOT)
    subprocess.run(['tar', '-x', '-C', str(directory)], input=archive, check=True)
    binaries = {}
    for package in PACKAGES:
        if not (directory / package).is_dir():
            continue
        binary = directory / ('bench-' + package.strip('./').replace('/', '-') + '.test')
        subprocess.run(['go', 'test', '-tags', 'benchmark', '-c', '-o', str(binary), package],
                       cwd=directory, check=True)
        binaries[package] = binary
    return binaries


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--base', required=True)
    parser.add_argument('--head', required=True)
    parser.add_argument('--rounds', type=int, default=8)
    parser.add_argument('--benchtime', default='50ms')
    parser.add_argument('--bench', default='.')
    parser.add_argument('--max-ratio', type=float, default=1.25)
    parser.add_argument('--out', default=str(ROOT / 'bench-results'))
    args = parser.parse_args(argv)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    revisions = {label: subprocess.check_output(['git', 'rev-parse', '--verify', rev + '^{commit}'], cwd=ROOT, text=True).strip()
                 for label, rev in [('base', args.base), ('head', args.head)]}
    env = dict(os.environ, GOMAXPROCS=os.environ.get('GOMAXPROCS', '1'))
    texts = {'base': [], 'head': []}
    samples = {'base': {}, 'head': {}}
    with tempfile.TemporaryDirectory(prefix='filesql-bench-') as tmp:
        dirs, binaries = {}, {}
        for label, sha in revisions.items():
            dirs[label] = Path(tmp) / label
            dirs[label].mkdir()
            binaries[label] = build(sha, dirs[label])
        for i in range(args.rounds):
            for label in (['base', 'head'] if i % 2 == 0 else ['head', 'base']):
                for package, binary in binaries[label].items():
                    # The test binary runs in its package's directory, as
                    # `go test` would, so testdata/ paths resolve.
                    text = subprocess.check_output(
                        [str(binary), '-test.run=^$', '-test.bench=' + args.bench,
                         '-test.benchtime=' + args.benchtime, '-test.count=1'],
                        cwd=dirs[label] / package, env=env, text=True)
                    texts[label].append(text)
                    for name, values in parse(text, package).items():
                        samples[label].setdefault(name, []).extend(values)
    for label, text in texts.items():
        (out / (label + '.txt')).write_text(''.join(text))

    rows = gate(samples['base'], samples['head'], args.max_ratio)
    (out / 'gate.json').write_text(json.dumps({'revisions': revisions, 'rounds': args.rounds,
                                               'benchtime': args.benchtime, 'gomaxprocs': env['GOMAXPROCS'],
                                               'max_ratio': args.max_ratio, 'rows': rows}, indent=2) + '\n')
    summary = subprocess.run(['go', 'run', BENCHSTAT, str(out / 'base.txt'), str(out / 'head.txt')],
                             cwd=ROOT, capture_output=True, text=True)
    (out / 'benchstat.txt').write_text(summary.stdout + summary.stderr)
    print(summary.stdout)
    if summary.returncode != 0:
        # The gate below works on the raw samples and still decides; only the
        # human-readable summary is missing, and the log should say why.
        print(f'::warning::benchstat exited with {summary.returncode}: {summary.stderr.strip()}')

    regressed = [row for row in rows if row['regressed']]
    for row in regressed:
        print(f"::error::{row['benchmark']} is {row['ratio']:.2f}x slower "
              f"({row['base_ns']:.0f} -> {row['head_ns']:.0f} ns/op)")
    if not rows:
        print('::error::no benchmark was measured at both commits')
        return 1
    return 1 if regressed else 0


if __name__ == '__main__':
    sys.exit(main())
