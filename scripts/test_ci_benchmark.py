"""The benchmark gate fails a real slowdown and lets noise through."""
import unittest

import ci_benchmark

OUTPUT = """goos: linux
BenchmarkTranslate/mysql-4         	     100	   1000.5 ns/op	  64 B/op	 1 allocs/op
BenchmarkOpen 	     100	   2000 ns/op
PASS
"""


class ParseTests(unittest.TestCase):
    def test_strips_the_procs_suffix_and_reads_ns(self):
        self.assertEqual(ci_benchmark.parse(OUTPUT), {
            'BenchmarkTranslate/mysql': [1000.5],
            'BenchmarkOpen': [2000.0],
        })

    def test_names_a_benchmark_after_its_package_outside_the_root(self):
        self.assertEqual(ci_benchmark.parse(OUTPUT, './internal/parser/ach'), {
            'internal/parser/ach.BenchmarkTranslate/mysql': [1000.5],
            'internal/parser/ach.BenchmarkOpen': [2000.0],
        })


class GateTests(unittest.TestCase):
    def test_consistent_slowdown_fails(self):
        rows = ci_benchmark.gate({'B': [100, 101, 99]}, {'B': [140, 141, 139]}, 1.25)
        self.assertTrue(rows[0]['regressed'])

    def test_slowdown_within_the_ratio_passes(self):
        rows = ci_benchmark.gate({'B': [100, 101, 99]}, {'B': [120, 121, 119]}, 1.25)
        self.assertFalse(rows[0]['regressed'])

    def test_noisy_median_with_a_fast_sample_passes(self):
        # The median is 1.5x, but one head sample is as fast as the base.
        rows = ci_benchmark.gate({'B': [100, 100, 100]}, {'B': [95, 150, 160]}, 1.25)
        self.assertFalse(rows[0]['regressed'])

    def test_benchmark_on_one_side_only_is_ignored(self):
        self.assertEqual(ci_benchmark.gate({'Old': [1]}, {'New': [1]}, 1.25), [])

    def test_rows_are_sorted_worst_first(self):
        rows = ci_benchmark.gate({'A': [100], 'B': [100]}, {'A': [110], 'B': [90]}, 1.25)
        self.assertEqual([row['benchmark'] for row in rows], ['A', 'B'])


if __name__ == '__main__':
    unittest.main()
