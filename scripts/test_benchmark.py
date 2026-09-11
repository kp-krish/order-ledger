import unittest

from scripts.benchmark import percentile


class BenchmarkStatisticsTest(unittest.TestCase):

    def test_percentile_interpolates_ordered_sample(self):
        self.assertEqual(percentile([4.0, 1.0, 3.0, 2.0], 0.50), 2.5)
        self.assertAlmostEqual(percentile([1.0, 2.0, 3.0, 4.0], 0.99), 3.97)

    def test_percentile_rejects_empty_sample(self):
        with self.assertRaises(ValueError):
            percentile([], 0.50)


if __name__ == "__main__":
    unittest.main()
