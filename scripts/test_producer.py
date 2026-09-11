import unittest

from scripts.producer import build_workload


class ProducerWorkloadTest(unittest.TestCase):

    def test_includes_exact_duplicate_event_ids(self):
        records = build_workload(orders=2, duplicates=3, out_of_order=0, seed=7)

        original_ids = [event["eventId"] for _, event in records[:4]]
        duplicate_ids = [event["eventId"] for _, event in records[4:]]

        self.assertEqual(duplicate_ids, original_ids[:3])

    def test_emits_one_three_two_for_out_of_order_orders(self):
        records = build_workload(orders=1, duplicates=0, out_of_order=1, seed=7)

        self.assertEqual([event["sequence"] for _, event in records], [1, 3, 2])
        self.assertEqual(len({key for key, _ in records}), 1)

    def test_rejects_more_out_of_order_orders_than_total_orders(self):
        with self.assertRaises(ValueError):
            build_workload(orders=1, duplicates=0, out_of_order=2, seed=7)


if __name__ == "__main__":
    unittest.main()

