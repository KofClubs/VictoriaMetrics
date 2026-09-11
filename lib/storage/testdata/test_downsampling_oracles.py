#!/usr/bin/env python3
"""使用手算结果检验 E2E 输入、五特征参考值和查询求值网格。"""

import pathlib
import sys
import unittest

sys.dont_write_bytecode = True
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import downsampling_compare as single
import downsampling_multiseries_compare as multi


class InputTests(unittest.TestCase):
    def assert_cadence(self, points):
        timestamps = sorted(timestamp for timestamp, _ in points)
        self.assertEqual(len(timestamps), len(set(timestamps)))
        deltas = {right - left for left, right in zip(timestamps, timestamps[1:])}
        self.assertEqual(deltas, {14_000, 15_000, 16_000})

    def test_three_hour_fixture(self):
        phases = single.fixture(0)
        self.assertEqual([len(rows) for rows in phases], [480, 480, 480])
        rows = [row for phase in phases for row in phase]
        self.assertEqual({row["metric"] for row in rows}, {single.METRIC, single.CONTROL_METRIC})
        for metric in (single.METRIC, single.CONTROL_METRIC):
            points = [(row["timestamp"], row["value"]) for row in rows if row["metric"] == metric]
            self.assertEqual(len(points), 720)
            self.assert_cadence(points)
            self.assertEqual(min(timestamp for timestamp, _ in points), 0)
            self.assertEqual(max(timestamp for timestamp, _ in points), 10_784_000)
            self.assertTrue(any(timestamp % 300_000 == 0 for timestamp, _ in points))
            self.assertTrue(any(timestamp == 3_600_000 for timestamp, _ in points))
        self.assertTrue(all(3_600_000 <= row["timestamp"] < 7_200_000 for row in phases[2]))
        self.assertLess(max(row["timestamp"] for row in phases[2]), min(row["timestamp"] for row in phases[1]))

    def test_long_dense_and_sparse_input(self):
        labels = multi.make_labels(8, 1)
        for index in (0, 4):
            main = multi.rows_for_series(index, labels[index], 0, 0, 1, 93)
            late = multi.rows_for_series(index, labels[index], 0, 0, 1, 93, late=True)
            self.assertEqual(len(late), 3)
            self.assertFalse({timestamp for timestamp, _ in main} & {timestamp for timestamp, _ in late})
            self.assertEqual(len(main) + len(late), 5760 if index == 0 else 21)
            self.assert_cadence(main + late)

    def test_dense_day_and_batch_boundaries(self):
        labels = multi.make_labels(8, 1)[0]
        points = []
        for first, last in ((0, 2), (2, 3)):
            points.extend(multi.rows_for_series(0, labels, 0, first, last, 3))
        points.extend(multi.rows_for_series(0, labels, 0, 0, 3, 3, late=True))
        self.assertEqual(len(points), 3 * 5760)
        self.assert_cadence(points)

    def test_sparse_activity_windows(self):
        expected = {"daily": list(range(8)), "gapped": [0, 1, 3, 4, 6, 7],
                    "early": list(range(5)), "late": list(range(2, 8))}
        for pattern, days in expected.items():
            actual = [day for day in range(8) if multi.active_day(pattern, day, 8)]
            self.assertEqual(actual, days)

    def test_single_series_multiple_blocks_and_labels(self):
        labels = multi.make_labels(160, 4)
        self.assertEqual(len({multi.labels_key(item) for item in labels}), 160)
        self.assertEqual(len({item["job"] for item in labels}), 7)
        self.assertEqual(len({item["instance"] for item in labels}), 13)
        # 31 日的持续采样覆盖 8928 个 5m 格子，超过单 Block 的 8192 行。
        points = multi.rows_for_series(0, labels[0], 0, 0, 31, 93)
        points.extend(multi.rows_for_series(0, labels[0], 0, 0, 31, 93, late=True))
        self.assertEqual(len(points), 178_560)
        self.assertEqual(len({timestamp // 300_000 for timestamp, _ in points}), 8928)
        self.assertEqual(len({timestamp // 3_600_000 for timestamp, _ in points}), 744)


class OracleTests(unittest.TestCase):
    def setUp(self):
        # 边界点明确归入后一个格子；最后一个样本不必是最大值。
        self.points = [(-1, -9.0), (0, 4.0), (14_000, -2.0), (299_999, 7.0),
                       (300_000, 10.0), (3_599_999, -5.0), (3_600_000, 6.0), (7_199_999, -1.0)]
        self.rows = [{"metric": single.METRIC, "timestamp": timestamp, "value": value}
                     for timestamp, value in reversed(self.points)]
        self.key = multi.labels_key({"__name__": single.METRIC, "series_id": "test"})

    def assert_both_oracles(self, resolution, expected):
        short = single.aggregate(self.rows, single.METRIC, resolution)
        actual = [(row["timestamp"], tuple(row[feature] for feature in single.FEATURES)) for row in short]
        self.assertEqual(actual, expected)
        self.assertEqual(multi.aggregate_map({self.key: list(reversed(self.points))}, resolution), {self.key: expected})

    def test_five_minute_features_and_boundary(self):
        self.assert_both_oracles(300_000, [
            (-1, (-9.0, -9.0, 1.0, -9.0, -9.0)),
            (299_999, (7.0, 9.0, 3.0, -2.0, 7.0)),
            (300_000, (10.0, 10.0, 1.0, 10.0, 10.0)),
            (3_599_999, (-5.0, -5.0, 1.0, -5.0, -5.0)),
            (3_600_000, (6.0, 6.0, 1.0, 6.0, 6.0)),
            (7_199_999, (-1.0, -1.0, 1.0, -1.0, -1.0))])

    def test_hour_features_and_shared_last_timestamp(self):
        self.assert_both_oracles(3_600_000, [
            (-1, (-9.0, -9.0, 1.0, -9.0, -9.0)),
            (3_599_999, (-5.0, 14.0, 5.0, -5.0, 10.0)),
            (7_199_999, (-1.0, 5.0, 2.0, -1.0, 6.0))])

    def test_actual_fixture_has_hand_calculated_first_buckets(self):
        rows = [row for phase in single.fixture(0) for row in phase]
        self.assertEqual(single.aggregate(rows, single.METRIC, 300_000)[0],
                         {"timestamp": 299_000, "last": -3.5, "sum": -99.75,
                          "count": 21.0, "min": -6.0, "max": -3.5})
        self.assertEqual(single.aggregate(rows, single.METRIC, 3_600_000)[0],
                         {"timestamp": 3_584_000, "last": -0.375, "sum": -146.625,
                          "count": 240.0, "min": -6.0, "max": 6.0})
        self.assertEqual(single.aggregate(rows, single.CONTROL_METRIC, 300_000)[0],
                         {"timestamp": 299_000, "last": 96.75, "sum": 2005.5,
                          "count": 21.0, "min": 94.25, "max": 96.75})
        for duration in single.RESOLUTIONS.values():
            points = single.aggregate(rows, single.METRIC, duration)
            self.assertEqual(sum(row["count"] for row in points), 720.0)

    def test_counts_accumulate_contributions_from_unequal_batches(self):
        rows = [{"metric": single.METRIC, "timestamp": timestamp, "value": value}
                for timestamp, value in [(0, 2.0), (14_000, -3.0), (29_000, 6.0), (45_000, 8.0), (59_000, -1.0)]]
        first = single.aggregate(rows[:3], single.METRIC, 300_000)[0]
        second = single.aggregate(rows[3:], single.METRIC, 300_000)[0]
        total = single.aggregate(rows, single.METRIC, 300_000)[0]
        self.assertEqual((first["count"], second["count"]), (3.0, 2.0))
        self.assertEqual(total, {"timestamp": 59_000, "last": -1.0, "sum": 12.0,
                                 "count": 5.0, "min": -3.0, "max": 8.0})
        self.assertEqual(first["count"] + second["count"], total["count"])
        self.assertNotEqual(len((first, second)), total["count"])

    def test_range_uses_evaluation_timestamp(self):
        points = [(284_000, 7.0), (584_000, 9.0)]
        self.assertEqual(single.range_expected(points, 299_999, 899_999, 300_000),
                         [(299_999, 7.0), (599_999, 9.0)])

    def test_range_left_open_right_closed_and_empty_windows(self):
        self.assertEqual(single.range_expected([(0, 5.0)], 300_000, 600_000, 300_000), [])
        self.assertEqual(single.range_expected([(300_000, 7.0)], 300_000, 600_000, 300_000), [(300_000, 7.0)])
        self.assertEqual(single.range_expected([], 0, 900_000, 300_000), [])
        self.assertEqual(single.range_expected([(3_584_000, -2.0)], 3_599_999, 7_199_999, 3_600_000),
                         [(3_599_999, -2.0)])

    def test_matrix_clips_only_shared_timestamp(self):
        data = {self.key: [(299_000, (7.0, 9.0, 3.0, -2.0, 7.0))]}
        self.assertEqual(multi.filter_points(data, 14_000, 298_999, feature_index=1), {})
        self.assertEqual(multi.filter_points(data, 14_000, 299_000, feature_index=1), {self.key: [(299_000, 9.0)]})
        self.assertEqual(multi.filter_points(data, 0, 300_000, selected=set(), feature_index=1), {})


if __name__ == "__main__":
    unittest.main()
