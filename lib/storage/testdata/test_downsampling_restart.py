#!/usr/bin/env python3
"""检验重启一致性测试能够拒绝响应缺失、部分响应和任意样本变化。"""

import copy
import pathlib
import sys
import unittest

sys.dont_write_bytecode = True
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from downsampling_restart import assert_unchanged, normalize_response


def response_fixture():
    return {"status": "success", "data": {"resultType": "matrix", "result": [
        {"metric": {"__name__": "restart_value", "series": "a"},
         "values": [[299.001, "1"], [599.999, "-0"]]},
        {"metric": {"__name__": "restart_value", "series": "b"},
         "values": [[299.001, "2"], [599.999, "3"]]},
    ]}}


class RestartResponseTests(unittest.TestCase):
    def test_series_and_label_order_do_not_change_snapshot(self):
        before_response = response_fixture()
        after_response = copy.deepcopy(before_response)
        after_response["data"]["result"].reverse()
        for series in after_response["data"]["result"]:
            series["metric"] = dict(reversed(list(series["metric"].items())))
        original = copy.deepcopy(after_response)
        before = normalize_response(before_response)
        after = normalize_response(after_response)
        result = assert_unchanged(before, after, "重新排列时间线")
        self.assertEqual(result["series"], 2)
        self.assertEqual(result["rows"], 4)
        self.assertTrue(result["exact_equal"])
        self.assertEqual(result["before_sha256"], result["after_sha256"])
        self.assertEqual(before[0]["values"], [[299.001, "1"], [599.999, "-0"]])
        self.assertEqual(after_response, original)

    def test_empty_results_cannot_prove_persistence(self):
        for scope in ("all-series", "one-series"):
            with self.subTest(scope=scope):
                response = response_fixture()
                if scope == "all-series":
                    response["data"]["result"] = []
                else:
                    response["data"]["result"][0]["values"] = []
                with self.assertRaises(AssertionError):
                    normalize_response(response)
        with self.assertRaises(AssertionError):
            assert_unchanged([], [], "空快照")

    def test_duplicate_series_are_rejected(self):
        response = response_fixture()
        duplicate = copy.deepcopy(response["data"]["result"][0])
        duplicate["metric"] = dict(reversed(list(duplicate["metric"].items())))
        response["data"]["result"].append(duplicate)
        with self.assertRaises(AssertionError):
            normalize_response(response)

    def test_failed_nonmatrix_and_partial_responses_are_rejected(self):
        for scenario in ("failed", "nonmatrix", "partial-top-level", "partial-data"):
            with self.subTest(scenario=scenario):
                response = response_fixture()
                if scenario == "failed":
                    response["status"] = "error"
                elif scenario == "nonmatrix":
                    response["data"]["resultType"] = "vector"
                elif scenario == "partial-top-level":
                    response["isPartial"] = True
                else:
                    response["data"]["isPartial"] = True
                with self.assertRaises(AssertionError):
                    normalize_response(response)

    def test_nonfinite_or_mistyped_points_are_rejected(self):
        cases = [("value", value) for value in ("NaN", "+Inf", "-Inf", "1e999", 1.0)]
        cases += [("timestamp", value) for value in (float("nan"), float("inf"),
                                                     -float("inf"), True, "299.001")]
        for column, value in cases:
            with self.subTest(column=column, value=value):
                response = response_fixture()
                response["data"]["result"][0]["values"][0][0 if column == "timestamp" else 1] = value
                with self.assertRaises(AssertionError):
                    normalize_response(response)

    def test_duplicate_or_descending_timestamps_are_rejected(self):
        for timestamp in (299.001, 298.999):
            with self.subTest(timestamp=timestamp):
                response = response_fixture()
                response["data"]["result"][0]["values"][1][0] = timestamp
                with self.assertRaises(AssertionError):
                    normalize_response(response)

    def test_labels_timestamps_and_sample_counts_must_match(self):
        before = normalize_response(response_fixture())
        for scenario in ("label-value", "label-name", "timestamp", "point-count", "series-count"):
            with self.subTest(scenario=scenario):
                response = response_fixture()
                first = response["data"]["result"][0]
                if scenario == "label-value":
                    first["metric"]["series"] = "changed"
                elif scenario == "label-name":
                    first["metric"]["new_label"] = first["metric"].pop("series")
                elif scenario == "timestamp":
                    first["values"][0][0] += 0.001
                elif scenario == "point-count":
                    first["values"].pop()
                else:
                    response["data"]["result"].pop()
                with self.assertRaises(AssertionError):
                    assert_unchanged(before, normalize_response(response), scenario)

    def test_value_strings_are_compared_without_tolerance(self):
        before = normalize_response(response_fixture())
        # 微小数值变化、数值字符串格式变化与零的符号变化均必须失败。
        for point_index, value in ((0, "1.000000000001"), (0, "1.0"), (1, "0")):
            with self.subTest(point_index=point_index, value=value):
                response = response_fixture()
                response["data"]["result"][0]["values"][point_index][1] = value
                with self.assertRaises(AssertionError):
                    assert_unchanged(before, normalize_response(response), "原始数值字符串变化")


if __name__ == "__main__":
    unittest.main()
