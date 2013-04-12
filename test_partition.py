# coding: utf-8

import unittest

from upload_to_pypln import partition


class TestPartition(unittest.TestCase):
    def test_less_than_one_iteration(self):
        result = list(partition([1, 2, 3, 4, 5], 10))
        expected = [[1, 2, 3, 4, 5]]
        self.assertEqual(result, expected)

    def test_one_iteration(self):
        result = list(partition([1, 2, 3, 4, 5], 5))
        expected = [[1, 2, 3, 4, 5]]
        self.assertEqual(result, expected)

    def test_more_than_one_iteration(self):
        result = list(partition([1, 2, 3, 4, 5, 6], 3))
        expected = [[1, 2, 3], [4, 5, 6]]
        self.assertEqual(result, expected)

    def test_more_than_one_iteration_with_rest(self):
        result = list(partition([1, 2, 3, 4, 5, 6, 7], 3))
        expected = [[1, 2, 3], [4, 5, 6], [7]]
        self.assertEqual(result, expected)
