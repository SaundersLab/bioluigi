import unittest

from bioluigi.utils import structure_apply, value_it


class Test_structure_apply(unittest.TestCase):
    def test_dict(self):
        s = {'x': 'abc', 'y': 'hello'}
        self.assertEqual(structure_apply(len, s), {'x': 3, 'y': 5})

    def test_list(self):
        s = ['abc', 'hello']
        self.assertEqual(structure_apply(len, s), [3, 5])

    def test_singleton(self):
        s = 'hello'
        self.assertEqual(structure_apply(len, s), 5)


class Test_value_it(unittest.TestCase):
    def test_dict(self):
        s = {'x': 'abc', 'y': 'hello'}
        self.assertEqual(list(value_it(s)), list(s.values()))

    def test_list(self):
        s = ['abc', 'hello']
        self.assertEqual(value_it(s), s)
