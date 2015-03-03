__author__ = 'stuart'

import os, sys, inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

from datastreams import DataSet, DataStream
import unittest


class JoinTests(unittest.TestCase):

    def test_inner_join(self):
        left = DataSet.from_csv("test_set_1.csv")
        right = DataSet.from_csv("test_set_2.csv")
        joined = left.join('inner', 'name', right)

        self.assertIn('stuart', joined.map(lambda entity: entity.name))
        self.assertEqual(2, sum(joined.map(lambda entity: entity.name == 'gatsby')))
        self.assertNotIn('max', joined.map(lambda entity: entity.name))
        self.assertNotIn('john', joined.map(lambda entity: entity.name))


class StreamTests(unittest.TestCase):

    def test_read_csv(self):
        stream = DataStream.from_csv("test_set_1.csv")

        self.assertEqual('carina', next(stream).name)
        self.assertEqual('stuart', next(stream).name)
        self.assertEqual('gatsby', next(stream).name)
        self.assertEqual('john', next(stream).name)

if __name__ == '__main__':
    unittest.main()
