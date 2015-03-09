__author__ = 'stuart'

import os, sys, inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

from datastreams import DataSet, DataStream
from dictstreams import DictSet, DictStream
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

    def test_read_file(self):
        stream = DataStream.from_file("test_set_1.csv")
        self.assertEqual('name,age,height', next(stream).strip())
        self.assertEqual('carina,27,60', next(stream).strip())
        self.assertEqual('stuart,27,72', next(stream).strip())
        self.assertEqual('gatsby,7,24', next(stream).strip())
        self.assertEqual('john,31,76', next(stream).strip())

    def test_read_csv(self):
        stream = DataStream.from_csv("test_set_1.csv")

        self.assertEqual('carina', next(stream).name)
        self.assertEqual('stuart', next(stream).name)
        self.assertEqual('gatsby', next(stream).name)
        self.assertEqual('john', next(stream).name)

    def test_batch(self):
        stream = DataStream.from_csv("test_set_1.csv")
        batched = stream.batch(2).collect()
        self.assertEqual(len(batched), 2)
        for batch in batched:
            self.assertEqual(len(batch), 2)

    def test_window(self):
        stream = DataStream(range(10))
        windowed = stream.window(3, 2)
        self.assertLessEqual(next(windowed), [0, 1, 2])
        self.assertLessEqual(next(windowed), [2, 3, 4])
        self.assertLessEqual(next(windowed), [4, 5, 6])
        self.assertLessEqual(next(windowed), [6, 7, 8])
        self.assertLessEqual(next(windowed), [8, 9, 10])

    def test_concat_map(self):
        stream = DataStream(xrange(20))
        batched = stream.batch(4)
        concat_mapped = batched.concat_map(
            lambda nums: map(lambda num: num + 1, nums))
        result = list(concat_mapped)
        self.assertListEqual(result, map(lambda num: num + 1, range(20)))

    def test_set(self):
        class Brad(object):
            def __init__(self, name, height, age):
                self.name = name
                self.height = height
                self.age = age

        stream = DataStream([Brad('b-rad', 72, 21)]) \
            .set('name', lambda row: 'brad')\
            .set('height', lambda row: 70) \
            .set('age', lambda row: 30)

        brad = next(stream)
        self.assertEqual(brad.height, 70)
        self.assertEqual(brad.name, 'brad')
        self.assertEqual(brad.age, 30)


class DictStreamTests(unittest.TestCase):

    def test_dicstream_set(self):
        stream = DictStream([{'name': 'brad', 'age': 25}])\
            .set('height', lambda row: 70)\
            .set('age', lambda row: 30)

        brad = next(stream)
        self.assertEqual(brad['height'], 70)
        self.assertEqual(brad['name'], 'brad')
        self.assertEqual(brad['age'], 30)

if __name__ == '__main__':
    unittest.main()
