__author__ = 'stuart'

import os, sys, inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

from datastreams import DataSet, DataStream, Datum, DictSet, DictStream
if sys.version_info[0] == 2 and sys.version_info[1] < 7:
    import unittest2 as unittest
else:
    import unittest
try:
    reduce
except:
    from functools import reduce


class JoinTests(unittest.TestCase):

    def test_inner_join(self):
        left = DataSet.from_csv("test_set_1.csv")
        right = DataSet.from_csv("test_set_2.csv")
        joined = left.join('inner', 'name', right)

        self.assertIn('stuart', joined.map(lambda entity: entity.name))
        self.assertEqual(2, sum(joined.map(lambda entity: entity.name == 'gatsby')))
        self.assertNotIn('max', joined.map(lambda entity: entity.name))
        self.assertNotIn('john', joined.map(lambda entity: entity.name))

    def test_outer_join(self):
        left = DataSet.from_csv("test_set_1.csv")
        right = DataSet.from_csv("test_set_2.csv")
        joined = left.join('outer', 'name', right)

        self.assertIn('max', joined.map(lambda entity: entity.name))
        self.assertEqual(2, sum(joined.map(lambda entity: entity.name == 'gatsby')))
        self.assertEqual(1, sum(joined.map(lambda entity: entity.name == 'max')))
        self.assertIn('max', joined.map(lambda entity: entity.name))
        self.assertIn('john', joined.map(lambda entity: entity.name))

    def test_left_join(self):
        left = DataSet.from_csv("test_set_1.csv")
        right = DataSet.from_csv("test_set_2.csv")
        joined = left.join('left', 'name', right)

        self.assertEqual(2, sum(joined.map(lambda entity: entity.name == 'gatsby')))
        self.assertEqual(0, sum(joined.map(lambda entity: entity.name == 'max')))
        self.assertNotIn('max', joined.map(lambda entity: entity.name))
        self.assertIn('john', joined.map(lambda entity: entity.name))

    def test_right_join(self):
        left = DataSet.from_csv("test_set_1.csv")
        right = DataSet.from_csv("test_set_2.csv")
        joined = left.join('right', 'name', right)

        self.assertEqual(2, sum(joined.map(lambda entity: entity.name == 'gatsby')))
        self.assertEqual(1, sum(joined.map(lambda entity: entity.name == 'max')))
        self.assertIn('max', joined.map(lambda entity: entity.name))
        self.assertNotIn('john', joined.map(lambda entity: entity.name))

    def test_group_by(self):
        stream = DataStream.from_csv("test_set_2.csv")
        grouped = stream.group_by('name')
        groupdict = dict(grouped)

        self.assertEqual(2, len(groupdict['gatsby']))
        self.assertEqual(1, len(groupdict['carina']))


class StreamTests(unittest.TestCase):

    def test_map(self):
        stream = DataStream(range(10))
        inced = stream.map(lambda num: num + 1)
        self.assertEqual(1, next(inced))
        self.assertEqual(2, next(inced))
        self.assertEqual(3, next(inced))
        self.assertEqual(4, next(inced))
        self.assertEqual(5, next(inced))

    def test_map_builtin(self):
        stream = DataStream(range(10))
        updated = list(map(lambda num: num + 1, stream))
        self.assertEqual(len(updated), 10)
        self.assertEqual(updated[0], 1)
        self.assertEqual(updated[1], 2)
        self.assertEqual(updated[2], 3)

    def test_filter(self):
        stream = DataStream(range(14))
        odds = stream.filter(lambda num: num % 2)
        self.assertEqual(next(odds), 1)
        self.assertEqual(next(odds), 3)
        self.assertEqual(next(odds), 5)

        posset = DataStream(range(10)).collect()
        negs = set(posset.filter(lambda num: num < 0))
        self.assertEqual(len(negs), 0)
        doubled = list(posset.map(lambda num: num * 2))
        self.assertEqual(len(doubled), len(posset))

    def test_filters(self):
        stream = DataStream(range(14))
        odd_filter = lambda num: num % 2 != 0
        gt5filter = lambda num: num > 5
        filtered = list(stream.filters([odd_filter, gt5filter]))
        self.assertListEqual(filtered, [7, 9, 11, 13])

    def test_filter_builtin(self):
        stream = DataStream(range(14))
        odds = list(filter(lambda num: num % 2, stream))
        self.assertEqual(odds[0], 1)
        self.assertEqual(odds[1], 3)
        self.assertEqual(odds[2], 5)

    def test_reduce(self):
        stream = DataStream(range(5))
        factorials = stream.map(lambda num: num + 1)\
            .reduce(lambda facts, num: facts * num)
        self.assertEqual(factorials, 5*4*3*2*1)

    def test_reduce_to_dataset(self):
        stream = DataStream(range(5))

        def filtreducer(agg, row):
            if row % 2 == 0:
                return agg + [row]
            return agg
        ghetto_filtered = stream.reduce(filtreducer, [])
        self.assertEqual(ghetto_filtered[0], 0)
        self.assertEqual(ghetto_filtered[1], 2)
        self.assertEqual(ghetto_filtered[2], 4)

    def test_reduce_builtin(self):
        stream = DataStream(range(5))
        summed = reduce(lambda a, b: a + b, stream, 0)
        self.assertEqual(summed, sum(range(5)))

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
        batched = DataStream(range(9)).batch(2)
        self.assertSequenceEqual(next(batched), [0, 1])
        self.assertSequenceEqual(next(batched), [2, 3])
        self.assertSequenceEqual(next(batched), [4, 5])
        self.assertSequenceEqual(next(batched), [6, 7])
        self.assertSequenceEqual(next(batched), [8])

        batched = DataStream(range(4)).batch(2).to_list()
        self.assertEqual(len(batched), 2)

    def test_window(self):
        stream = DataStream(range(10))
        windowed = stream.window(3, 2)
        self.assertSequenceEqual(next(windowed).to_list(), [0, 1, 2])
        self.assertSequenceEqual(next(windowed).to_list(), [2, 3, 4])
        self.assertSequenceEqual(next(windowed).to_list(), [4, 5, 6])
        self.assertSequenceEqual(next(windowed).to_list(), [6, 7, 8])
        self.assertSequenceEqual(next(windowed).to_list(), [8, 9])

    def test_concat(self):
        stream = DataStream([[], [1], [2, 3]])
        flattened = stream.concat()
        self.assertEqual(next(flattened), 1)
        self.assertEqual(next(flattened), 2)
        self.assertEqual(next(flattened), 3)

    def test_concat_map(self):
        stream = DataStream(range(20))
        batched = stream.batch(4)
        concat_mapped = batched.concat_map(
            lambda nums: map(lambda num: num + 1, nums))
        result = list(concat_mapped)
        self.assertSequenceEqual(result, list(map(lambda num: num + 1, range(20))))

    def test_for_each(self):
        stream = DataStream(range(20))
        not_changed = stream.for_each(lambda num: num + 1)
        self.assertEqual(next(not_changed), 0)
        self.assertEqual(next(not_changed), 1)
        self.assertEqual(next(not_changed), 2)

    def test_take_now(self):
        stream = DataStream(range(13))
        not_iter = stream.take_now(5)
        self.assertEqual(len(not_iter), 5)
        self.assertEqual(not_iter[0], 0)

    def test_drop_take(self):
        stream = DataStream(range(10))
        second_half = stream.drop(5).take(5)
        self.assertEqual(next(second_half), 5)
        self.assertEqual(next(second_half), 6)
        self.assertEqual(next(second_half), 7)
        self.assertEqual(next(second_half), 8)

    def test_count(self):
        n = 20
        self.assertEqual(DataStream(range(n)).count(), n)

    def test_count_frequency(self):
        stream = DataStream("Hello, world!")
        counts = stream.count_frequency()
        self.assertEqual(dict(counts)['l'], 3)
        self.assertEqual(dict(counts)['e'], 1)
        self.assertEqual(dict(counts)['!'], 1)

    def test_to_dict(self):
        stream = DataStream("Hello, world!")
        counts = stream.count_frequency().to_dict()
        self.assertEqual(counts['l'], 3)
        self.assertEqual(counts['e'], 1)
        self.assertEqual(counts['!'], 1)

    def test_to_list(self):
        stream = DataStream(range(20))
        streamlist = stream.to_list()
        self.assertListEqual(streamlist, list(range(20)))

    def test_to_set(self):
        stream = DataStream(range(19))
        range10 = stream\
            .map(lambda num: abs(num - 9))\
            .to_set()
        self.assertSetEqual(range10, set(range(10)))

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

    def test_map_method(self):
        class Brad(object):
            def __init__(self, name, height, age):
                self.name = name
                self.height = height
                self.age = age

            def get_name(self, upper):
                return self.name.upper() if upper else self.name

        stream = DataStream([Brad('b-rad', 72, 21)]) \
            .set('name', lambda row: 'brad') \
            .set('height', lambda row: 70) \
            .set('age', lambda row: 30)

        BRAD_NAME = next(stream.map_method('get_name', True))
        self.assertEqual(BRAD_NAME, 'BRAD')

    def test_call(self):
        global _test_call_passed
        _test_call_passed = False

        def make_test_pass(dataset):
            global _test_call_passed
            _test_call_passed = isinstance(dataset, DataSet)

        DataSet(range(10)).call(make_test_pass)
        self.assertTrue(_test_call_passed)
        del _test_call_passed

    def test_execute(self):
        global _test_execute_count
        _test_execute_count = 0

        def inc_execute_count(num):
            global _test_execute_count
            _test_execute_count += 1

        self.assertEqual(_test_execute_count, 0)
        DataStream(range(20))\
            .for_each(inc_execute_count)\
            .execute()
        self.assertEqual(_test_execute_count, 20)
        del _test_execute_count

    def test_pick_attrs(self):
        def test_attrs(obj):
            self.assertIn('b', dir(obj))
            self.assertNotIn('a', dir(obj))

        DataStream([Datum({'a': 1, 'b': 2}), Datum({'bbb': 0, 'b': 5})])\
            .pick_attrs(['b'])\
            .for_each(test_attrs)\
            .execute()

    def test_dedupe(self):
        stream = DataStream([[0, 1], [0, 2], [1, 1]])
        deduped = stream.dedupe(lambda row: row[0])
        self.assertSequenceEqual([[0, 1], [1, 1]], deduped.to_list())

    def test_sample(self):
        stream = DataStream(range(100))
        sampled = stream.sample(0.5, 5).to_list()
        self.assertEqual(len(sampled), 5)

    def test_slots_set(self):
        class Thing(object):
            __slots__ = ['name', 'age']

            def __init__(self, name, age):
                self.name = name
                self.age = age

        class Other(object):
            __slots__ = ['name', 'weight']

            def __init__(self, name, weight):
                self.name = name
                self.weight = weight

        things = DataStream([Thing('brad', 24), Thing('alice', 54)])
        others = DataStream([Other('brad', 170), Other('angela', 115)])
        other_things = things.join('inner', 'name', others).set('age', value=20)
        self.assertEqual(next(other_things).age, 20)


class DataSetTests(unittest.TestCase):
    def test_map(self):
        stream2 = DataSet(range(10)) \
            .take_now(5) \
            .map(lambda num: num + 1)
        self.assertEqual(1, next(stream2))
        self.assertEqual(2, next(stream2))
        self.assertEqual(3, next(stream2))
        self.assertEqual(4, next(stream2))
        self.assertEqual(5, next(stream2))


class FilterRadixTests(unittest.TestCase):

    def test_radix_eq(self):
        stream = DataStream(range(10))
        just_three = stream.where('real').eq(3)
        self.assertListEqual(list(just_three), [3])

    def test_radix_is_in(self):
        stream = DataStream(range(20))
        some_primes = [1, 2, 3, 5, 7]
        those_primes = list(stream.where('real').is_in(some_primes))
        self.assertListEqual(those_primes, some_primes)


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
