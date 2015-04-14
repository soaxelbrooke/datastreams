__author__ = 'stuart'

from datastreams import DataStream
from datastreams import JoinedObject
from itertools import product


class RddStream(DataStream):

    def __init__(self, source_rdd):
        self._source = source_rdd

    @staticmethod
    def Stream(rdd):
        return RddStream(rdd)

    def map(self, function):
        return self.Stream(self._source.map(function))

    def chain(self):
        return self.Stream(self._source.flatMap(lambda x: x))

    def concat_map(self, function):
        return self.Stream(self._source.flatMap(function))

    def filter(self, filter_fn):
        return self.Stream(self._source.filter(filter_fn))

    def collect(self):
        return self._source.collect()

    def take(self, n):
        return self.Stream(self._source.take(n))

    def take_now(self, n):
        return self.take(n)

    def drop(self, n):
        raise NotImplementedError("Spark does not implement drop currently")

    def execute(self):
        self.collect()

    def batch(self, batch_size):
        raise NotImplementedError("Spark does not implement batching, try using"
                                  " the probably non existent"
                                  " StreamingRddStream!")

    def window(self, length, interval):
        raise NotImplementedError("Spark does not implement windowing, try "
                                  "using the probably non existent"
                                  " StreamingRddStream!")

    def group_by_fn(self, key_fn):
        return self.Stream(self._source.groupBy(key_fn))

    def count_frequency(self):
        return self.Stream(self._source.countByValue())

    def apply(self, function):
        return self.Stream(function(self))

    def call(self, function):
        function(self)

    def sort_by(self, key_fn, descending=True):
        return self.Stream(self._source.sortByKey(
            ascending=not descending, keyfunc=key_fn))

    def dedupe(self):
        return self.Stream(self._source.distinct())

    def reverse(self):
        return self.Stream(self._source.takeOrdered(len(self), key=lambda x: -x))

    @staticmethod
    def combine_joined(joined):
        def product_pairs(group_pair):
            groupa = group_pair[0] or [None]
            groupb = group_pair[1] or [None]
            return product(groupa, groupb)

        return joined \
            .map(lambda kvpair: kvpair[1]) \
            .flatMap(product_pairs) \
            .map(lambda pair: JoinedObject(pair[0], pair[1]))

    def left_join_by(self, left_key_fn, right_key_fn, right):
        right_grouped = right._source.groupBy(right_key_fn)
        left_grouped = self._source.groupBy(left_key_fn)
        results = self.combine_joined(left_grouped.leftOuterJoin(right_grouped))
        return self.Stream(results)

    def right_join_by(self, left_key_fn, right_key_fn, right):
        right_grouped = right._source.groupBy(right_key_fn)
        left_grouped = self._source.groupBy(left_key_fn)
        results = self.combine_joined(left_grouped.rightOuterJoin(right_grouped))
        return self.Stream(results)

    def inner_join_by(self, left_key_fn, right_key_fn, right):
        right_grouped = right._source.groupBy(right_key_fn)
        left_grouped = self._source.groupBy(left_key_fn)
        results = self.combine_joined(left_grouped.join(right_grouped))
        return self.Stream(results)

    def outer_join_by(self, left_key_fn, right_key_fn, right):
        raise NotImplementedError
