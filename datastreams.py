from itertools import imap, islice
from object_join import JoinedObject
import csv
from copy import copy
from collections import defaultdict, deque


class Datum(object):
    def __init__(self, attributes):
        for name, value in attributes:
            setattr(self, name, value)


class DataStream(object):
    def __init__(self, source):
        self._source = iter(source)
        self._transform = lambda row: row
        self._predicate = lambda row: True

    def __iter__(self):
        return (self._transform(row) for row in self._source
                if self._predicate(row))

    def __repr__(self):
        return str(list(self._source))

    def __add__(self, other):
        return DataSet(self._source + other._source)

    def next(self):
        while True:
            src_next = next(self._source)
            if self._predicate(src_next):
                return self._transform(src_next)

    def reduce(self, function, initial):
        return DataSet(reduce(function, self, initial))

    def map(self, function):
        self._transform = function
        return DataStream(self)

    def concat(self):
        return DataStream(result for results in self for result in results)

    def concat_map(self, function):
        return self.map(function).concat()

    def filter(self, filter_fn):
        self._predicate = filter_fn
        return DataStream(self)

    def set(self, attr, transfer_func):
        def row_setattr(row):
            new_row = copy(row)
            setattr(new_row, attr, transfer_func(row))
            return new_row
        self._transform = row_setattr
        return DataStream(self)

    def get(self, name, default=None):
        def row_getattr(row):
            return getattr(row, name)
        self._transform = row_getattr
        return DataStream(self)

    def delete(self, key):
        def obj_del(row):
            new_row = copy(row)
            delattr(new_row, key)
            return new_row
        self._transform = obj_del
        return DataStream(self)

    def for_each(self, function):
        def apply_fn(row):
            function(row)
            return row
        self._transform = apply_fn
        return DataStream(self)

    def take(self, n):
        return DataStream(islice(self, 0, n))

    def take_now(self, n):
        return DataSet([next(self) for _ in range(n)])

    def drop(self, n):
        return DataStream(islice(self, n, None))

    def collect(self):
        return DataSet(self)

    def collect_as(self, constructor):
        return DataSet(imap(constructor, self))

    def batch(self, batch_size):
        def batch_iter():
            while True:
                try:
                    yield self.take_now(batch_size)
                except StopIteration:
                    break
        return DataStream(batch_iter())

    def window(self, length, interval):
        queue = deque(maxlen=length)

        def window_iter():
            while True:
                try:
                    queue.extend(self.take_now(interval))
                    yield DataSet(queue)
                except StopIteration:
                    break
        return DataStream(window_iter())

    @staticmethod
    def from_csv(path, headers=None, constructor=Datum):
        source_file = open(path)
        if headers is None:
            headers = [h.strip() for h in source_file.readline().split(",")]
        reader = DataStream.iter_csv(source_file)
        return DataStream(constructor(zip(headers, row)) for row in reader)

    @staticmethod
    def iter_csv(source_file):
        reader = csv.reader(source_file)
        for row in reader:
            yield row
        source_file.close()
        raise StopIteration


class DataSet(DataStream):
    def __init__(self, source):
        super(DataSet, self).__init__(source)
        self._source = list(source)

    def __len__(self):
        return len(self._source)

    def __getitem__(self, item):
        return self._source[item]

    def apply(self, function):
        return DataSet(function(self))

    def reduce_right(self, function, init):
        return DataSet(reduce(function, self, init))

    def join(self, how, key, right):
        """ Returns a dataset joined using keys from right dataset only
        :type how: str
        :type right: DataSet
        :type key: str
        :rtype: DataSet
        """
        if how == 'left':
            return self.left_join(key, right)
        elif how == 'right':
            return self.right_join(key, right)
        elif how == 'inner':
            return self.inner_join(key, right)
        elif how == 'outer':
            return self.outer_join(key, right)
        else:
            raise ValueError("Invalid value for how: {}, must be left, right, "
                             "inner, or outer.".format(str(how)))

    def join_by(self, how, left_key_fn, right_key_fn, right):
        """ Uses two key functions perform a join.  Key functions should produce
        hashable types to be used to compare/index dicts.
        :type how: str
        :type left_key_fn: (object) -> object
        :type right_key_fn: (object) -> object
        :type right: DataSet
        :rtype: DataSet
        """
        raise NotImplementedError

    def left_join(self, key, right):
        """ Returns a dataset joined using keys from right dataset only
        :type right: DataSet
        :type key: str
        :rtype: DataSet
        """
        return self.left_join_by(
            lambda row: getattr(row, key), lambda row: getattr(row, key), right)

    def left_join_by(self, left_key_fn, right_key_fn, right):
        """
        :param left_key_fn:
        :param right_key_fn:
        :param right:
        :return:
        """
        raise NotImplementedError

    def right_join(self, key, right):
        """ Returns a dataset joined using keys in right dataset only
        :type right: DataSet
        :type key: str
        :rtype: DataSet
        """
        raise NotImplementedError

    def inner_join(self, key, right):
        """ Returns a dataset joined using keys in both dataset only
        :type right: DataSet
        :type key: str
        :rtype: DataSet
        """
        joiner = defaultdict(list)
        for ele in right:
            joiner[getattr(ele, key)].append(ele)
        joined = []
        for ele in self:
            for other in joiner[getattr(ele, key)]:
                joined.append(JoinedObject(ele, other))
        return DataSet(joined)

    def outer_join(self, key, right):
        """ Returns a dataset joined using keys in either datasets
        :type right: DataSet
        :type key: str
        :rtype: DataSet
        """
        raise NotImplementedError

    def sort_by(self, key_fn, descending=True):
        return DataStream(sorted(self._source, key=key_fn, reverse=descending))

    def reverse(self):
        return DataStream(element for element in self._source[::-1])

    def stream(self):
        return DataStream(iter(self))

    @staticmethod
    def from_csv(path, headers=None, constructor=Datum):
        return DataSet(DataStream.from_csv(path, headers, constructor))