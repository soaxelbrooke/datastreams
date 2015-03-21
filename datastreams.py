from itertools import imap, islice, ifilter
from object_join import JoinedObject
import csv
from copy import copy
from collections import defaultdict, deque


class Datum(object):
    def __init__(self, attributes):
        for name, value in attributes:
            setattr(self, name, value)


class DataStream(object):

    @staticmethod
    def Stream(iterable):
        return DataStream(iterable)

    @staticmethod
    def Set(iterable):
        return DataSet(iterable)

    def __init__(self, source):
        self._source = iter(source)
        self._transform = lambda row: row
        self._predicate = lambda row: True

    def __iter__(self):
        return imap(self._transform, ifilter(self._predicate, self._source))

    def next(self):
        while True:
            src_next = next(self._source)
            if self._predicate(src_next):
                return self._transform(src_next)

    def reduce(self, function, initial):
        return self.Set(reduce(function, self, initial))

    def map(self, function):
        self._transform = function
        return self.Stream(self)

    def concat(self):
        return self.Stream(result for results in self for result in results)

    def concat_map(self, function):
        return self.map(function).concat()

    def filter(self, filter_fn):
        self._predicate = filter_fn
        return self.Stream(self)

    def set(self, attr, transfer_func):
        def row_setattr(row):
            new_row = copy(row)
            setattr(new_row, attr, transfer_func(row))
            return new_row
        return self.map(row_setattr)

    def get(self, name, default=None):
        def row_getattr(row):
            return getattr(row, name) if hasattr(row, name) else default
        return self.map(row_getattr)

    def delete(self, key):
        def obj_del(row):
            new_row = copy(row)
            delattr(new_row, key)
            return new_row
        return self.map(obj_del)

    def for_each(self, function):
        def apply_fn(row):
            function(row)
            return row
        return self.map(apply_fn)

    def take(self, n):
        return self.Stream(islice(self, 0, n))

    def take_now(self, n):
        return self.Set([next(self) for _ in range(n)])

    def drop(self, n):
        return self.Stream(islice(self, n, None))

    def collect(self):
        return self.Set(self)

    def collect_as(self, constructor):
        return self.map(constructor).collect()

    def batch(self, batch_size):
        return self.window(batch_size, batch_size)

    def window(self, length, interval):
        queue = deque(maxlen=length)

        def window_iter():
            while True:
                try:
                    queue.extend(self.take_now(interval))
                    yield self.Set(queue)
                except StopIteration:
                    break
        return self.Stream(window_iter())

    @classmethod
    def from_file(cls, path):
        source_file = open(path)
        return DataStream(cls.iter_file(source_file))

    @staticmethod
    def iter_file(source_file):
        for line in source_file:
            yield line
        source_file.close()
        raise StopIteration

    @classmethod
    def from_csv(cls, path, headers=None, constructor=Datum):
        source_file = open(path)
        if headers is None:
            headers = [h.strip() for h in source_file.readline().split(",")]
        reader = cls.iter_csv(source_file)
        return cls.Stream(constructor(zip(headers, row)) for row in reader)

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
        return self.Set(function(self))

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
        if how == 'left':
            return self.left_join_by(left_key_fn, right_key_fn, right)
        elif how == 'right':
            return self.right_join_by(left_key_fn, right_key_fn, right)
        elif how == 'inner':
            return self.inner_join_by(left_key_fn, right_key_fn, right)
        elif how == 'outer':
            return self.outer_join_by(left_key_fn, right_key_fn, right)
        else:
            raise ValueError("Invalid value for how: {}, must be left, right, "
                             "inner, or outer.".format(str(how)))

    def left_join(self, key, right):
        """ Returns a dataset joined using keys from right dataset only
        :type right: DataSet
        :type key: str
        :rtype: DataSet
        """
        key_fn = lambda ele: getattr(ele, key)
        return self.left_join_by(key_fn, key_fn, right)

    def left_join_by(self, left_key_fn, right_key_fn, right):
        """ Returns a dataset joined using key functions to evaluate equality
        :type left_key_fn: (object) -> object
        :type right_key_fn: (object) -> object
        :type right: DataSet
        :rtype: DataSet
        """
        joiner = defaultdict(list)
        for ele in right:
            joiner[right_key_fn(ele)].append(ele)
        joined = []
        for ele in self:
            for other in joiner.get(left_key_fn(ele), [None]):
                joined.append(JoinedObject(ele, other))
        return self.Set(joined)

    def right_join(self, key, right):
        """ Returns a dataset joined using keys in right dataset only
        :type right: DataSet
        :type key: str
        :rtype: DataSet
        """
        key_fn = lambda ele: getattr(ele, key)
        return self.right_join_by(key_fn, key_fn, right)

    def right_join_by(self, left_key_fn, right_key_fn, right):
        """ Returns a dataset joined using key functions to evaluate equality
        :type left_key_fn: (object) -> object
        :type right_key_fn: (object) -> object
        :type right: DataSet
        :rtype: DataSet
        """
        joiner = defaultdict(list)
        for ele in self:
            joiner[left_key_fn(ele)].append(ele)
        joined = []
        for ele in right:
            for other in joiner.get(right_key_fn(ele), [None]):
                joined.append(JoinedObject(ele, other))
        return self.Set(joined)

    def inner_join(self, key, right):
        """ Returns a dataset joined using keys in both dataset only
        :type right: DataSet
        :type key: str
        :rtype: DataSet
        """
        key_fn = lambda ele: getattr(ele, key)
        return self.inner_join_by(key_fn, key_fn, right)

    def inner_join_by(self, left_key_fn, right_key_fn, right):
        """ Returns a dataset joined using key functions to evaluate equality
        :type left_key_fn: (object) -> object
        :type right_key_fn: (object) -> object
        :type right: DataSet
        :rtype: DataSet
        """
        joiner = defaultdict(list)
        for ele in right:
            joiner[right_key_fn(ele)].append(ele)
        joined = []
        for ele in self:
            for other in joiner[left_key_fn(ele)]:
                joined.append(JoinedObject(ele, other))
        return self.Set(joined)

    def outer_join(self, key, right):
        """ Returns a dataset joined using keys in either datasets
        :type right: DataSet
        :type key: str
        :rtype: DataSet
        """
        key_fn = lambda ele: getattr(ele, key)
        return self.outer_join_by(key_fn, key_fn, right)

    def outer_join_by(self, left_key_fn, right_key_fn, right):
        """ Returns a dataset joined using key functions to evaluate equality
        :type left_key_fn: (object) -> object
        :type right_key_fn: (object) -> object
        :type right: DataSet
        :rtype: DataSet
        """
        left_joiner = defaultdict(list)
        for ele in self:
            left_joiner[left_key_fn(ele)].append(ele)
        right_joiner = defaultdict(list)
        for ele in right:
            right_joiner[right_key_fn(ele)].append(ele)
        keys = set(left_joiner.keys() + right_joiner.keys())

        def iter_join(l, r, join_keys):
            for join_key in join_keys:
                for ele in l.get(join_key, [None]):
                    for other in r.get(join_key, [None]):
                        yield JoinedObject(ele, other)

        return self.Set(iter_join(left_joiner, right_joiner, keys))

    def sort_by(self, key_fn, descending=True):
        return self.Stream(sorted(self._source, key=key_fn, reverse=descending))

    def reverse(self):
        return self.Stream(element for element in self._source[::-1])

    def to_stream(self):
        return self.Stream(iter(self))

    @classmethod
    def from_csv(cls, path, headers=None, constructor=Datum):
        return cls.Set(DataStream.from_csv(path, headers, constructor))