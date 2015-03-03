from itertools import imap, islice
from collections import defaultdict
import csv
from copy import copy


class DataStream(object):
    def __init__(self, source):
        self._source = source
        self._transform = lambda row: row
        self._predicate = lambda row: True

    def __iter__(self):
        return (self._transform(row) for row in self._source
                if self._predicate(row))

    def __repr__(self):
        return str(list(self._source))

    def __add__(self, other):
        return DataSet(self._source + other._source)

    def reduce(self, function, initial):
        return DataSet(reduce(function, self, initial))

    def map(self, function):
        self._transform = function
        return DataStream(self)

    def filter(self, filter_fn):
        self._predicate = filter_fn
        return DataStream(self)

    def set(self, attr, transfer_func):
        def rowsetattr(row):
            newrow = copy(row)
            setattr(newrow, attr, transfer_func(row))
            return newrow
        self._transform = rowsetattr
        return DataStream(self)

    def get(self, name, default=None):
        def rowgetattr(row):
            return getattr(row, name)
        self._transform = rowgetattr
        return DataStream(self)

    def delete(self, key):
        def objdel(row):
            newrow = copy(row)
            delattr(newrow, key)
            return newrow
        self._transform = objdel
        return DataStream(self)

    def take(self, n):
        return DataStream(islice(self, 0, n))

    def drop(self, n):
        return DataStream(islice(self, n, None))

    def collect(self):
        return DataSet(self)
    
    def collectas(self, constructor):
        return DataSet(imap(constructor, self))

    @staticmethod
    def _object_join(a, b):
        ab = type(a.__class__.__name__ + b.__class__.__name__, 
                  (a.__class__, b.__class__, object), 
                  dict(b.__dict__.items() + a.__dict__.items()))
        return ab.__new__(ab)

    @staticmethod
    def fromcsv(path, headers=None, headers_from_file=True, constructor=dict):
        with open(path) as source_file:
            if headers_from_file:
                headers = [h.strip() for h in source_file.readline().split(",")]
            if constructor == dict:
                reader = csv.DictReader(source_file, headers)
            else:
                reader = (constructor(line) for line in source_file)
            return DataSet(row for row in reader)


class JoinedOjbect(object):
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def __getattr__(self, attr):
        if attr == 'left': 
            return self.left
        elif attr == 'right':
            return self.right
        else:
            return self.get_from_sources(attr)

    def get_from_sources(self, attr):
        if hasattr(self.left, attr):
            return getattr(self.left, attr)
        elif hasattr(self.right, attr):
            return getattr(self.right, attr)
        else:
            raise AttributeError("Neither of joined object's parents have "
                "attribute '{}'".format(attr))

class DataSet(DataStream):
    def __init__(self, source):
        super(DataSet, self).__init__(source)
        self._source = list(source)

    def __len__(self):
        return len(self._source)

    def __getitem__(self, item):
        return self._source[item]

    def reduceright(self, function, init):
        return DataSet(reduce(function, self, init))

    def join(self, other, key):
        joiner = {getattr(ele, key): ele for ele in other}
        joined = (JoinedOjbect(ele, joiner[getattr(ele, key)]) for ele in self)
        return DataSet(joined)

    def sortby(self, key_fn, descending=True):
        return DataStream(sorted(self._source, key=key_fn, reverse=descending))

    def reverse(self):
        return DataStream(element for element in self._source[::-1])

    def stream(self):
        return DataStream(iter(self))

