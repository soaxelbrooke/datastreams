from itertools import imap, islice
from collections import defaultdict
import csv


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

    def set(self, key, transfer_func):
        def rowset(row):
            row[key] = transfer_func(row)
            return row
        self._transform = rowset
        return DataStream(self)

    def setattr(self, attr, transfer_func):
        def rowsetattr(row):
            setattr(row, attr, transfer_func(row))
            return row
        self._transform = rowsetattr
        return DataStream(self)

    def get(self, key):
        def rowget(row):
            return row.get(key)
        self._transform = rowget
        return DataStream(self)

    def getattr(self, name, default=None):
        def rowgetattr(row):
            return getattr(row, name)
        self._transform = rowgetattr
        return DataStream(self)

    def delete(self, key):
        self._transform = lambda row: {k: v for k, v in row.items() if k != key}
        return DataStream(self)

    def take(self, n):
        return DataStream(islice(self, 0, n))

    def drop(self, n):
        return DataStream(islice(self, n, None))

    def collect(self):
        return DataSet(self)
    
    def collectas(self, constructor):
        return DataSet(imap(constructor, self))

    def join(self, on, dataset):
        joiner = defaultdict(dict)
        for element in self + dataset:
            joiner[element[on]].update(element)
        return DataSet(joiner.values())

    def select(self, *args):
        self._transform = lambda row: {k: v for k, v in row.items() if k in args}
        return DataStream(self)

    def groupby(self, key, reduce_fn, init):
        groups = {}
        for row in self:
            groups[row[key]] = reduce_fn(groups.get(row[key], init), row)
        return groups

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

    def sortby(self, key_fn, descending=True):
        return DataStream(sorted(self._source, key=key_fn, reverse=descending))

    def reverse(self):
        return DataStream(element for element in self._source[::-1])

    def stream(self):
        return DataStream(iter(self))

