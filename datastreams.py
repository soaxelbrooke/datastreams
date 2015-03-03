from itertools import imap, islice
from object_join import JoinedObject
import csv
from copy import copy
from collections import defaultdict


class Datum(object):
    def __init__(self, attributes):
        for name, value in attributes:
            setattr(self, name, value)


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
    def fromcsv(path, headers=None, constructor=Datum):
        with open(path) as source_file:
            if headers is None:
                headers = [h.strip() for h in source_file.readline().split(",")]
            reader = csv.reader(source_file)
            return DataSet(constructor(zip(headers, row)) for row in reader)


        # with open(path) as source_file:
        #     if headers_from_file:
        #         headers = [h.strip() for h in source_file.readline().split(",")]
        #     if constructor == dict:
        #         reader = csv.DictReader(source_file, headers)
        #     else:
        #         reader = (constructor(line) for line in source_file)
        #     return DataSet(row for row in reader)


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

    def left_join(self, key, right):
        """ Returns a dataset joined using keys from right dataset only
        :type right: DataSet
        :type key: str
        :rtype: DataSet
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

    def sortby(self, key_fn, descending=True):
        return DataStream(sorted(self._source, key=key_fn, reverse=descending))

    def reverse(self):
        return DataStream(element for element in self._source[::-1])

    def stream(self):
        return DataStream(iter(self))

    @staticmethod
    def fromcsv(path, headers=None, constructor=Datum):
        return DataSet(DataStream.fromcsv(path, headers, constructor))