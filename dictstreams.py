from datastreams import DataStream, DataSet
from collections import defaultdict
from copy import copy


class DictStream(DataStream):

    @staticmethod
    def Stream(iterable):
        return DictStream(iterable)

    @staticmethod
    def Set(iterable):
        return DictSet(iterable)

    def set(self, key, transfer_func):
        def rowset(row):
            newrow = copy(row)
            newrow[key] = transfer_func(row)
            return newrow
        return self.map(rowset)

    def get(self, key, default=None):
        def rowget(row):
            return row.get(key, default)
        return self.map(rowget)

    def delete(self, key):
        self._transform = lambda row: {k: v for k, v in row.items() if k != key}
        return self.Stream(self)

    def select(self, *args):
        self._transform = lambda row: {k: v for k, v in row.items() if k in args}
        return self.Stream(self)

    def groupby(self, key, reduce_fn, init):
        groups = {}
        for row in self:
            groups[row[key]] = reduce_fn(groups.get(row[key], init), row)
        return groups


class DictSet(DictStream, DataSet):

    def join(self, on, dictset):
        joiner = defaultdict(dict)
        for element in self + dictset:
            joiner[element[on]].update(element)
        return self.Set(joiner.values())

