from datastreams import DataStream, DataSet, FilterRadix
from copy import copy


class DictStream(DataStream):

    @staticmethod
    def Stream(iterable,
               transform=lambda row: row,
               predicate=lambda row: True):
        return DictStream(iterable, transform=transform, predicate=predicate)

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
        transform = lambda row: dict((k, v) for k, v in row.items() if k != key)
        return self.Stream(self, transform=transform)

    def select(self, *args):
        transform = lambda row: dict((k, v) for k, v in row.items() if k in args)
        return self.Stream(self, transform=transform)

    def groupby(self, key, reduce_fn, init):
        groups = {}
        for row in self:
            groups[row[key]] = reduce_fn(groups.get(row[key], init), row)
        return groups

    def where(self, name):
        return DictFilterRadix(self, name)

class DictFilterRadix(FilterRadix):
    @staticmethod
    def getattr(row, name):
        return row.get(name)


class DictSet(DictStream, DataSet):
    pass  # TODO implement dict inner/outer joins

