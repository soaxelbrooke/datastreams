from datastreams import DataStream, DataSet


class DictStream(DataStream):

    @staticmethod
    def Stream(iterable,
               transform=lambda row: row,
               predicate=lambda row: True):
        return DictStream(iterable, transform=transform, predicate=predicate)

    @staticmethod
    def Set(iterable):
        return DictSet(iterable)

    @staticmethod
    def getattr(row, name):
        return row.get(name)

    @staticmethod
    def hasattr(row, name):
        return name in row

    @staticmethod
    def setattr(row, name, value):
        row[name] = value

    def delete(self, key):
        transform = lambda row: dict((k, v) for k, v in row.items() if k != key)
        return self.Stream(self, transform=transform)

    @staticmethod
    def join_objects(left, right):
        joined = {}
        joined.update(left.items())
        joined.update(right.items())
        joined['left'] = left
        joined['right'] = right
        return joined


class DictSet(DictStream, DataSet):
    pass  # TODO implement dict inner/outer joins

