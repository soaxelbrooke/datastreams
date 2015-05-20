from itertools import islice, chain
import csv
from copy import copy
import random
try:
    from collections import defaultdict, deque, Counter, namedtuple
except ImportError:
    from backport_collections import defaultdict, deque, Counter, namedtuple
import sys
try:
    reduce
except NameError:
    from functools import reduce


class Datum(object):
    def __init__(self, attributes):
        if isinstance(attributes, dict):
            for name, value in attributes.items():
                setattr(self, name, value)
        else:
            for name, value in attributes:
                setattr(self, name, value)

    def __repr__(self):
        return "Datum({})".format(self.__dict__)


class DataStream(object):
    """ Foundation for the package - :py:class:`DataStream` allows you to chain
    map/filter/reduce/etc style operations together:

    >>> stream = DataStream(range(10))
    >>> stream.filter(lambda n: n % 2 == 0).map(lambda n: n*5).to_list()
    ... [0, 10, 20, 30, 40]

    DataStreams are evaluated lazily (using generators), providing memory efficiency and speed.  Using :py:func:`collect` produces a :py:class:`DataSet`, which evalutes the whole stream and caches the result.
    """

    @staticmethod
    def Stream(iterable,
               transform=lambda row: row,
               predicate=lambda row: True):
        # TODO document why for this!
        return DataStream(iterable, transform=transform, predicate=predicate)

    @staticmethod
    def Set(iterable):
        # TODO document why for this!
        return DataSet(iterable)

    def __init__(self, source,
                 transform=lambda row: row,
                 predicate=lambda row: True):
        self._source = iter(source)
        self._transform = transform
        self._predicate = predicate

    def __iter__(self):
        return (self._transform(row)
                for row in self._source
                if self._predicate(row))

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, str(self._source))

    def __str__(self):
        return self.__str__()

    def __next__(self):
        while True:
            src_next = next(self._source)
            if self._predicate(src_next):
                return self._transform(src_next)

    def next(self):
        return self.__next__()

    def reduce(self, function, initial):
        """ Applying a reducing function to rows in a stream

        :param function function: reducing function, with parameters ``last_iteration``, ``next_value``
        :param initial: initial value for the reduction
        :rtype: DataSet
        """
        return self.Set(reduce(function, self, initial))

    def map(self, function):
        """ Apply a function to each row in this stream

        >>> DataStream(range(5)).map(lambda n: n * 5).to_list()
        ... [0, 5, 10, 15, 20]

        :param function function: function to apply
        :rtype: DataStream
        """
        return self.Stream(self, transform=function)

    def map_method(self, method, *args, **kwargs):
        """ Call named method of each row using supplied args/kwargs

        >>> DataStream(['hi', 'hey', 'yo']).map_method('upper').to_list()
        ... ['HI', 'HEY', 'YO']

        :param str method: name of method to be called
        :rtype: DataStream
        """
        return self.map(lambda row: getattr(row, method)(*args, **kwargs))

    def concat(self):
        """ Alias for :py:func:`chain`

        >>> DataStream(['this', 2, None]).map(dir).concat().to_list()
        ... ['__add__', '__class__', '__contains__', '__delattr__', ...]

        :rtype: DataStream
        """
        return self.chain()

    def concat_map(self, function):
        """ :py:func:`map` a function over the stream, then concat it

        >>> DataStream(['this', 2, None]).concat_map(dir).to_list()
        ... ['__add__', '__class__', '__contains__', '__delattr__', ...]

        :param function function: function to apply
        :rtype: DataStream
        """
        return self.map(function).concat()

    def chain(self):
        """ Chains together iterables, flattening them

        >>> DataStream(['this', 2, None]).map(dir).chain().to_list()
        ... ['__add__', '__class__', '__contains__', '__delattr__', ...]

        :rtype: DataStream
        """
        return self.Stream(chain.from_iterable(self))

    def filter(self, filter_fn):
        """ Filters a stream using the passed in predicate function.

        >>> DataStream(range(10)).filter(lambda n: n % 2 == 0).to_list()
        ... [0, 2, 4, 6, 8]

        :param function filter_fn: only passes values for which filter_fn returns ``True``
        :rtype: DataStream
        """
        return self.Stream(self, predicate=filter_fn)

    def filters(self, filter_fns):
        """ Apply a list of filter functions

        >>> evens_less_than_six = [lambda n: n < 6, lambda n: n % 2 == 0]
        >>> DataStream(range(10)).filters(evens_less_than_six).to_list()
        ... [0, 2, 4]

        :param list[function] filter_fns: list of filter functions
        :rtype: DataStream
        """
        predicate = lambda row: all([pred(row) for pred in filter_fns])
        return self.Stream(self, predicate=predicate)

    def filter_method(self, method, *args, **kwargs):
        """ Filters using a method of the stream row using passed in args/kwargs

        >>> DataStream(['hi', 'h1', 'ho']).filter_method('isalpha').to_list()
        ... ['hi', 'ho']

        :param str method: name of method to be called
        :rtype: DataStream
        """
        return self.filter(lambda row: getattr(row, method)(*args, **kwargs))

    def set(self, name, transfer_func=None, value=None):
        """ Sets the named attribute of each row in the stream using the supplied function

        :param  name: attribute name
        :param transfer_func: function that takes the row and returns the value to be stored at the named attribute
        :rtype: DataStream
        """
        if transfer_func is not None:
            def row_setattr(row):
                new_row = copy(row)
                setattr(new_row, name, transfer_func(row))
                return new_row
        else:
            def row_setattr(row):
                new_row = copy(row)
                setattr(new_row, name, value)
                return new_row

        return self.map(row_setattr)

    def get(self, name, default=None):
        """ Gets the named attribute of each row in the stream

        >>> Person = namedtuple('Person', ['name', 'year_born'])
        >>> DataStream([Person('amy', 1987), Person('brad', 1980)]).get('year_born').to_list()
        ... [1987, 1980]

        :param str attr: attribute name
        :param default: default value to use if attr name not found in row
        :rtype: DataStream
        """
        def row_getattr(row):
            return getattr(row, name) if hasattr(row, name) else default
        return self.map(row_getattr)

    def delete(self, attr):
        """ Deletes the named attribute for each row in the stream """
        def obj_del(row):
            new_row = copy(row)
            delattr(new_row, attr)
            return new_row
        return self.map(obj_del)

    def for_each(self, function):
        """ Calls a function for each row in the stream, but passes the row value through

        >>> from pprint import pprint
        >>> DataStream(range(3)).for_each(pprint).execute()
        ... 0
        ... 1
        ... 2
        ... <datastreams.DataStream at 0x7f6995ea4790>

        :param function function: function to call on each row
        :rtype: DataStream
        """
        def apply_fn(row):
            function(row)
            return row
        return self.map(apply_fn)

    def take(self, n):
        """ Takes n rows from the stream

        >>> DataStream(range(100000)).take(3).to_list()
        ... [0, 1, 2]

        :param int n: number of rows to be taken
        :rtype: DataStream
        """
        return self.Stream(islice(self, 0, n))

    def take_now(self, n):
        """ Like take, but evaluates immediately and returns a :py:class:`DataSet`

        >>> DataStream(range(100000)).take_now(3)
        ... DataSet([0, 1, 2])

        :param int n: number of rows to be taken
        :rtype: DataSet
        """
        return self.Set([next(self) for _ in range(n)])

    def drop(self, n):
        """ Drops n rows from the stream

        >>> DataStream(range(10)).drop(5).to_list()
        ... [5, 6, 7, 8, 9]

        :param int n: number of rows to be dropped
        :rtype: DataStream
        """
        return self.Stream(islice(self, n, None))

    def collect(self):
        """ Collects the stream into a :py:class:`DataSet`

        >>> DataStream(range(5)).map(lambda n: n * 5).collect()
        ... DataSet([0, 5, 10, 15, 20])

        :rtype: DataSet
        """
        return self.Set(self)

    def collect_as(self, constructor):
        """ Collects using a constructor

        >>> DataStream(range(5)).collect_as(str)
        ... DataSet(['0', '1', '2', '3', '4'])

        :param constructor: class or constructor function
        :rtype: DataSet
        """
        return self.map(constructor).collect()

    def execute(self):
        """ Evaluates the stream (nothing happens until a stream is evaluted)

        >>> from pprint import pprint
        >>> DataStream(range(3)).for_each(pprint).execute()
        ... 0
        ... 1
        ... 2
        ... <datastreams.DataStream at 0x7f6995ea4790>
        """
        list(self)

    def batch(self, batch_size):
        """ Batches rows of a stream in a given chunk size

        >>> DataStream(range(10)).batch(2).to_list()
        ... [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9]]

        :param int batch_size: size of each batch
        :rtype: DataStream
        """
        return self.window(batch_size, batch_size)

    def window(self, length, interval):
        """ Windows the rows of a stream in a given length and interval

        >>> DataStream(range(5)).window(3, 2).to_list()
        ... [DataSet([0, 1, 2]), DataSet([2, 3, 4])]

        :param int length: length of window
        :param int interval: distance between windows
        :rtype: DataStream
        """
        queue = deque(maxlen=length)

        def window_iter():
            queue.extend(self.take_now(length))
            yield self.Set(queue)
            while True:
                for _ in range(interval):
                    queue.popleft()
                try:
                    for _ in range(interval):
                        queue.append(next(self))
                    yield self.Set(queue)
                except StopIteration:
                    if len(queue) != 0:
                        yield self.Set(queue)
                    break
        return self.Stream(window_iter())

    def dedupe(self, key_fn=lambda a: a):
        """ Removes duplicates from a stream, returning only unique values.

        >>> DataStream('aaaabcccddd').dedupe().to_list()
        ... ['a', 'b', 'c', 'd']

        :param function key_fn: function returning a hashable value used to determine uniqueness
        :return: DataStream
        """
        seen = set()

        def unique():
            for row in self:
                if key_fn(row) not in seen:
                    seen.add(key_fn(row))
                    yield row
        return self.Stream(unique())

    def sample(self, probability, n):
        """ Sample N rows with a given probability of choosing a given row
        
        >>> DataStream(range(100)).sample(0.1, 5)
        ... 

        :param float probability: probability that a sample is chosen
        :param int n: population size to sample
        :rtype: DataStream
        """
        return self.filter(lambda row: random.random() > probability).take(n)

    def group_by(self, key):
        """ Groups a stream by key, returning a :py:class:`DataSet` of ``(K, tuple(V))``

        >>> stream = DataStream(range(3) * 3)
        >>> stream.group_by('real').to_dict()
        ... {0: (0, 0, 0), 1: (1, 1, 1), 2: (2, 2, 2)}

        :param str key: attribute name to group by
        :rtype: DataSet
        """
        return self.group_by_fn(lambda ele: getattr(ele, key))

    def group_by_fn(self, key_fn):
        """ Groups a stream by function, returning a :py:class:`DataSet` of ``(K, tuple(V))``

        >>> stream = DataStream(['hi', 'hey', 'yo', 'sup'])
        >>> stream.group_by_fn(lambda w: len(w)).to_dict()
        ... {2: ('hi', 'yo'), 3: ('hey', 'sup')}

        :param function key_fn: key function returning hashable value to group by
        :rtype: DataSet
        """
        grouper = defaultdict(list)
        for ele in self:
            grouper[key_fn(ele)].append(ele)
        return self.Set(grouper.items())

    def to_dict(self):
        """ Converts a stream to a :py:class:`dict`

        >>> stream = DataStream(['hi', 'hey', 'yo', 'sup'])
        >>> stream.group_by_fn(lambda w: len(w)).to_dict()
        ... {2: ('hi', 'yo'), 3: ('hey', 'sup')}

        :rtype: dict
        """
        return dict(self.collect())

    def to_list(self):
        """ Converts a stream to a :py:class:`list`

        >>> DataStream(range(5)).map(lambda n: n * 5).to_list()
        ... [0, 5, 10, 15, 20]

        :rtype: list
        """
        return list(self.collect())

    def to_set(self):
        """ Converts a stream to a :py:class:`set`

        >>> DataStream([1, 2, 3, 4, 2, 3]).to_set()
        ... {1, 2, 3, 4}

        :rtype: set
        """
        return set(self.collect())

    def pipe_to_stdout(self):
        """ Pipes stream to stdout using ``sys.stdout.write`` """
        map(sys.stdout.write, self)

    def count_frequency(self):
        """ Counts frequency of each row in the stream

        >>> DataStream(['a', 'a', 'b', 'c']).count_frequency()
        ... DataSet([('a', 2), ('b', 1), ('c', 1)])

        :rtype: DataSet
        """
        return self.Set(Counter(self).items())

    def join(self, how, key, right):
        """ Returns a dataset joined using keys from right dataset only

        :param str how: ``left``, ``right``, ``outer``, or ``inner``
        :param DataStream right: :py:class:`DataStream` to be joined with
        :param str key: attribute name to join on
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

        :param str how: ``left``, ``right``, ``outer``, or ``inner``
        :param DataStream right: :py:class:`DataStream` to be joined with
        :param function left_key_fn: key function that produces a hashable value from left stream
        :param function right_key_fn: key function that produces a hashable value from right stream
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

        :param DataStream right: :py:class:`DataStream` to be joined with
        :param str key: attribute name to join on
        :rtype: DataSet
        """
        key_fn = lambda ele: getattr(ele, key)
        return self.left_join_by(key_fn, key_fn, right)

    def left_join_by(self, left_key_fn, right_key_fn, right):
        """ Returns a dataset joined using key functions to evaluate equality

        :param str how: ``left``, ``right``, ``outer``, or ``inner``
        :param function left_key_fn: key function that produces a hashable value from left stream
        :param function right_key_fn: key function that produces a hashable value from right stream
        :param DataStream right: :py:class:`DataStream` to be joined with
        :rtype: DataSet
        """
        joiner = defaultdict(list)
        for ele in right:
            joiner[right_key_fn(ele)].append(ele)
        joined = []
        for ele in self:
            for other in joiner.get(left_key_fn(ele), [None]):
                joined.append(join_objects(ele, other))
        return self.Set(joined)

    def right_join(self, key, right):
        """ Returns a dataset joined using keys in right dataset only

        :param DataStream right: :py:class:`DataStream` to be joined with
        :param str key: attribute name to join on
        :rtype: DataSet
        """
        key_fn = lambda ele: getattr(ele, key)
        return self.right_join_by(key_fn, key_fn, right)

    def right_join_by(self, left_key_fn, right_key_fn, right):
        """ Returns a dataset joined using key functions to evaluate equality

        :param function left_key_fn: key function that produces a hashable value from left stream
        :param function right_key_fn: key function that produces a hashable value from right stream
        :param DataStream right: :py:class:`DataStream` to be joined with
        :rtype: DataSet
        """
        joiner = defaultdict(list)
        for ele in self:
            joiner[left_key_fn(ele)].append(ele)
        joined = []
        for ele in right:
            for other in joiner.get(right_key_fn(ele), [None]):
                joined.append(join_objects(ele, other))
        return self.Set(joined)

    def inner_join(self, key, right):
        """ Returns a dataset joined using keys in both dataset only

        :param DataStream right: :py:class:`DataStream` to be joined with
        :param str key: attribute name to join on
        :rtype: DataSet
        """
        key_fn = lambda ele: getattr(ele, key)
        return self.inner_join_by(key_fn, key_fn, right)

    def inner_join_by(self, left_key_fn, right_key_fn, right):
        """ Returns a dataset joined using key functions to evaluate equality

        :param function left_key_fn: key function that produces a hashable value from left stream
        :param function right_key_fn: key function that produces a hashable value from right stream
        :param DataStream right: :py:class:`DataStream` to be joined with
        :rtype: DataSet
        """
        joiner = defaultdict(list)
        for ele in right:
            joiner[right_key_fn(ele)].append(ele)
        joined = []
        for ele in self:
            for other in joiner[left_key_fn(ele)]:
                joined.append(join_objects(ele, other))
        return self.Set(joined)

    def outer_join(self, key, right):
        """ Returns a dataset joined using keys in either datasets

        :param DataStream right: :py:class:`DataStream` to be joined with
        :param str key: attribute name to join on
        :rtype: DataSet
        """
        key_fn = lambda ele: getattr(ele, key)
        return self.outer_join_by(key_fn, key_fn, right)

    def outer_join_by(self, left_key_fn, right_key_fn, right):
        """ Returns a dataset joined using key functions to evaluate equality

        :param function left_key_fn: key function that produces a hashable value from left stream
        :param function right_key_fn: key function that produces a hashable value from right stream
        :param DataStream right: :py:class:`DataStream` to be joined with
        :rtype: DataSet
        """
        left_joiner = defaultdict(list)
        for ele in self:
            left_joiner[left_key_fn(ele)].append(ele)
        right_joiner = defaultdict(list)
        for ele in right:
            right_joiner[right_key_fn(ele)].append(ele)
        keys = set(left_joiner.keys()).union(set(right_joiner.keys()))

        def iter_join(l, r, join_keys):
            for join_key in join_keys:
                for ele in l.get(join_key, [None]):
                    for other in r.get(join_key, [None]):
                        yield join_objects(ele, other)

        return self.Set(iter_join(left_joiner, right_joiner, keys))

    def pick_attrs(self, attr_names):
        """ Picks attributes from each row in a stream.  This is helpful for limiting row attrs to only those you want to save in a database, etc.

        >>> Person = namedtuple('Person', ['name', 'year_born'])
        >>> DataStream([Person('amy', 1987), Person('brad', 1980)]).pick_attrs(['year_born']).to_list()
        ... [Datum({'year_born': 1987}), Datum({'year_born': 1980})]

        :param list[str] attr_names: list of attribute names to keep
        :rtype: DataStream
        """
        def attr_filter(row):
            return Datum(dict((name, getattr(row, name)) for name in attr_names))
        return self.map(attr_filter)

    def where(self, name):
        """ Short hand for common filter functions - ``where`` selects an attribute to be filtered on, with a condition like ``gt`` or ``contains`` following it.

        >>> Person = namedtuple('Person', ['name', 'year_born'])
        >>> DataStream([Person('amy', 1987), Person('brad', 1980)]).where('year_born').gt(1983).to_list()
        ... [Person(name='amy', year_born=1987)]

        :param str name: attribute name to filter on
        :rtype: FilterRadix
        """
        return FilterRadix(self, name)

    @classmethod
    def from_file(cls, path):
        """ Stream lines from a file

        >>> DataStream.from_file('hamlet.txt').concat_map(str.split).take(7)
        ... ['The', 'Tragedy', 'of', 'Hamlet,', 'Prince', 'of', 'Denmark']

        :param str path: path to file to be streamed
        :rtype: DataStream
        """
        source_file = open(path)
        return cls.Stream(cls.iter_file(source_file))

    @staticmethod
    def iter_file(source_file):
        for line in source_file:
            yield line
        source_file.close()
        raise StopIteration

    @classmethod
    def from_csv(cls, path, headers=None, constructor=Datum):
        """ Stream rows from a csv file

        >>> DataStream.from_csv('payments.csv').to_list()
        ... [Datum({'name': 'joe', 'charge': 174.93}), Datum({'name': 'sally', 'charge': 198.05}), ...]

        :param str path: path to csv to be streamed
        :param list[str] headers: manual names for headers - if present, first row is pulled in as data, if ``None``, first row is used as headers
        :param constructor: class or function to construct for each row
        :rtype: DataStream
        """
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

    @classmethod
    def from_stdin(cls):
        """ Stream rows from stdin

        :rtype: DataStream
        """
        return cls.Stream(sys.stdin)


class FilterRadix(object):
    def __init__(self, stream, attr_name):
        self._source = stream
        self.attr_name = attr_name

    def eq(self, value):
        name = self.attr_name
        return self._source.filter(lambda row: getattr(row, name) == value)

    def neq(self, value):
        name = self.attr_name
        return self._source.filter(lambda row: getattr(row, name) != value)

    def gt(self, value):
        name = self.attr_name
        return self._source.filter(lambda row: getattr(row, name) > value)

    def gteq(self, value):
        name = self.attr_name
        return self._source.filter(lambda row: getattr(row, name) >= value)

    def lt(self, value):
        name = self.attr_name
        return self._source.filter(lambda row: getattr(row, name) < value)

    def lteq(self, value):
        name = self.attr_name
        return self._source.filter(lambda row: getattr(row, name) <= value)

    def is_in(self, value):
        name = self.attr_name
        return self._source.filter(lambda row: getattr(row, name) in value)

    def not_in(self, value):
        name = self.attr_name
        return self._source.filter(lambda row: getattr(row, name) not in value)

    def isinstance(self, value):
        name = self.attr_name
        return self._source.filter(
            lambda row: isinstance(getattr(row, name), value))

    def notinstance(self, value):
        name = self.attr_name
        return self._source.filter(
            lambda row: not isinstance(getattr(row, name), value))

    def is_(self, value):
        name = self.attr_name
        return self._source.filter(lambda row: getattr(row, name) is value)

    def is_not(self, value):
        name = self.attr_name
        return self._source.filter(lambda row: getattr(row, name) is not value)

    def contains(self, value):
        name = self.attr_name
        return self._source.filter(lambda row: value in getattr(row, name))

    def doesnt_contain(self, value):
        name = self.attr_name
        return self._source.filter(lambda row: value not in getattr(row, name))

    def len_eq(self, value):
        name = self.attr_name
        return self._source.filter(lambda row: len(getattr(row, name)) == value)

    def len_gt(self, value):
        name = self.attr_name
        return self._source.filter(lambda row: len(getattr(row, name)) > value)

    def len_lt(self, value):
        name = self.attr_name
        return self._source.filter(lambda row: len(getattr(row, name)) < value)

    def len_gteq(self, value):
        name = self.attr_name
        return self._source.filter(lambda row: len(getattr(row, name)) >= value)

    def len_lteq(self, value):
        name = self.attr_name
        return self._source.filter(lambda row: len(getattr(row, name)) <= value)


class DataSet(DataStream):
    """ Like a :py:class:`DataStream`, but with the source cached as a list.  Able to perform tasks that require the whole source, like sorting and reversing.
    """

    def __init__(self, source):
        super(DataSet, self).__init__(source)
        self._source = list(source)

    def __len__(self):
        return len(self._source)

    def __getitem__(self, item):
        return self._source[item]

    def __repr__(self):
        head, tail = ', '.join(map(str, self[:5])), ', '.join(map(str, self[-5:]))
        return "{}([{}, ... {}])".format(self.__class__.__name__, head, tail)

    def __str__(self):
        return self.__repr__()

    def take_now(self, n):
        return self.Set([self._source[i] for i in range(n)])

    def apply(self, function):
        """ Apply a function to the whole dataset

        :param function function: function to be called on the whole dataset
        :rtype: DataSet
        """
        return self.Set(function(self))

    def call(self, function):
        """ Call a function with the whole dataset, returning the original

        >>> from pprint import pprint
        >>> DataSet([1, 2, 3]).apply(pprint)
        ... DataSet([1, 2, 3])
        ... DataSet([1, 2, 3])

        :param function function: function to be called on the whole dataset
        :rtype: DataSet
        """
        function(self)
        return self

    def sort_by(self, key_fn, descending=True):
        """ Sort the :py:class:`DataSet` using the given key function

        >>> Person = namedtuple('Person', ['name', 'year_born'])
        >>> DataSet([Person('amy', 1987), Person('brad', 1980)]).sort_by(lambda p: p.year_born)
        ... DataSet([Datum({'name': 'amy', 'year_born': 1980}), Datum({'name': 'brad', 'year_born': 1987})])

        :param function key_fn: function used select the key used to sort the dataset
        :param bool descending: sorts descending if ``True``
        :rtype: DataSet
        """
        return self.Stream(sorted(self._source, key=key_fn, reverse=descending))

    def reverse(self):
        """ Reverses a :py:class:`DataSet`

        >>> DataSet(range(5)).reverse()
        ... DataSet([4, 3, 2, 1, 0])

        :rtype: DataSet
        """
        return self.Stream(element for element in self._source[::-1])

    def to_stream(self):
        """ Streams from this dataset

        :rtype: DataStream
        """
        return self.Stream(iter(self))

    @classmethod
    def from_csv(cls, path, headers=None, constructor=Datum):
        return cls.Set(DataStream.from_csv(path, headers, constructor))


def get_object_attrs(obj):
    if hasattr(obj, '__dict__'):
        return obj.__dict__
    elif hasattr(obj, '__slots__'):
        return dict((key, getattr(obj, key)) for key in obj.__slots__)
    else:
        return {}


def join_objects(left, right):
    joined_class = type(left.__class__.__name__ + right.__class__.__name__, (Datum,), {})
    attrs = {}
    attrs.update(get_object_attrs(right))
    attrs.update(get_object_attrs(left))
    attrs['left'] = left
    attrs['right'] = right
    return joined_class(attrs)
