# Python DataStreams

A streaming library to make your data processing beautiful and succinct.

```python
>>> from datastreams import DataStream

>>> hello = DataStream("Hello, world!")\
...     .filter(lambda char: char.isalpha())\
...     .map(lambda char: char.lower())\
...     .count_frequency()

>>> print(list(hello))
[('e', 1), ('d', 1), ('h', 1), ('o', 2), ('l', 3), ('r', 1), ('w', 1)]
```
