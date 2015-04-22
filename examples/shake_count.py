__author__ = 'stuart'

from datastreams import DataStream
import re
from functools import partial
from datetime import datetime

started = datetime.now()
print("Started at {}".format(started))

word_counts = DataStream.from_file("shakespeare_complete.txt")\
    .take(10000).map_method('lower')\
    .map(partial(re.split, '\W+'))\
    .count_frequency().take(1000).to_list()

# print(word_counts)
ended = datetime.now()
print("Ended at {}, total run time {}".format(ended, ended - started))
