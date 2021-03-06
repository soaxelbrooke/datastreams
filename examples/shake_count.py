__author__ = 'stuart'

from datastreams import DataStream
import re
from functools import partial
from datetime import datetime

started = datetime.now()
print("Started at {}".format(started))

word_counts = DataStream.from_file("shakespeare_complete.txt") \
    .map_method('lower') \
    .map(partial(re.split, '\W+')) \
    .filter(lambda word: word != '') \
    .count_frequency() \
    .sort_by(lambda pair: pair[1]) \
    .take(100).to_list()

print("\n\n\nTop 100 words:\n{}\n\n\n".format(word_counts))

ended = datetime.now()
print("Ended at {}, total run time {}".format(ended, ended - started))
