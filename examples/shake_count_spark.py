__author__ = 'stuart'

from datastreams.rddstreams import RddStream
import re
from functools import partial
from datetime import datetime
from pyspark import SparkContext

started = datetime.now()
print("Started at {}".format(started))

sc = SparkContext("local", "App Name", pyFiles=['/home/stuart/Projects/datastreams/datastreams/datastreams.py',])
word_counts = RddStream(sc.textFile("/home/stuart/Projects/datastreams/examples/shakespeare_complete.txt")) \
    .map_method('lower') \
    .concat_map(partial(re.split, '\W+'))\
    .filter(lambda word: word != '') \
    .count_frequency()\
    .sort_by(lambda pair: pair[1])\
    .take(100).to_list()

print("\n\n\nTop 100 words:\n{}\n\n\n".format(word_counts))

ended = datetime.now()
print("Ended at {}, total run time {}".format(ended, ended - started))
