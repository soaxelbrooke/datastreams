from datastreams.datastreams import *

DataStream.from_stdin().map(lambda line: line.upper()).pipe_to_stdout()
