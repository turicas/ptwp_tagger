#!/usr/bin/env python
#-*- coding: utf-8 -*-
import glob
import os
import sys

from matplotlib import pyplot

# There are some outliers that make the histogram useless. For now we'll remove
# any execution time above 1 second.
data = {}

filenames = glob.glob("data/worker-*.dat")
if not filenames:
    sys.stderr.write("You need data files in data/worker-*.dat\n")
    sys.exit(1)

for filename in filenames:
    worker_name = filename.split("-")[-1].split('.')[0]

    with open(filename) as fd:
        data[worker_name] = [float(line.split('\t')[1].strip())
                                for line in fd.readlines()]

try:
    os.mkdir('data/graphs')
except OSError:
    pass

for worker in data.keys():
    pyplot.hist(data[worker], label=worker)
    pyplot.legend(fancybox=True)
    pyplot.xlabel("Worker execution time (in seconds)")
    pyplot.savefig("data/graphs/{}_execution_time_histogram.png".format(worker))
    pyplot.close()
