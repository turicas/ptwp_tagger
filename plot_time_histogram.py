#!/usr/bin/env python
#-*- coding: utf-8 -*-
import glob
import sys

from matplotlib import pyplot

# There are some outliers that make the histogram useless. For now we'll remove
# any execution time above 1 second.
THRESHOLD = 30.0

data = {}
outliers = {}

filenames = glob.glob("data/worker-*.dat")
if not filenames:
    sys.stderr.write("You need data files in data/worker-*.dat\n")
    sys.exit(1)

for filename in filenames:
    worker_name = filename.split("-")[-1].split('.')[0]

    with open(filename) as fd:
        data[worker_name] = [float(line.split('\t')[1].strip())
                                for line in fd.readlines()]

        original_size = len(data[worker_name])
        data[worker_name] = filter(lambda x: x < THRESHOLD, data[worker_name])
        filtered_size = len(data[worker_name])

        outliers[worker_name] = original_size - filtered_size


labels = ["{} ({})".format(worker, outliers[worker]) for worker in data.keys()]
pyplot.hist(data.values(), label=labels)
pyplot.legend(title="Worker (outliers removed)", #fontsize="small",
        fancybox=True)
pyplot.suptitle("Execution times greater than {}s were "
        "removed".format(THRESHOLD))
pyplot.xlabel("Worker execution time (in seconds)")
pyplot.savefig("data/execution_time_histogram.png")
