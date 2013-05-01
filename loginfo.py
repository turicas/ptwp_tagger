#!/usr/bin/env python
# coding: utf-8

from glob import glob
from math import sqrt


def stddev(numbers):
    size = len(numbers)
    mean = sum(numbers) / size
    return sqrt(sum((mean - number) ** 2 for number in numbers) / size)


for filename in glob('data/worker-*.dat'):
    with open(filename, 'r') as fobj:
        durations = [float(line.split('\t')[1].strip()) \
                     for line in fobj if line.strip()]
        worker = filename.split('/')[-1].split('-')[-1].replace('.dat', '')
        print(worker)
        print('  min={}'.format(min(durations)))
        print('  max={}'.format(max(durations)))
        print('  avg={}'.format(sum(durations) / len(durations)))
        print('  stddev={}'.format(stddev(durations)))
        print('')
