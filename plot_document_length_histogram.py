#!/usr/bin/env python
#-*- coding: utf-8 -*-
import glob
import sys

from matplotlib import pyplot
from math import sqrt
import numpy

TEXT_SIZE_THRESHOLD = 1000
TOKEN_COUNT_THRESHOLD = 500

filename = "data/document_lengths.dat"

def stddev(numbers):
    size = len(numbers)
    mean = sum(numbers) / size
    return sqrt(sum((mean - number) ** 2 for number in numbers) / size)


with open(filename) as fd:
    text_lengths, token_counts = [], []
    for line in fd.readlines():
        text_length, token_count = line.split('\t')
        text_length = float(text_length.strip())
        token_count = float(token_count.strip())

        if text_length <= TEXT_SIZE_THRESHOLD:
            text_lengths.append(text_length)

        if token_count <= TOKEN_COUNT_THRESHOLD:
            token_counts.append(token_count)

print("len(text) <= {}; len(tokens) <= {}".format(TEXT_SIZE_THRESHOLD,
    TOKEN_COUNT_THRESHOLD))

pyplot.hist(text_lengths, bins=TEXT_SIZE_THRESHOLD / 10.0)
pyplot.title("Text length histogram")
pyplot.savefig("data/graphs/document_length_histogram.png")
desc = ('Text lengths\n'
'  min={}\n'
'  max={}\n'
'  avg={}\n'
'  stddev={}\n'
'  variance={}\n'
'').format(min(text_lengths), max(text_lengths),
    sum(text_lengths) / len(text_lengths), numpy.std(text_lengths),
    numpy.var(text_lengths))
print(desc)
pyplot.close()


pyplot.hist(token_counts, bins=TOKEN_COUNT_THRESHOLD / 5.0)
pyplot.suptitle("Token count histogram")
pyplot.savefig("data/graphs/token_count_histogram.png")
desc = ('Token counts\n'
'  min={}\n'
'  max={}\n'
'  avg={}\n'
'  stddev={}\n'
'  variance={}\n'
'').format(min(token_counts), max(token_counts),
    sum(token_counts) / len(token_counts), numpy.std(token_counts),
    numpy.var(token_counts))
print(desc)
