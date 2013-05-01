#!/usr/bin/env python
# coding: utf-8

import glob
import os
import sys

from collections import defaultdict
from re import compile as regexp_compile


regexp_finished = regexp_compile(r'Job finished: id=([a-f0-9]+), '
                                 'worker=([a-zA-Z0-9]+)')

regexp_job_duration = regexp_compile(r'\[API\] Request to router: ({[^\n]+})')


def parse_log(filename):
    with open(filename) as fobj:
        contents = fobj.read()

    ids_and_worker_names = regexp_finished.findall(contents)
    job_ids = defaultdict(list)
    map(lambda x: job_ids[x[1]].append(x[0]), ids_and_worker_names)

    job_durations = {}
    for raw_message in regexp_job_duration.findall(contents):
        if 'job finished' in raw_message:
            instruction = 'data = {}'.format(raw_message)
            namespace = {}
            exec instruction in namespace
            data = namespace['data']
            job_durations[data['job id']] = data['duration']

    # We need to append instead of writing, because we want information from
    # more than one log file (there is one for each server where there is a
    # broker running).
    log_files = {worker: open('data/worker-{}.dat'.format(worker), 'a')
                 for worker in job_ids.keys()}

    worker_durations = defaultdict(list)
    for worker, job_ids in job_ids.items():
        for job_id in job_ids:
            if job_id in job_durations:
                log_files[worker].write("{}\t{}\n"
                                        .format(job_id, job_durations[job_id]))
                del job_durations[job_id]
            else:
                sys.stderr.write('[{}] ERROR: job id {} not in job durations.\n'
                                 .format(filename, job_id))

    for worker, fobj in log_files.items():
        fobj.close()

    if len(job_durations):
        for key, value in job_durations.items():
            sys.stderr.write('[{}] ERROR: job duration not used: {}\n'
                             .format(filename, key))

def main():
    if not os.path.exists('data'):
        os.mkdir('data')
    # Move old files, so this will not append to them.
    for existing_file in glob.glob("data/worker-*.dat"):
        os.rename(existing_file, "{}.old".format(existing_file))

    for filename in glob.glob("logs/*-broker.log"):
        parse_log(filename)

if __name__ == '__main__':
    main()
