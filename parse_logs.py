#!/usr/bin/env python
# coding: utf-8

from collections import defaultdict
import glob
import os
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
    log_files = {worker: open('data/worker-{}.csv'.format(worker), 'a')
                 for worker in job_ids.keys()}

    worker_durations = defaultdict(list)
    for worker, job_ids in job_ids.items():
        for job_id in job_ids:
            if job_id in job_durations:
                log_files[worker].write("{}\t{}\n"
                                        .format(job_id, job_durations[job_id]))

    for worker, fobj in log_files.items():
        fobj.close()


def main():
    # Move old files, so this will not append to them.
    for existing_file in glob.glob("data/worker-*.csv"):
        os.rename(existing_file, "{}.old".format(existing_file))

    for filename in glob.glob("data/*_broker.log"):
        parse_log(filename)

if __name__ == '__main__':
    main()
