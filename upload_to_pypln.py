#!/usr/bin/env python
# coding: utf-8

import argparse
import sys
import time

from datetime import timedelta
from re import compile as regexp_compile
from tempfile import TemporaryFile

import pymongo

from pypln.api import PyPLN


regexp_mongodb = regexp_compile(r'([^:]+):([^/]+)/([^/]+)/(.+)')


def partition(iterator, n):
    iterator = iter(iterator)
    finished = False
    while not finished:
        values = []
        for i in range(n):
            try:
                values.append(iterator.next())
            except StopIteration:
                finished = True
        if values:
            yield values


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mongodb',
                        help='MongoDB server/db/collection (format: '
                             'host:port/db/collection)')
    parser.add_argument('pypln',
                        help='Main URL to PyPLN installation. Example: '
                             'http://demo.pypln.org/')
    parser.add_argument('username',
                        help='Username to log-in PyPLN installtion')
    parser.add_argument('password',
                        help='Password to log-in PyPLN installtion')
    parser.add_argument('corpus',
                        help='Name of the corpus to upload documents to '
                             "(if doesn't exists, will be created)")
    parser.add_argument('--pages-per-request',
                        help='Number of pages to send in a single request')
    parser.add_argument('--max-pages',
                        help='Maximum number of pages to send')
    args = parser.parse_args()
    if args.pages_per_request:
        pages_per_request = int(args.pages_per_request)
    else:
        pages_per_request = 10

    mongo_config = regexp_mongodb.findall(args.mongodb)
    if not mongo_config:
        sys.stdout.write('Error: "mongodb" should be in format '
                         'host:port/db/collection\n')
        exit(1)

    print 'Connecting to MongoDB...'
    mongo = dict(zip(('host', 'port', 'db', 'collection'), mongo_config[0]))
    connection = pymongo.Connection(host=mongo['host'],
                                    port=int(mongo['port']), safe=True)
    db = connection[mongo['db']]
    collection = db[mongo['collection']]

    print 'Logging into PyPLN at {}...'.format(args.pypln)
    pypln = PyPLN(args.pypln)
    pypln.login(args.username, args.password)

    print 'Selecting (or creating) corpus {}...'.format(args.corpus)
    corpora = pypln.corpora()
    find = [corpus for corpus in corpora \
            if corpus.name.lower() == args.corpus.lower()]
    if not find:
        corpus = pypln.add_corpus(name=args.corpus,
                                  description='Portuguese Wikipedia')
    else:
        corpus = find[0]
        # fix a bug in pypln.api:
        corpus.url = '{}corpora/{}'.format(args.pypln, corpus.slug)

    print 'Uploading...'
    query_filter = {'uploaded': False}
    total = float(collection.count())
    if args.max_pages:
        max_pages = int(args.max_pages)
    else:
        max_pages = total
    counter = collection.find({'uploaded': True}).count()
    initial_counter = counter
    report = '\r  {:07d} / {:07d} ({:5.2f}%), {:10.3f}s ({:9.3f}p/s). ETA: {}'
    start_time = time.time()
    cursor = collection.find(query_filter, timeout=False)
    page_iterator = partition(cursor, pages_per_request)
    for pages in page_iterator:
        temp_files, filenames = [], []
        for page in pages:
            temp_file = TemporaryFile()
            temp_file.write(page['text'].encode('utf-8'))
            temp_file.seek(0)
            temp_files.append(temp_file)
            filename = u'{}.txt'.format(page['title'])
            filenames.append(filename)
        corpus.add_documents(temp_files, filenames)
        for page in pages:
            collection.update({'_id': page['_id']},
                              {'$set': {'uploaded': True}})

        counter += len(pages)
        percentual = 100 * (counter / total)
        delta_time = time.time() - start_time
        rate = (counter - initial_counter) / delta_time
        eta = timedelta(((max_pages - counter) / rate) / (24 * 3600))
        sys.stdout.write(report.format(counter, int(total), percentual,
                                       delta_time, rate, eta))
        sys.stdout.flush()

        if max_pages and counter >= max_pages:
            break
    sys.stdout.write('\n')
    cursor.close()


if __name__ == '__main__':
    main()
