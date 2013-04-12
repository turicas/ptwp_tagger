#!/usr/bin/env python
# coding: utf-8

import argparse
import sys
import time

from re import compile as regexp_compile
from tempfile import TemporaryFile

import pymongo

from pypln.api import PyPLN


regexp_mongodb = regexp_compile(r'([^:]+):([^/]+)/([^/]+)/(.+)')


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
    args = parser.parse_args()

    mongo_config = regexp_mongodb.findall(args.mongodb)
    if not mongo_config:
        sys.stdout.write('Error: "mongodb" should be in format '
                         'host:port/db/collection\n')
        exit(1)

    mongo = dict(zip(('host', 'port', 'db', 'collection'), mongo_config[0]))
    connection = pymongo.Connection(host=mongo['host'],
                                    port=int(mongo['port']), safe=True)
    db = connection[mongo['db']]
    collection = db[mongo['collection']]

    pypln = PyPLN(args.pypln)
    pypln.login(args.username, args.password)

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

    query_filter = {'uploaded': False}
    total = float(collection.find(query_filter).count())
    counter = 0
    report = '\r{:07d} uploaded pages ({:5.2f}%), {:10.3f}s ({:15.3f}p/s)'
    start_time = time.time()
    for page in collection.find(query_filter):
        temp_file = TemporaryFile()
        temp_file.write(page['text'].encode('utf-8'))
        temp_file.seek(0)
        filename = u'{}.txt'.format(page['title'])
        corpus.add_document(temp_file, filename=filename)
        collection.update({'_id': page['_id']}, {'$set': {'uploaded': True}})

        counter += 1
        percentual = 100 * (counter / total)
        delta_time = time.time() - start_time
        rate = counter / delta_time
        sys.stdout.write(report.format(counter, percentual, delta_time, rate))
        sys.stdout.flush()
    sys.stdout.write('\n')


if __name__ == '__main__':
    main()
