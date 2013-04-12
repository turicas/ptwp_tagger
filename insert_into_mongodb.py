#!/usr/bin/env python
# coding: utf-8

import argparse
import codecs
import sys
import time

from os import path, walk
from re import compile as regexp_compile, DOTALL

import pymongo


doc_format = '''
<doc id="([^"]+)" url="([^"]+)" title="[^"]+">
([^\n]+)

(.*)
</doc>'''.strip()

doc_regexp = regexp_compile(doc_format, flags=DOTALL)
doc_fields = ('id', 'url', 'title', 'text')
regexp_mongodb = regexp_compile(r'([^:]+):([^/]+)/([^/]+)/(.+)')


def parse_doc(text):
    '''Parse a WP page in "<doc>...</doc>" format and return a dict'''
    result = doc_regexp.findall(text)
    page = dict(zip(doc_fields, result[0]))
    page['text'] = page['text'].replace('</ref>', '')\
                               .replace('</math>', '').strip()
    return page


def parse_docs(raw_text):
    '''Given a string with "<doc>...</doc>"s, return a list of dicts'''
    raw_documents = raw_text.split('</doc>')[:-1]
    return [parse_doc(document + '</doc>') for document in raw_documents]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mongodb',
                        help='MongoDB server/db/collection (format: '
                             'host:port/db/collection)')
    parser.add_argument('txt_path',
                        help='Path to text files in <doc>...</doc> format')
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
    db.drop_collection(mongo['collection'])
    collection = db[mongo['collection']]

    counter = 0
    start_time = time.time()
    for dirpath, dirnames, filenames in walk(args.txt_path):
        for filename in filenames:
            filepath = path.join(dirpath, filename)
            with codecs.open(filepath, encoding='utf-8') as fobj:
                contents = fobj.read()
                pages = parse_docs(contents)
                for page in pages:
                    page['uploaded'] = False
                    collection.insert(page)
            counter += len(pages)
            delta_time = time.time() - start_time
            rate = counter / delta_time
            sys.stdout.write('\r{:010d} pages, {:9.3f}s ({:15.9f}p/s)'
                             .format(counter, delta_time, rate))
            sys.stdout.flush()

    delta_time = time.time() - start_time
    rate = counter / delta_time
    sys.stdout.write('\r{:010d} pages, {:9.3f}s ({:15.9f}p/s)\n'
                     .format(counter, delta_time, counter / delta_time))
    connection.close()


if __name__ == '__main__':
    main()
