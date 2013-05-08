# coding: utf-8

import sys
import pymongo

from datetime import timedelta
from time import time

from django.conf import settings

from apps.core.models import Document
from sqlite_corpus import TaggedSQLiteCorpusWriter


def main():
    if len(sys.argv) != 2:
        exit(1)

    corpus_name = sys.argv[1]
    doc_ids = Document.objects.filter(corpus__name=corpus_name).values_list(
        'id', 'blob')

    conn = pymongo.connection.Connection(host=settings.MONGODB_CONFIG['host'],
            port=settings.MONGODB_CONFIG['port'])
    collection = conn['pypln']['analysis']

    filename = '/srv/pypln/{}.sqlite'.format(corpus_name)
    writer = TaggedSQLiteCorpusWriter(filename)
    writer.metadata = {'name': 'Portuguese Wikipedia',
                       'readme': ('Portuguese Wikipedia pages, '
                                  'retrieved in April 2013.')}

    report = '\r  {:07d} / {:07d} ({:5.2f}%), {:10.3f}s ({:9.3f}p/s). ETA: {}'
    total = float(len(doc_ids))
    counter = 0
    start_time = time()
    find = collection.find_one
    errors = []
    for doc_id, filename in doc_ids:
        counter += 1

        header = 'id:{}:'.format(doc_id)
        pos = find({"_id": header + "pos"}, {"value": 1, "_id": 0})
        text = find({"_id": header + "text"}, {"value": 1, "_id": 0})
        if None in (pos, text):
            continue
        filename = filename[1:-4] # removes '/' and '.txt'
        pos = pos['value']
        if pos is None:
            pos = []
        text = text['value']
        if text is None:
            text = ''

        try:
            writer.add_document(filename, text, pos, commit=False)
        except ValueError:
            errors.append(filename)
            print('Document {} was already added.'.format(filename))

        if counter % 10000 == 0:
            writer.commit()

        if counter % 1000 == 0:
            percentual = 100 * (counter / total)
            delta_time = time() - start_time
            rate = counter / delta_time
            eta = timedelta(((total - counter) / rate) / (24 * 3600))
            sys.stdout.write(report.format(counter, int(total), percentual,
                                           delta_time, rate, eta))
            sys.stdout.flush()

    with open('/srv/pypln/errors-export.txt', 'a') as fp:
        for error in errors:
            fp.write('{}\n'.format(error))


if __name__ == '__main__':
    main()
