# coding: utf-8

import sys

from datetime import timedelta
from time import time

import pymongo

from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned

from apps.core.models import Document, gridfs_storage


if len(sys.argv) == 2:
    corpus_name = sys.argv[1]
    queryset = Document.objects.filter(corpus__name=corpus_name)
else:
    sys.exit(1)

def main():
    conn = pymongo.connection.Connection(host="172.16.4.51", port=27020, safe=True)
    collection = conn['ptwp_prod']['pages']

    uploaded_docs, not_uploaded_docs = 0, 0

    total_docs = collection.count()
    start_time = time()
    last_time = time()
    last_checked_docs = 0
    day_per_second = 3600 * 24

    start_time = time()
    sys.stdout.write('Retrieving page titles...\n')
    sys.stdout.flush()
    page_titles = [(doc['_id'], doc['title']) for doc in collection.find({}, {'title': 1, '_id': 1}, timeout=False)]
    delta_time = time() - start_time
    sys.stdout.write('OK! Duration = {}, len = {}\n'.format(timedelta(delta_time / day_per_second), len(page_titles)))
    sys.stdout.flush()


    start_time = time()
    sys.stdout.write('Retrieving blob names...\n')
    sys.stdout.flush()
    blob_names = {x: True for x in queryset.values_list('blob', flat=True)}
    delta_time = time() - start_time
    sys.stdout.write('OK! Duration = {}, len = {}\n'.format(timedelta(delta_time / day_per_second), len(blob_names)))
    sys.stdout.flush()

    valid_name = gridfs_storage.get_valid_name
    for index, (doc_id, page_title) in enumerate(page_titles, start=1):
        filename = u'/' + valid_name(u'{}.txt'.format(page_title))
        if filename in blob_names:
            uploaded = True
            uploaded_docs += 1
        else:
            uploaded = False
            not_uploaded_docs += 1
        collection.update({"_id": doc_id}, {"$set": {"uploaded": uploaded}})

        if index % 10 == 0:
            checked_docs = not_uploaded_docs + uploaded_docs
            total_time = timedelta((time() - start_time) / day_per_second)
            rate = (checked_docs - last_checked_docs) / (time() - last_time)
            eta = timedelta(((total_docs - checked_docs) / rate) / day_per_second)
            sys.stdout.write("\r{:06d}/{:06d} ({:06d} not, "
                    "{:06d} ok) {} ETA: {}".format(checked_docs, total_docs, not_uploaded_docs,
                        uploaded_docs, total_time, eta))
            sys.stdout.flush()
            last_checked_docs = checked_docs
            last_time = time()


if __name__ == '__main__':
    main()
