# coding: utf-8

import sys

from datetime import timedelta
from time import time

import pymongo

from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned

from apps.core.models import Document, gridfs_storage



def main():
    if len(sys.argv) != 2:
        sys.exit(1)

    corpus_name = sys.argv[1]
    queryset = Document.objects.filter(corpus__name=corpus_name)
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
    page_titles = set(u'/' + gridfs_storage.get_valid_name(
        u'{}.txt'.format(doc['title'])) for doc in collection.find(
            {}, {'title': 1, '_id': 1}, timeout=False))
    delta_time = time() - start_time
    sys.stdout.write('OK! Duration = {}, len = {}\n'.format(timedelta(delta_time / day_per_second), len(page_titles)))
    sys.stdout.flush()


    start_time = time()
    sys.stdout.write('Retrieving blob names...\n')
    sys.stdout.flush()
    blob_names = set(queryset.values_list('blob', flat=True))
    delta_time = time() - start_time
    sys.stdout.write('OK! Duration = {}, len = {}\n'.format(timedelta(delta_time / day_per_second), len(blob_names)))
    sys.stdout.flush()

    diff = blob_names - page_titles
    sys.stdout.write("Filenames that don't exist in mongodb: {}\n".format(
        len(diff)))
    sys.stdout.flush()


if __name__ == '__main__':
    main()
