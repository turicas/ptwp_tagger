# coding: utf-8

import sys
import pymongo

from django.conf import settings

from apps.core.models import Document


def main():
    if len(sys.argv) == 2:
        corpus_name = sys.argv[1]
        queryset = Document.objects.filter(corpus__name=corpus_name)
    else:
        queryset = Document.objects.all()

    conn = pymongo.connection.Connection(host=settings.MONGODB_CONFIG['host'],
            port=settings.MONGODB_CONFIG['port'])
    collection = conn['pypln']['analysis']

    total_size = 0
    total_documents = 0
    size = sys.getsizeof
    tuple_size = size((None, None, None)) # 3-elements tuple size
    for doc in queryset:
        doc_entry = collection.find_one({u"_id": u"id:{}:pos".format(doc.id)})
        if doc_entry is None:
            continue
        total_documents += 1

        doc_pos = doc_entry[u'value']
        if doc_pos is None:
            total_size += size(None)
        else:
            for token, tag, offset in doc_pos:
                total_size += size(token) + size(tag) + size(offset) + \
                              tuple_size

    total_size += size([None for i in range(total_documents)])
    print("Total documents: {}".format(total_documents))
    print("Total size: {}".format(total_size))


if __name__ == '__main__':
    main()
