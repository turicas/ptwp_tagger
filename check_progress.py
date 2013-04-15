import sys
import pymongo

from django.conf import settings

from apps.core.models import Document

if len(sys.argv) == 2:
    corpus_name = sys.argv[1]
    queryset = Document.objects.filter(corpus__name=corpus_name)
else:
    queryset = Document.objects.all()

conn = pymongo.connection.Connection(host=settings.MONGODB_CONFIG['host'],
        port=settings.MONGODB_CONFIG['port'])
collection = conn['pypln']['analysis']

expected_properties = set([u'mimetype', u'freqdist',
    u'average_sentence_repertoire', u'language', u'average_sentence_length',
    u'sentences', u'pos', u'momentum_1', u'momentum_2', u'momentum_3',
    u'momentum_4', u'file_metadata', u'tokens', u'repertoire', u'text',
    u'tagset'])

inexistent_ids = []
incomplete_ids = []
total_documents = len(queryset)

for doc in queryset:
    doc_entry = collection.find_one(
        {u"_id": u"id:{}:_properties".format(doc.id)})


    if doc_entry is None:
        print("Document {} ({}) not found in mongodb.".format(doc, doc.id))
        inexistent_ids.append(doc.id)
        continue

    doc_properties = set(doc_entry[u'value'])
    if doc_properties != expected_properties:
        print("Properties for document {} ({}) differ from expected.".format(doc,
            doc.id))
        diff = expected_properties - doc_properties
        print("\t\tDiff: {}".format(diff))
        incomplete_ids.append(doc.id)

print("Total documents: {}".format(total_documents))
print("Inexistent documents: {}".format(len(inexistent_ids)))
print("Incomplete documents: {}".format(len(incomplete_ids)))
