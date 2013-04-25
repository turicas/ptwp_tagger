import sys
import pymongo

from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned

from apps.core.models import Document, gridfs_storage

if len(sys.argv) == 2:
    corpus_name = sys.argv[1]
    queryset = Document.objects.filter(corpus__name=corpus_name)
else:
    queryset = Document.objects.all()

conn = pymongo.connection.Connection(host="172.16.4.51", port=27020, safe=True)
collection = conn['ptwp_prod']['pages']

uploaded_docs, duplicated_docs, not_uploaded_docs = 0, 0, 0
duplicated_names = []

total_docs = collection.count()

for doc in collection.find(timeout=False):
    try:
        queryset.get(blob="/"+gridfs_storage.get_valid_name(u"{}.txt".format(doc["title"])))
    except ObjectDoesNotExist:
        uploaded = False
        not_uploaded_docs += 1
    except MultipleObjectsReturned:
        # por enquanto fica assim pra passarmos por ele de
        # novo depois.
        uploaded = False
        duplicated_docs += 1
        duplicated_names.append(doc.blob.name)
    else:
        uploaded = True
        uploaded_docs += 1

    collection.update({"_id": doc['_id']}, {"$set": {"uploaded": uploaded}})
    checked_docs = not_uploaded_docs + uploaded_docs
    sys.stdout.write("\r{:06d}/{:06d} ({:06d} not, {:06d} duplicated, "
            "{:06d} ok)".format(checked_docs, total_docs, not_uploaded_docs,
                duplicated_docs, uploaded_docs))
    sys.stdout.flush()
sys.stdout.write("\n{}".format(duplicated_names))
sys.stdout.flush()
