# coding: utf-8

'''Writer and Reader classes that mimic NLTK Corpus API

These classes can mimic NLTK TaggedCorpus classes. Still need to add support
for document categories, chunks, paragrahs and sentences.
Status of implemented methods:
    - `fileids`: implemented
    - `readme`: implemented
    - `categories`: not implemented
    - `raw`: implemented only for documents (do not retrieve entire corpus
             because it could be too much data)
    - `paras`: not impelmented
    - `sents`: not implemented
    - `words`: implemented only for documents
    - `tagged_paras`: not implemented
    - `tagged_sents`: not implemented
    - `tagged_words`: implemented only for documents
    - `chunked_paras`: not implemented
    - `chunked_sents`: not implemented
    - `chunked_words`: not implemented
'''

from json import dumps as json_dumps, loads as json_loads
import sqlite3
from gzip import zlib


compress = zlib.compress
decompress = zlib.decompress


def serialize(obj):
    return buffer(compress(json_dumps(obj)))


def deserialize(obj):
    return json_loads(decompress(obj))


class TaggedSQLiteCorpusWriter(object):
    '''Write a part-of-speech tagged corpus on a SQLite database

    You can incrementally add documents to this corpus, just instantiate
    another `TaggedSQLiteCorpusWriter` object when needed and call
    `add_document` as many times as needed.
    You can also update metadata information.
    '''

    def __init__(self, filename):
        '''Initialize the object, open the database

        `filename` is the name of sqlite database to be created. Prefer to use
        `.sqlite` extension.
        '''
        self._db = sqlite3.Connection(filename)
        self._cursor = self._db.cursor()
        self._create_tables()

    def _create_tables(self):
        queries = [
        'CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)',
        'CREATE INDEX IF NOT EXISTS idx_meta_key ON meta (key)',
        '''CREATE TABLE IF NOT EXISTS document (name TEXT PRIMARY KEY,
                                                text BLOB, tokens BLOB,
                                                pos BLOB)''',
        'CREATE INDEX IF NOT EXISTS idx_document_name ON document (name)',
        ]
        for query in queries:
            self._cursor.execute(query)
        self._db.commit()

    def __del__(self):
        self._cursor.close()
        self._db.commit()
        self._db.close()

    @property
    def metadata(self):
        return dict(self._cursor.execute('SELECT key, value FROM meta'))

    @metadata.setter
    def metadata(self, new_value):
        if not isinstance(new_value, dict):
            raise ValueError('`metadata` must be a dictionary')
        self._cursor.execute('DELETE FROM meta')
        for key, value in new_value.items():
            self._cursor.execute('INSERT INTO meta (key, value) VALUES (?, ?)',
                                 (key, value))
        self._db.commit()

    def add_document(self, document_name, raw_text, tagged_tokens,
                     commit=True):
        '''Add a document to this corpus, commiting in the end

        Raises ValueError if a document with the same name already exists.
        '''
        query = '''INSERT INTO document (name, text, tokens, pos) VALUES
                (?, ?, ?, ?)'''
        raw_text = serialize(raw_text)
        tokens = serialize([element[0] for element in tagged_tokens])
        tagged_tokens = serialize(tagged_tokens)
        try:
            self._cursor.execute(query, (document_name, raw_text, tokens,
                                         tagged_tokens))
        except sqlite3.IntegrityError:
            raise ValueError('Document `{}` already exists'.format(document_name))
        else:
            if commit:
                self._db.commit()

    def commit(self):
        self._db.commit()


class TaggedSQLiteCorpusReader(object):
    def __init__(self, filename):
        self._db = sqlite3.Connection(filename)
        self._cursor = self._db.cursor()

    def __del__(self):
        # do not need to commit since this object is read-only
        self._cursor.close()
        self._db.close()

    @property
    def metadata(self):
        return dict(self._cursor.execute('SELECT key, value FROM meta'))

    def readme(self):
        # this method exists just to be compatible with NLTK API, since
        # 'readme' is a corpus metadata like any other
        query = 'SELECT value FROM meta WHERE key="readme"'
        results = list(self._cursor.execute(query))
        if not results:
            raise NotImplemented('README not provided for this corpus')
        else:
            return results[0][0]

    def fileids(self):
        results = self._cursor.execute('SELECT name FROM document')
        return [result[0] for result in results]

    def _get_document_property(self, name, property_name):
        query = 'SELECT {} FROM document WHERE name=?'.format(property_name)
        results = list(self._cursor.execute(query, (name, )))
        if not results:
            raise ValueError('Document `{}` does not exist on corpus'.format(name))
        else:
            return deserialize(results[0][0])

    def raw(self, fileid):
        return self._get_document_property(fileid, 'text')

    def words(self, fileid):
        return self._get_document_property(fileid, 'tokens')

    def tagged_words(self, fileid):
        pos = self._get_document_property(fileid, 'pos')
        return [tuple(element) for element in pos]
