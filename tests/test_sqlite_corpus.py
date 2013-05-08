# coding: utf-8

import os

import nltk

from sqlite_corpus import TaggedSQLiteCorpusWriter, TaggedSQLiteCorpusReader


def test():
    corpus_filename = 'my_corpus.sqlite'

    try:
        os.unlink(corpus_filename)
    except OSError:
        pass

    documents = {'test1': 'This is a test.',
                 'test2': 'Another test!',
                 'testN': 'This is the N-th test. Enough.'}
    writer = TaggedSQLiteCorpusWriter(corpus_filename)
    writer.metadata = {'name': 'test corpus', 'readme': 'bla bla bla'}
    for document_name, raw_text in documents.items():
        pos = nltk.pos_tag(nltk.word_tokenize(raw_text))
        writer.add_document(document_name, raw_text, pos)

    document_name, raw_text = documents.items()[0]
    pos = nltk.pos_tag(nltk.word_tokenize(raw_text))
    try:
        writer.add_document(document_name, raw_text, pos)
    except ValueError:
        assert True
    else:
        assert False, 'ValueError not raised'

    reader = TaggedSQLiteCorpusReader(corpus_filename)
    assert reader.metadata == {'name': 'test corpus', 'readme': 'bla bla bla'}

    assert reader.fileids() == ['test1', 'test2', 'testN']

    doc = reader.fileids()[0]
    assert reader.raw(doc) == 'This is a test.'

    assert reader.words(doc) == ['This', 'is', 'a', 'test', '.']

    assert reader.tagged_words(doc) == [('This', 'DT'), ('is', 'VBZ'),
                                        ('a', 'DT'), ('test', 'NN'),
                                        ('.', '.')]

    os.unlink(corpus_filename)
