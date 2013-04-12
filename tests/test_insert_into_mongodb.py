# coding: utf-8

import unittest

from textwrap import dedent

from insert_into_mongodb import parse_doc, parse_docs


class TestInsertMongo(unittest.TestCase):
    def test_extract_page_from_txt(self):
        contents = dedent('''
        <doc id="0" url="http://eggs" title="ham">
        ham

        this is a test.
        </doc>
        ''').strip()
        result = parse_doc(contents)
        expected = {'id': '0', 'url': 'http://eggs', 'title': 'ham',
                    'text': 'this is a test.'}
        self.assertEqual(result, expected)

    def test_extract_page_from_txt_empty_text(self):
        contents = dedent('''
        <doc id="0" url="http://eggs" title="ham">
        ham


        </doc>
        ''').strip()
        result = parse_doc(contents)
        expected = {'id': '0', 'url': 'http://eggs', 'title': 'ham',
                    'text': ''}
        self.assertEqual(result, expected)

    def test_extract_page_from_txt_removing_some_tags(self):
        contents = dedent('''
        <doc id="0" url="http://eggs" title="ham">
        ham

        bla bla bla
        this is some page.</math>
        </ref>
        </doc>
        ''').strip()
        result = parse_doc(contents)
        expected = {'id': '0', 'url': 'http://eggs', 'title': 'ham',
                    'text': 'bla bla bla\nthis is some page.'}
        self.assertEqual(result, expected)

    def test_parse_docs(self):
        contents = dedent('''
        <doc id="0" url="http://eggs" title="ham">
        ham

        bla bla bla
        </doc>
        <doc id="1" url="http://eggs-1" title="ham_1">
        ham 1

        wikipedia rules
        </doc>
        <doc id="2" url="http://eggs-2" title="ham_2">
        ham 2

        python too
        </doc>
        ''').strip()
        result = parse_docs(contents)
        expected = [{'id': '0', 'url': 'http://eggs', 'title': 'ham',
                     'text': 'bla bla bla'},
                    {'id': '1', 'url': 'http://eggs-1', 'title': 'ham 1',
                     'text': 'wikipedia rules'},
                    {'id': '2', 'url': 'http://eggs-2', 'title': 'ham 2',
                     'text': 'python too'}, ]
        self.assertEqual(result, expected)
