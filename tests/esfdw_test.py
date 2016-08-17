from datetime import datetime
from mock import MagicMock, patch
import unittest

from multicorn import Qual, ColumnDefinition

from esfdw.esfdw import ESForeignDataWrapper
from esfdw.es_helper import MatchList


class TestQualProcessing(unittest.TestCase):

    def setUp(self):
        self._fdw = ESForeignDataWrapper({'doc_type': 'foo_doc'}, [])

    def test_normalize_operator(self):
        for op in ('=', '~~', '<@', '<', '>', '<=', '>='):
            self.assertEqual(self._fdw._normalize_operator(op, 'foo'),
                             (op, False))
            self.assertEqual(self._fdw._normalize_operator(op, None),
                             (op, False))
            self.assertEqual(self._fdw._normalize_operator('!' + op, 'foo'),
                             (op, True))
            self.assertEqual(self._fdw._normalize_operator('!' + op, None),
                             (op, True))

        # <> is handled specially
        self.assertEqual(self._fdw._normalize_operator('<>', 'foo'),
                         ('=', True))
        self.assertEqual(self._fdw._normalize_operator('<>', None),
                         ('<>', False))

    def test_process_qual(self):
        must_list = MatchList()
        must_not_list = MatchList()
        self._fdw._process_qual(must_list, must_not_list, 'foo', '=', 'bar')
        self.assertEqual(len(must_list), 1)
        self.assertEqual(len(must_not_list), 0)
        self._fdw._process_qual(must_list, must_not_list, 'foo', '<>', 'bar')
        self.assertEqual(len(must_list), 1)
        self.assertEqual(len(must_not_list), 1)
        self._fdw._process_qual(must_list, must_not_list, 'foo', '~~', 'bar%')
        self.assertEqual(len(must_list), 2)
        self.assertEqual(len(must_not_list), 1)
        # Not pushing down ILIKE since we can't write a case-insensitive regexp
        # filter
        self._fdw._process_qual(must_list, must_not_list, 'foo', '~~*', 'bar%')
        self.assertEqual(len(must_list), 2)
        self.assertEqual(len(must_not_list), 1)

    def test_make_match_lists(self):
        quals = [
            Qual('foo', '=', 'bar'),
            Qual('quux', '<>', 'baz'),
            Qual('num', '<@', '[1, 10]'),
            Qual('a', ('=', True), ['x', 'y', 'z']),
            Qual('b', ('~~', True), ['d', 'e', 'f']),
            Qual('c', ('!~~', True), ['a%b', 'c_d', '_e%f%']),
            Qual('d', ('~~', False), ['g%', 'h_%', 'i%']),
            Qual('e', ('<>', False), [1, 2])
        ]
        ml, mnl = self._fdw._make_match_lists(quals)
        self.assertEqual(len(ml), 7)
        self.assertIn({'terms': {'a': ['x', 'y', 'z']}}, ml)
        self.assertIn({'or': [
            {'regexp': {'b': 'd'}},
            {'regexp': {'b': 'e'}},
            {'regexp': {'b': 'f'}}
        ]
        }, ml)
        self.assertIn({'prefix': {'d': 'g'}}, ml)
        self.assertIn({'regexp': {'d': 'h..*'}}, ml)
        self.assertIn({'prefix': {'d': 'i'}}, ml)
        self.assertEqual(len(mnl), 3)
        self.assertIn({'and': [
            {'regexp': {'c': 'a.*b'}},
            {'regexp': {'c': 'c.d'}},
            {'regexp': {'c': '.e.*f.*'}}
        ]
        }, mnl)
        self.assertIn({'terms': {'e': [1, 2]}}, mnl)


class TestAppendFilter(unittest.TestCase):

    def setUp(self):
        self._fdw = ESForeignDataWrapper({'doc_type': 'foo'}, [])
        self._ml = MatchList()

    def test_equals(self):
        self._fdw._append_filter(self._ml, 'foo', '=', 'bar')
        self.assertEqual(self._ml, [{'term': {'foo': 'bar'}}])

        self._ml = MatchList()
        self._fdw._append_filter(self._ml, 'foo', '=', 100)
        self.assertEqual(self._ml, [{'term': {'foo': 100}}])

        self._ml = MatchList()
        self._fdw._append_filter(
            self._ml, 'foo', '=', datetime(
                year=2015, month=7, day=1))
        self.assertEqual(
            self._ml, [{'term': {'foo': datetime(year=2015, month=7, day=1)}}])

    def test_not_equals(self):
        self._fdw._append_filter(self._ml, 'foo', '<>', 'baz')
        self.assertEqual(self._ml, [{'not': {'term': {'foo': 'baz'}}}])

        self._ml = MatchList()
        self._fdw._append_filter(self._ml, 'foo', '<>', 4)
        self.assertEqual(self._ml, [{'not': {'term': {'foo': 4}}}])

        self._ml = MatchList()
        self._fdw._append_filter(
            self._ml,
            'foo',
            '<>',
            datetime(
                year=2015,
                month=8,
                day=31,
                hour=10,
                minute=0,
                second=30))
        self.assertEqual(self._ml, [{'not': {'term': {'foo': datetime(
            year=2015, month=8, day=31, hour=10, minute=0, second=30)}}}])

    def test_is_none(self):
        self._fdw._append_filter(self._ml, 'foo', '=', None)
        self.assertEqual(self._ml, [{'missing': {'field': 'foo'}}])

    def test_is_not_none(self):
        self._fdw._append_filter(self._ml, 'foo', '<>', None)
        self.assertEqual(self._ml, [{'exists': {'field': 'foo'}}])

    def test_like(self):
        self._fdw._append_filter(self._ml, 'foo', '~~', 'bar%')
        self.assertEqual(self._ml, [{'prefix': {'foo': r'bar'}}])

        self._ml = MatchList()
        self._fdw._append_filter(self._ml, 'foo', '~~', '%bar%b_z')
        self.assertEqual(self._ml, [{'regexp': {'foo': r'.*bar.*b.z'}}])

        self._ml = MatchList()
        self._fdw._append_filter(self._ml, 'foo', '~~', 'foo%bar%')
        self.assertEqual(self._ml, [{'regexp': {'foo': r'foo.*bar.*'}}])

        self._ml = MatchList()
        self._fdw._append_filter(self._ml, 'foo', '~~', '_bar%')
        self.assertEqual(self._ml, [{'regexp': {'foo': r'.bar.*'}}])

    def test_range(self):
        self._fdw._append_filter(
            self._ml,
            'foo',
            '<@',
            '["2015-12-01 00:00:00","2015-12-02 01:00:00.123456")')
        self.assertEqual(self._ml, [{'range': {
            'foo': {
                'gte': '2015-12-01T00:00:00.000000',
                'lt': '2015-12-02T01:00:00.123456'
            }}}])

        self._ml = MatchList()
        self._fdw._append_filter(
            self._ml,
            'foo',
            '<@',
            '["2015-12-01 00:00:00.000001","2015-12-02 01:00:00"]')
        self.assertEqual(self._ml, [{'range': {
            'foo': {
                'gte': '2015-12-01T00:00:00.000001',
                'lte': '2015-12-02T01:00:00.000000'
            }}}])

        self._ml = MatchList()
        self._fdw._append_filter(
            self._ml,
            'foo',
            '<@',
            '("2015-12-01 00:00:00","2015-12-02 01:00:00"]')
        self.assertEqual(self._ml, [{'range': {
            'foo': {
                'gt': '2015-12-01T00:00:00.000000',
                'lte': '2015-12-02T01:00:00.000000'
            }}}])

        self._ml = MatchList()
        self._fdw._append_filter(
            self._ml,
            'foo',
            '<@',
            '("2015-12-01 00:00:00.123456","2015-12-02 01:00:00.987654")')
        self.assertEqual(self._ml, [{'range': {
            'foo': {
                'gt': '2015-12-01T00:00:00.123456',
                'lt': '2015-12-02T01:00:00.987654'
            }}}])

        self._ml = MatchList()
        self._fdw._append_filter(self._ml, 'foo', '<@', '[-1,5]')
        self.assertEqual(self._ml, [{'range': {
            'foo': {
                'gte': '-1',
                'lte': '5'
            }}}])

    def test_lt(self):
        self._fdw._append_filter(
            self._ml,
            'foo',
            '<',
            datetime(
                year=2015,
                month=12,
                day=1,
                hour=13,
                minute=24,
                second=59))
        self.assertEqual(self._ml, [{'range': {'foo': {'lt': datetime(
            year=2015, month=12, day=1, hour=13, minute=24, second=59)}}}])

        self._ml = MatchList()
        self._fdw._append_filter(self._ml, 'foo', '<', 4)
        self.assertEqual(self._ml, [{'range': {
            'foo': {
                'lt': 4
            }}}])

    def test_lte(self):
        self._fdw._append_filter(
            self._ml,
            'foo',
            '<=',
            datetime(
                year=2015,
                month=12,
                day=1,
                hour=13,
                minute=24,
                second=59))
        self.assertEqual(self._ml, [{'range': {'foo': {'lte': datetime(
            year=2015, month=12, day=1, hour=13, minute=24, second=59)}}}])

        self._ml = MatchList()
        self._fdw._append_filter(self._ml, 'foo', '<=', 4)
        self.assertEqual(self._ml, [{'range': {
            'foo': {
                'lte': 4
            }}}])

    def test_gt(self):
        self._fdw._append_filter(
            self._ml,
            'foo',
            '>',
            datetime(
                year=2015,
                month=12,
                day=1,
                hour=13,
                minute=24,
                second=59))
        self.assertEqual(self._ml, [{'range': {'foo': {'gt': datetime(
            year=2015, month=12, day=1, hour=13, minute=24, second=59)}}}])

        self._ml = MatchList()
        self._fdw._append_filter(self._ml, 'foo', '>', 4)
        self.assertEqual(self._ml, [{'range': {
            'foo': {
                'gt': 4
            }}}])

    def test_gte(self):
        self._fdw._append_filter(
            self._ml,
            'foo',
            '>=',
            datetime(
                year=2015,
                month=12,
                day=1,
                hour=13,
                minute=24,
                second=59))
        self.assertEqual(self._ml, [{'range': {'foo': {'gte': datetime(
            year=2015, month=12, day=1, hour=13, minute=24, second=59)}}}])

        self._ml = MatchList()
        self._fdw._append_filter(self._ml, 'foo', '>=', 4)
        self.assertEqual(self._ml, [{'range': {
            'foo': {
                'gte': 4
            }}}])


@patch('esfdw.esfdw.elasticsearch.Elasticsearch')
class TestESIntegrationPoints(unittest.TestCase):

    def setUp(self):
        self._columns = {
            'f__o_o': ColumnDefinition('f__o_o', type_name='text'),
            'bar': ColumnDefinition('bar', type_name='int'),
            'baz': ColumnDefinition('baz', type_name='text[]'),
            'quux': ColumnDefinition('quux', type_name='int')
        }
        self._fdw = ESForeignDataWrapper({'doc_type': 'foo_doc',
                                          'index': 'our_index'},
                                         self._columns)
        self._quals = [
            Qual('f__o_o', '=', 'value'),
            Qual('bar', '>', 5)
        ]

    @patch('esfdw.esfdw.scan')
    def test_execute(self, scan_mock, _elasticsearch_mock):
        scan_mock.return_value = [
            {'fields': {'f__o_o': ['value'], 'bar': [6], 'baz': ['a', 'b', 'c']}},
            {'fields': {'f__o_o': ['value'], 'bar': [7], 'baz': ['d', 'e', 'f']}},
            {'fields': {'f__o_o': ['value'], 'bar': [8], 'baz': ['g', 'h'], 'quux': ['hi']}}
        ]
        rows = list(
            self._fdw.execute(
                self._quals, [
                    'f__o_o', 'bar', 'baz', 'quux']))

        expected_query = {
            'fields': ['f__o_o', 'bar', 'baz', 'quux'],
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': [
                                {
                                    'term': {
                                        'f__o_o': 'value'
                                    },
                                },
                                {
                                    'range': {
                                        'bar': {
                                            'gt': 5
                                        }
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        }
        scan_mock.assert_called_once_with(
            self._fdw.esclient,
            query=expected_query,
            index='our_index',
            doc_type='foo_doc',
            size=self._fdw._SCROLL_SIZE,
            scroll=self._fdw._SCROLL_LENGTH)

        expected_rows = [
            {'f__o_o': 'value', 'bar': 6, 'baz': ['a', 'b', 'c'], 'quux': None},
            {'f__o_o': 'value', 'bar': 7, 'baz': ['d', 'e', 'f'], 'quux': None},
            {'f__o_o': 'value', 'bar': 8, 'baz': ['g', 'h'], 'quux': 'hi'}
        ]
        self.assertEqual(rows, expected_rows)

    def test_get_rel_size(self, _elasticsearch_mock):
        self._fdw.esclient.search.return_value = {
            'hits': {
                'total': 200
            }
        }
        rel_size = self._fdw.get_rel_size(self._quals, self._columns.keys())
        expected_query = {
            'size': 0,
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': [
                                {
                                    'term': {
                                        'f__o_o': 'value'
                                    },
                                },
                                {
                                    'range': {
                                        'bar': {
                                            'gt': 5
                                        }
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        }
        self._fdw.esclient.search.assert_called_once_with(
            index='our_index', body=expected_query, doc_type='foo_doc')
        self.assertEqual(rel_size, (200, 400))

    @patch('esfdw.esfdw.scan')
    def test_execute_column_name_translation(
            self, scan_mock, _elasticsearch_mock):
        columns = {
            'object__nested_field': ColumnDefinition(
                'object__nested_field',
                type_name='text')}
        fdw = ESForeignDataWrapper({'doc_type': 'foo_doc',
                                    'index': 'our_index',
                                    'column_name_translation': 'true'},
                                   columns)
        quals = [Qual('object__nested_field', '=', 'value')]
        scan_mock.return_value = [
            {'fields': {'object.nested-field': ['value']}}]
        rows = list(fdw.execute(quals, ['object__nested_field']))

        expected_query = {
            'fields': ['object.nested-field'],
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': [
                                {
                                    'term': {
                                        'object.nested-field': 'value'
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        }
        scan_mock.assert_called_once_with(
            fdw.esclient,
            query=expected_query,
            index='our_index',
            doc_type='foo_doc',
            size=fdw._SCROLL_SIZE,
            scroll=fdw._SCROLL_LENGTH)

        expected_rows = [{'object__nested_field': 'value'}]
        self.assertEqual(rows, expected_rows)

    @patch('esfdw.esfdw.scan')
    def test_id(
            self, scan_mock, _elasticsearch_mock):
        columns = {
            '_id': ColumnDefinition(
                '_id',
                type_name='text')}
        fdw = ESForeignDataWrapper({'doc_type': 'foo_doc',
                                    'index': 'our_index',
                                    'column_name_translation': 'true'},
                                   columns)
        quals = [Qual('_id', '=', 'value')]
        scan_mock.return_value = [
            {'_id': 'value'}]
        rows = list(fdw.execute(quals, ['_id']))

        expected_query = {
            'fields': ['_id'],
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'must': [
                                {
                                    'term': {
                                        '_id': 'value'
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        }
        scan_mock.assert_called_once_with(
            fdw.esclient,
            query=expected_query,
            index='our_index',
            doc_type='foo_doc',
            size=fdw._SCROLL_SIZE,
            scroll=fdw._SCROLL_LENGTH)

        expected_rows = [{'_id': 'value'}]
        self.assertEqual(rows, expected_rows)


if __name__ == '__main__':
    unittest.main()
