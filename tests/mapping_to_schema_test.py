from mock import patch
import unittest

from esfdw.mapping_to_schema import generate_table_spec, generate_schema, TableSpec, ColumnSpec


class TestMappingToSchema(unittest.TestCase):

    def test_generate_table_spec(self):
        mapping = {
            'index1': {
                'mappings': {
                    '_default_': {
                        'dynamic_templates': {}
                    },
                    'doc1': {
                        'properties': {
                            'a': {
                                'index': 'not_analyzed',
                                'type': 'string',
                                'doc_values': True
                            },
                            'b': {
                                'properties': {
                                    'c': {
                                        'properties': {
                                            'd': {
                                                'type': 'date',
                                                'format': 'dateOptionalTime'
                                            }
                                        }
                                    },
                                    'e': {
                                        'type': 'boolean'
                                    }
                                }
                            },
                            'f-f': {
                                'type': 'double'
                            },
                            'g': {
                                'type': 'long'
                            },
                            'h': {
                                'type': 'short'
                            }
                        }
                    },
                    'doc2': {
                        'properties': {
                            'a': {
                                'type': 'string'
                            }
                        }
                    },
                    'doc-3': {
                        'properties': {
                            'z': {
                                'type': 'boolean'
                            }
                        }
                    }
                }
            },
            'index2': {
                'mappings': {
                    'doc1': {
                        'properties': {
                            'aa': {
                                'type': 'date'
                            }
                        }
                    }
                }
            }
        }
        spec = sorted(list(generate_table_spec(mapping, ['index1'], [
                      'doc1', 'doc-3'])), key=lambda x: (x.index, x.name))
        self.assertEqual(
            spec, [
                TableSpec(
                    'doc1', [
                        ColumnSpec(
                            'a', 'text'), ColumnSpec(
                            'f_f', 'double precision'), ColumnSpec(
                            'b__c__d', 'timestamp'), ColumnSpec(
                                'b__e', 'boolean'), ColumnSpec(
                                    'g', 'bigint'), ColumnSpec(
                                        'h', 'smallint')], 'doc1', 'index1'), TableSpec(
                                            'doc_3', [
                                                ColumnSpec(
                                                    'z', 'boolean')], 'doc-3', 'index1')])

        spec = sorted(list(generate_table_spec(mapping, ['index1'], None)),
                      key=lambda x: (x.index, x.name))
        self.assertEqual(spec,
                         [TableSpec('doc1',
                                    [ColumnSpec('a', 'text'),
                                     ColumnSpec('f_f', 'double precision'),
                                        ColumnSpec('b__c__d', 'timestamp'),
                                        ColumnSpec('b__e', 'boolean'),
                                        ColumnSpec('g', 'bigint'),
                                        ColumnSpec('h', 'smallint')],
                                    'doc1', 'index1'),
                             TableSpec('doc2', [ColumnSpec('a', 'text')], 'doc2', 'index1'),
                             TableSpec('doc_3', [ColumnSpec('z', 'boolean')], 'doc-3', 'index1')]
                         )

        spec = sorted(list(generate_table_spec(
            mapping, [], ['doc1', 'doc-3'])), key=lambda x: (x.index, x.name))
        self.assertEqual(spec,
                         [TableSpec('doc1',
                                    [ColumnSpec('a', 'text'),
                                     ColumnSpec('f_f', 'double precision'),
                                        ColumnSpec('b__c__d', 'timestamp'),
                                        ColumnSpec('b__e', 'boolean'),
                                        ColumnSpec('g', 'bigint'),
                                        ColumnSpec('h', 'smallint')],
                                    'doc1', 'index1'),
                             TableSpec('doc_3', [ColumnSpec('z', 'boolean')], 'doc-3', 'index1'),
                             TableSpec('doc1', [ColumnSpec('aa', 'timestamp')], 'doc1', 'index2')],
                         )

    @patch('esfdw.mapping_to_schema.generate_table_spec')
    def test_generate_schema(self, generate_table_spec_mock):
        generate_table_spec_mock.return_value = [
            TableSpec(
                'table1', [
                    ColumnSpec(
                        'a', 'text'), ColumnSpec(
                        'b', 'integer')], 'table1', 'myindex')]
        expected_schema = [
            """DROP FOREIGN TABLE IF EXISTS table1;
CREATE FOREIGN TABLE table1 (
    a text,
    b integer
) SERVER es_srv OPTIONS (
    doc_type 'table1',
    index 'myindex',
    column_name_translation 'true'
);
"""]
        schema = list(generate_schema(None, None, None, 'es_srv'))
        self.assertEqual(expected_schema, schema)


if __name__ == '__main__':
    unittest.main()
