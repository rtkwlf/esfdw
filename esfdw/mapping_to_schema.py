from __future__ import print_function
import argparse
from collections import namedtuple
import copy
import json
import sys


ColumnSpec = namedtuple('ColumnSpec', 'column_name data_type')
TableSpec = namedtuple('TableSpec', 'name columns doc_type index')
# Separator for name components of nested objects, used to construct column
# names for a nested field.
# i.e., foo.bar.baz becomes foo__bar__baz
NESTED_FIELD_SEPARATOR = '__'
# A map from JSON types to PostgreSQL types.
# Types not in the map are assumed to have the same name in both type systems.
TYPE_MAP = {
    'string': 'text',
    'long': 'bigint',
    'short': 'smallint',
    'double': 'double precision',
    'date': 'timestamp'
}


def translate_es_name(name):
    """Translate Elasticsearch name to PostgreSQL name.

    Replaces dashes with underscores.
    """
    if name == '@timestamp':
        return 'timestamp'
    return name.replace('-', '_')


def generate_columns(mapping, path=None):
    """Given a dict representing ES field mappings for a particular object,
    generate ColumnSpec objects representing the corresponding columns.

    Nested objects are handled recursively, with `generate_columns` called for
    the mapping of each object. The recursive calls include a path argument,
    a list of the field names of the outer objects in which the current
    object is nested. This path is used to construct the column name.
    """
    if path is None:
        path = []
    for field in mapping:
        path_to_field = copy.copy(path)
        path_to_field.append(translate_es_name(field))
        if 'type' in mapping[field]:
            es_type = mapping[field]['type']
            yield ColumnSpec(NESTED_FIELD_SEPARATOR.join(path_to_field),
                             TYPE_MAP.get(es_type, es_type))
        else:
            # Nested object
            for column_spec in generate_columns(
                    mapping[field]['properties'], path=path_to_field):
                yield column_spec


def generate_table_spec(mapping, include_indices, include_doc_types):
    """Generate TableSpec objects corresponding to the mappings in `mapping`,
    a dict whose structure is expected to match the JSON that is returned by the
    get mapping API (https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-mapping.html).

    One TableSpec is generated per doc_type per index, with the name of the
    table derived from the doc_type.

    `include_indices` is a list that contains the indices that should be processed.
    If empty or None, all indices in the mapping are processed.

    `include_doc_types` is a list that contains the doc_types for which tables
    should be generated.
    If empty or None, all doc_types are included.
    """
    for index in mapping:
        if include_indices and index not in include_indices:
            continue
        for doc_type in mapping[index]['mappings']:
            # _default_ and .percolator are possible mapping keys that are not
            # doc_types
            if doc_type in (
                    '_default_',
                    '.percolator') or include_doc_types and doc_type not in include_doc_types:
                continue
            table = translate_es_name(doc_type)
            columns = list(
                generate_columns(
                    mapping[index]['mappings'][doc_type]['properties']))
            if columns:
                yield TableSpec(table, columns, doc_type, index)


def generate_schema(mapping, include_indices, include_doc_types, server):
    """Generate `DROP FOREIGN TABLE` and `CREATE FOREIGN TABLE` DDL from
    Elasticsearch field mappings.

    `server` is the name of the entity created with `CREATE SERVER`
    to use in the foreign table schema.

    All other arguments are as described in `generate_table_spec`.
    """
    for table_spec in generate_table_spec(
            mapping, include_indices, include_doc_types):
        columns = ',\n'.join(
            '    %s %s' %
            (col.column_name, col.data_type) for col in table_spec.columns)
        yield \
            """DROP FOREIGN TABLE IF EXISTS %(table)s;
CREATE FOREIGN TABLE %(table)s (
%(columns)s
) SERVER %(server)s OPTIONS (
    doc_type '%(doc_type)s',
    index '%(index)s',
    column_name_translation 'true'
);
""" % {'table': table_spec.name, 'columns': columns, 'server': server,
                'doc_type': table_spec.doc_type, 'index': table_spec.index}


def main():
    description = \
        """Generate foreign table schema from Elasticsearch mappings

Given a JSON file with Elasticsearch mappings (see https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html
for the format and https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-mapping.html
for an explanation of how to extract from a running cluster), generate
`CREATE FOREIGN TABLE` statements to create tables for use with esfdw.

The script generates a foreign table per doc_type per index, with the name of
the table derived from the name of the doc_type, except with dashes replaced with
underscores.

Column types are translated from the Elasticsearch equivalent and are always
scalar. Nested objects are not represented as JSON; instead, a column definition
is generated for every nested leaf field. Elasticsearch mappings do not contain
an indication of whether the field is a list field, which means that the script
cannot know when to make a column an array. The schema can be fixed up manually
if array columns are desired.

Column names match the Elasticsearch field names except for the following:
  * Dashes are replaced with single underscores
  * The field separator for nested objects is `__` rather than `.`
    That is, the Elasticsearch field foo.bar.baz is mapped to the column name foo__bar__baz
"""
    parser = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument(
        '-d',
        '--doc-types',
        nargs='+',
        help='doc_type names for which to generate foreign table definitions')
    parser.add_argument(
        '-i',
        '--indices',
        nargs='+',
        help='names of indices whose mappings should be processed when generating foreign table definitions')
    parser.add_argument(
        '-s',
        '--server',
        required=True,
        help='name of the PostgreSQL server object to use in the foreign table definition')
    args = parser.parse_args()

    mapping = json.load(sys.stdin)
    print(
        '\n'.join(
            generate_schema(
                mapping,
                args.indices,
                args.doc_types,
                args.server)))


if __name__ == "__main__":
    main()
