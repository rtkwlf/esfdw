from datetime import datetime
import logging
import re

from multicorn import ForeignDataWrapper, ANY, ALL
from multicorn.utils import log_to_postgres

import elasticsearch
from elasticsearch.helpers import scan

from .es_helper import MatchList, get_filtered_query


class ESForeignDataWrapper(ForeignDataWrapper):

    # These operators and negated variants (prefixed with a '!') can be
    # offloaded to ElasticSearch.
    _PUSHED_DOWN_OPERATORS = ['=', '<>', '~~', '<@', '<', '>', '<=', '>=']

    # How many results should be returned in response to a single scroll request.
    # A lower value increases the number of round trips to the ElasticSearch
    # server to fetch the data.
    # A higher value increases the memory pressure on the ElasticSearch client
    # node issuing the search.
    _SCROLL_SIZE = 5000

    # How long a consistent view of the index should be maintained for scrolled
    # search. The default is 5 minutes.
    # This should be greater than the time it takes for an ES query to complete.
    # On the other hand, unnecessarily high values needlessly increase the load
    # on the cluster.
    _SCROLL_LENGTH = '5m'

    def __init__(self, options, columns):
        super(ESForeignDataWrapper, self).__init__(options, columns)
        self._esclient = None
        self._options = options
        self._columns = columns
        self._doc_type = options['doc_type']
        if options.get('column_name_translation') == 'true':
            self._column_to_es_field = self.convert_column_name
        else:
            self._column_to_es_field = lambda column: column

    @property
    def esclient(self):
        if self._esclient is None:
            params = {}
            if 'hostname' in self._options:
                params['host'] = self._options['hostname']
            if 'port' in self._options:
                params['port'] = self._options['port']
            self._esclient = elasticsearch.Elasticsearch([params])
        return self._esclient

    def get_index(self, _quals):
        """Get the ElasticSearch index or indices to query.

        By default, we obtain the index from the foreign table options.
        However, this method can be overridden to derive the index from the query
        quals. For example, the `timestamp` qual could be used to select one or
        more time-based indices.
        """
        return self._options['index']

    def convert_column_name(self, column):
        """Given a column name, return the corresponding Elasticsearch field name.

        The default implementation replaces `__` with `.` (allowing for specifying
        fields in nested objects) and `_` with `-` (to reflect common ElasticSearch
        convention).

        `_id` is left untranslated.
        `timestamp` is converted to `@timestamp` to match Logstash conventions.

        This method is used only for foreign tables that were created with the option
        `column_name_translation` set to the value `true`.

        This method can be overridden in a subclass if a different implementation
        is desired.
        """
        if column == '_id':
            return column
        elif column == 'timestamp':
            return '@timestamp'
        return column.replace('__', '.').replace('_', '-')

    def _endpoint_to_datetime(self, endpoint):
        # When dealing with date and time ranges, we get a string formatted as
        # %Y-%m-%d %H:%M:%S(.%f)?
        valid_time_formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f']
        for fmt in valid_time_formats:
            try:
                return datetime.strptime(endpoint, fmt)
            except ValueError:
                pass
        # No expected time formats matched, so no conversion
        return endpoint

    def _append_filter(self, filter_list, field, operator, value):
        if operator == '=':
            if value is None:
                filter_list.append_missing(field)
            else:
                filter_list.append_term(field, value)
        elif operator == '<>':
            if value is None:
                filter_list.append_exists(field)
            else:
                filter_list.append_term(field, value, not_value=True)
        elif operator == '~~':
            # LIKE operator
            if value.find('%') == len(value) - 1 and '_' not in value:
                filter_list.append_prefix(field, value[:-1])
            else:
                value = re.escape(value).replace(
                    r'\%', '.*').replace(r'\_', '.')
                filter_list.append_filter({'regexp': {field: value}})
        elif operator == '<@':
            # range operator
            # value looks something like ["start","end")
            # (including quotation marks when dealing with date strings)
            def _format_endpoint(endpoint):
                endpoint = self._endpoint_to_datetime(endpoint)
                if isinstance(endpoint, datetime):
                    return endpoint.strftime('%Y-%m-%dT%H:%M:%S.%f')
                return endpoint

            start, end = value.split(',')
            if start[0] == '[':
                start_op = 'gte'
            else:
                start_op = 'gt'
            start = start[1:].replace('"', '')

            if end[-1] == ']':
                end_op = 'lte'
            else:
                end_op = 'lt'
            end = end[:-1].replace('"', '')

            params = {
                start_op: _format_endpoint(start),
                end_op: _format_endpoint(end)
            }
            filter_list.append_range(field, **params)
        elif operator in ('<', '<=', '>', '>='):
            operator_map = {
                '<': 'lt',
                '<=': 'lte',
                '>': 'gt',
                '>=': 'gte'
            }
            params = {
                operator_map[operator]: value
            }
            filter_list.append_range(field, **params)

    def _normalize_operator(self, operator, value):
        negation = False
        if operator[0] == '!':
            # Negative operators are handled as their positive counterparts
            # but are added to the must_not list.
            operator = operator[1:]
            negation = True
        if operator == '<>' and value is not None:
            # Generally handle a not-equals in the must_not list.
            # IS NOT NULL, which is passed down to us as '<> None', is handled
            # as a `missing` filter in the must_list.
            operator = '='
            negation = True
        return operator, negation

    def _process_qual(self, must_list, must_not_list, field, operator, value):
        operator, negation = self._normalize_operator(operator, value)
        if operator in self._PUSHED_DOWN_OPERATORS:
            filter_list = must_not_list if negation else must_list
            self._append_filter(filter_list, field, operator, value)

    def _make_match_lists(self, quals):
        must_list = MatchList()
        must_not_list = MatchList()
        for qual in quals:
            field = self._column_to_es_field(qual.field_name)
            if qual.list_any_or_all == ANY:
                if qual.operator[0] == '=':
                    must_list.append_terms(field, qual.value)
                else:
                    match_list = MatchList()
                    operator, negation = self._normalize_operator(
                        qual.operator[0], True)
                    for elem in qual.value:
                        self._append_filter(match_list, field, operator, elem)
                    if negation:
                        # a <> any(x, y, z) => a <> x or a <> y or a <> z =>
                        # not (a = x and a = y and a = z)
                        must_not_list.append_filter({'and': match_list})
                    else:
                        must_list.append_filter({'or': match_list})
            elif qual.list_any_or_all == ALL:
                if qual.operator[0] == '<>':
                    # a <> all(x, y, z) => a <> x and a <> y and a <> z => not
                    # (a = x or a = y or a = z)
                    must_not_list.append_terms(field, qual.value)
                else:
                    for elem in qual.value:
                        self._process_qual(
                            must_list, must_not_list, field, qual.operator[0], elem)
            else:
                self._process_qual(
                    must_list,
                    must_not_list,
                    field,
                    qual.operator,
                    qual.value)
        return must_list, must_not_list

    def execute(self, quals, columns, _sortkeys=None):
        must_list, must_not_list = self._make_match_lists(quals)
        if must_list or must_not_list:
            query = get_filtered_query(
                must_list=must_list,
                must_not_list=must_not_list)
        else:
            query = {}
        # It's not clear if we should be using `fields` or `_source` here.
        # `fields` is useful for "stored" fields, which are stored separately
        # from the main _source JSON document. The idea is that the entire document
        # does not need to be reparsed when loading only a subset of the fields.
        # When fields aren't "stored" but are doc_values, they still seem to be
        # stored independently.
        # Tests suggest that at least in some cases when dealing with doc_values,
        # `fields` 1.16 times better than `_source`.
        query['fields'] = [self._column_to_es_field(
            column) for column in columns]
        # When using fields, the values always come back in an array, to make for
        # more consistent treatment of any actual array fields that we may have
        # requested. If the field is not truly an array field, the value comes back
        # in an array of one element.
        default_value = [None]
        log_to_postgres('query: %s' % query, logging.DEBUG)
        for result in scan(
                self.esclient,
                query=query,
                index=self.get_index(quals),
                doc_type=self._doc_type,
                size=self._SCROLL_SIZE,
                scroll=self._SCROLL_LENGTH):
            obs = result.get('fields', {})

            def _massage_value(value, column):
                if column == '_id':
                    # `_id` is special in that it's always present in the top-level
                    # result, not under `fields`.
                    return result['_id']
                # If the column type is an array, return the list.
                # Otherwise, return the first element of the array.
                if self._columns[column].type_name.endswith('[]'):
                    return value
                return value[0]
            row = {
                column: _massage_value(
                    obs.get(
                        self._column_to_es_field(column),
                        default_value),
                    column) for column in columns}
            yield row

    def get_rel_size(self, quals, columns):
        must_list, must_not_list = self._make_match_lists(quals)
        if must_list or must_not_list:
            query = get_filtered_query(
                must_list=must_list,
                must_not_list=must_not_list)
        else:
            query = {}
        query['size'] = 0
        results = self.esclient.search(
            index=self.get_index(quals),
            body=query,
            doc_type=self._doc_type)
        return (results['hits']['total'], len(columns) * 100)
