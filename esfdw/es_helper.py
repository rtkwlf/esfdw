class MatchList(list):
    """Helper for building ES query objects. Boolean queries take a list of sub-terms. This
    is designed to be able programmatically build the sub-terms. This object can be
    treated as a list.
    """

    def append_filter(self, new_filter, not_value=False, constant_score=None):
        """Append a filter to our list. Enclose in a 'not' if requested"""
        if not_value:
            new_filter = {
                'not': new_filter
            }
        if constant_score:
            new_filter = {
                'constant_score': {
                    'filter': new_filter,
                    'boost': constant_score
                }
            }
        self.append(new_filter)

    def append_range(
            self,
            key,
            not_value=False,
            constant_score=None,
            **kwargs):
        """Append a range filter.

        See http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-range-filter.html
        for parameters
        """
        self.append_filter(
            {
                'range': {key: kwargs}
            }, not_value, constant_score
        )

    def append_exists(self, key, not_value=False, constant_score=None):
        """An exists test. Add to a must-not filter to test for not exists"""
        self.append_filter(
            {
                'exists': {'field': key}
            }, not_value, constant_score
        )

    def append_missing(self, key, not_value=False, constant_score=None):
        """A missing test."""
        self.append_filter(
            {
                'missing': {'field': key}
            }, not_value, constant_score
        )

    def append_term(self, key, value, not_value=False, constant_score=None):
        """Single term filter. See if the given key has the given value"""
        if value is not None:
            self.append_filter(
                {
                    'term': {key: value}
                }, not_value, constant_score
            )

    def append_prefix(self, key, value, not_value=False, constant_score=None):
        """Single prefix filter. See if the given key has the given value as its prefix"""
        if value is not None:
            self.append_filter(
                {
                    'prefix': {key: value}
                }, not_value, constant_score
            )

    def append_terms(
            self,
            key,
            value_list,
            not_value=False,
            constant_score=None):
        """Multi-term filter. See if the given key has one of the given value_list"""
        if value_list is not None:
            self.append_filter(
                {
                    'terms': {key: value_list}
                }, not_value, constant_score
            )


def get_filtered_query(must_list=None, must_not_list=None):
    """Get the correct query string for a boolean filter. Accept must and
    must_not lists. Use MatchList for generating the appropriate lists.
    """
    bool_filter = {}
    if must_list:
        bool_filter['must'] = must_list
    if must_not_list:
        bool_filter['must_not'] = must_not_list
    result = {
        'query': {
            'filtered': {
                'filter': {
                    'bool': bool_filter
                }
            }
        }
    }
    return result
