# esfdw

esfdw is a PostgreSQL [foreign data wrapper](http://www.postgresql.org/docs/current/static/postgres-fdw.html) for Elasticsearch.

Elasticsearch is widely used for document and log data storage, in particular as part of the [ELK stack](https://www.elastic.co/webinars/introduction-elk-stack). esfdw allows PostgreSQL to be used as a query engine for data stored in Elasticsearch. Use cases include:
  * Writing SQL JOIN queries against Elasticsearch documents and letting the PostgreSQL engine do the heavy lifting
  * Running window functions on data stored in Elasticsearch
  * Applying PostgreSQL aggregations that do not currently have a native Elasticsearch equivalent

esfdw depends on [Multicorn](http://multicorn.org), a PostgreSQL extension for writing foreign data wrappers in Python.

## Features

#### Supported functionality

  * SELECT queries
  <a name="pushed_operators"></a>
  * Converting many common PostgreSQL operators to Elasticsearch filters, which are then used in the Elasticsearch query that retrieves the documents. This greatly improves performance by reducing the amount of data that needs to be fetched from Elasticsearch into PostgreSQL. Operators currently pushed down to Elasticsearch are `=`, `<>`, `LIKE`, `<@` (range), `<`, `<=`, `>`, and `>=`.
  * Using the [`fields` query parameter](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-fields.html) to reduce the amount of data transferred between Elasticsearch and PostgreSQL
  * Optionally, [converting PostgreSQL column names to Elasticsearch field names](#column_name_translation) to match the respective naming conventions of the databases
    * This functionality is opt-in and is enabled via the `column_name_translation` foreign table option
  * Specifying the document type and index or indices to use in the Elasticsearch query on a per-table basis
  * [Mostly-automatic generation](#mapping_to_schema) of `CREATE FOREIGN TABLE` DDL from Elasticsearch mappings
  * Estimating the resulting relation size so as to help the planner (`get_rel_size` Multicorn method)

#### Outstanding functionality

The following is not currently supported, but contributions are always welcome!

  * UPDATE or DELETE statements. In other words, the Multicorn write API is not implemented.
  * Pushing down sort (e.g., `ORDER BY`) qualifiers to Elasticsearch.

#### Caveats

esfdw issues Elasticsearch queries using the scan/scroll REST API. The performance, therefore, is greatly affected by the throughput and latency of the network connection between the PostgreSQL server and the Elasticsearch cluster.

While any valid PostgreSQL query can be run, only [some operators](#pushed_operators) are pushed down to Elasticsearch. In particular, aggregations are not pushed down at all. Where the query includes components that cannot be pushed down to Elasticsearch, the requisite data set is fetched into PostgreSQL, and any necessary query processing is then performed inside the PostgreSQL engine. For example, a `SELECT COUNT(*) FROM foo WHERE col = 'value'` PostgreSQL query will be translated to an Elasticsearch query with a `term` filter for field `col` and value `value`. All documents matching this filter will be fetched from Elasticsearch and will then be counted by PostgreSQL.

## Requirements

  * Multicorn 1.2.x and up.
  * esfdw has only been tested with Python 2.7, although there should be no major roadblocks to Python 3 support. If you try it with Python 3, let us know how it goes.
  * esfdw has been tested with Elasticsearch 1.7.x. However, the queries used by Elasticsearch are not exotic and should work with other Elasticsearch versions. Let us know if you run into problems.
  * esfdw uses the `elasticsearch-py` Elasticsearch client.

## Installation

  * [Install Multicorn](http://multicorn.org/#idid3).
  * Install the `elasticsearch` pip. **Please ensure that you install a version that's [appropriate for your version of Elasticsearch](http://elasticsearch-py.readthedocs.org/en/master/#compatibility).**
  * `python setup.py install` to install esfdw directly from the git repository.

## Usage

### Server creation

Example:

```sql
CREATE EXTENSION multicorn;
CREATE SERVER es_srv FOREIGN DATA WRAPPER multicorn OPTIONS (
    wrapper 'esfdw.ESForeignDataWrapper',
    hostname 'my.es_server.com',
    port '9200'
);
```

##### Options

  * `hostname` is the hostname of the Elasticsearch server. If unspecified, it defaults to `localhost`.
  * `port` is the port number for the connection to the Elasticsearch server. If unspecified, it defaults to `9200`.

### Foreign table creation

In the default implementation, a esfdw foreign table corresponds to exactly one doc_type. This doc_type is specified as a mandatory option to the `CREATE FOREIGN TABLE` statement.

Similarly, the `CREATE FOREIGN TABLE` statement also requires an option to specify the index or indices to use for the search ([multiple indices notation](https://www.elastic.co/guide/en/elasticsearch/reference/current/multi-index.html) is supported). If more sophisticated behaviour to derive the index name is desired (e.g., dynamically deriving the index based on the constraints in a query), this can be implemented by subclassing `esfdw.ESForeignDataWrapper` and overriding the `get_index` method.

Example:

```sql
CREATE FOREIGN TABLE foreign_es_table (
    col1 int,
    col2 text,
    nested_object__a text,
    nested_object__b text
) server es_srv OPTIONS (
    index 'logstash-2015.12.*',
    doc_type 'my_log',
    column_name_translation 'true'
);
```

##### Options

  * `index` is the value of the index parameter to use in Elasticsearch searches.
  * `doc_type` is the value of the doc_type parameter to use in Elasticsearch searches.
  <a name="column_name_translation"></a>
  * `column_name_translation` specifies whether PostgreSQL column name undergo translation when mapped to Elasticsearch field names. If the value of this option is `true`, the following translations occur:
    * An underscore (`_`) is converted to a dash (`-`)
    * A double underscore (`__`) is converted to a dot (`.`) and can be used for nested Elasticsearch fields
    * `timestamp` is mapped to `@timestamp` to match the common Logstash convention
    * For example, the PostgreSQL column name `foo__bar_baz` is converted to the Elasticsearch field `foo.bar-baz`

<a name="mapping_to_schema"></a>
#### Automatic (mostly) Elasticsearch mapping conversion to foreign table schema

The `esfdw.mapping_to_schema` module can be used to convert Elasticsearch field mappings to foreign table schema in a mostly-automatic way. Run `python -m esfdw.mapping_to_schema -h` for usage details.

Given a JSON file with Elasticsearch mappings (see https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html for the concept and https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-mapping.html for an explanation of how to extract from a running cluster), the script generates corresponding `CREATE FOREIGN TABLE` statements.

The script generates a foreign table per doc_type per index, with the name of the table derived from the name of the doc_type, except that dashes are replaced with underscores.

Column types are translated from the Elasticsearch equivalent and are always scalar. Nested objects are not represented as JSON; instead, a column definition is generated for every nested leaf field. Elasticsearch mappings do not contain an indication of whether the field is a list field, which means that the script cannot know when to make a column an array. The schema can be fixed up manually if array columns are desired.

Column names match the Elasticsearch field names except that [standard esfdw name translation rules are applied](#column_name_translation).

### Debugging

esfdw logs at the debug level the Elasticsearch queries that it issues. To see the message, you need to [configure the PostgreSQL log level](http://www.postgresql.org/docs/current/static/runtime-config-logging.html) to display DEBUG messages.

For example, to have the Elasticsearch query displayed in psql, run the following in your psql session:
`SET client_min_messages TO DEBUG;`

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Make sure the tests are passing (`python setup.py test`)
5. Push to the branch (`git push origin my-new-feature`)
6. Create a new Pull Request

Please include new tests with your changes! Tests live in the `tests` directory, and test files are named `<module>_test.py`, where `<module>` is the name of the module under test in the `esfdw` directory.
