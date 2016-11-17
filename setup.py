from setuptools import setup

long_description = \
"""esfdw is a PostgreSQL `foreign data
wrapper <http://www.postgresql.org/docs/current/static/postgres-fdw.html>`__
for Elasticsearch.

Elasticsearch is widely used for document and log data storage, in
particular as part of the `ELK
stack <https://www.elastic.co/webinars/introduction-elk-stack>`__. esfdw
allows PostgreSQL to be used as a query engine for data stored in
Elasticsearch. Use cases include:

- Writing SQL JOIN queries against Elasticsearch documents and letting the PostgreSQL engine do the heavy lifting
- Running window functions on data stored in Elasticsearch
- Applying PostgreSQL aggregations that do not currently have a native Elasticsearch equivalent

esfdw depends on `Multicorn <http://multicorn.org>`__, a PostgreSQL
extension for writing foreign data wrappers in Python.
"""

setup(
    name='esfdw',
    description='PostgreSQL foreign data wrapper for Elasticsearch',
    long_description=long_description,
    version='0.1.1',
    author='Arctic Wolf Networks, Inc.',
    author_email='info@arcticwolf.com',
    license='MIT',
    packages=['esfdw'],
    test_suite='tests',
    url='https://github.com/rtkwlf/esfdw',
    download_url='https://github.com/rtkwlf/esfdw/tarball/0.1.1'
)
