# Oracle Select

An easy interface for reading from an Oracle database with cx_Oracle.


## To use:

Usage is meant to be simple. Provide your username, password, and host address when you instantiate the DB class. Each select statement opens and closes a new connection. Results are returned as lists of dicts or an empty list if no results are found. For each result, any LOB-like fields are read into strings/bytes for easy handling.

    >>> from oracle_select import DB
    >>> db = DB(username='your.username', password='password', host='DBNAME.WORLD')
    >>> sql = """select * from ps_table"""
    >>> db.select(sql)


### Using bind variables

Bind variables can be passed to the select statement in two ways:

As a dictionary when referencing named sql variables:

    >>> sql = """select * from ps_table where emplid = :emplid"""
    >>> db.select(sql, binds={'emplid': '12345678'})

Or as a tuple when referencing numeric sql variables:

    >>> sql = """select * from ps_table where emplid = :1"""
    >>> db.select(sql, binds=('12345678',))


### Fetch One vs. Many vs. All

By default all results will be returned. Sometimes, when the result set is large, this is a bad idea. To limit your result set, modify the fetch parameter. Then the select statement will only return N results.

    >>> sql = """select * from ps_table"""
    >>> db.select(sql, fetch=10)  # fetch first ten results
    >>> db.select(sql, fetch=0)  # fetch all results (default)


### Fetch Large Datasets with a Generator

Large datasets can be retrieved using the `select_iter` method:

    >>> sql = """select * from ps_large_table"""
    >>> for chunk in db.select_iter(sql):
    >>>    for result in chunk:
    >>>        print(result)


Results will be returned in chunks of `fetch_size`. To limit the total number of results, set `max_rows`. By default chunks of 1,000 records will be returned until the entire result set has been consumed (`max_rows = None`).


## Utils

The `oracle_select.utils` module provides some useful tools for working with PeopleSoft, including an interface for polling the process monitor.
