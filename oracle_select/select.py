import copy
import cx_Oracle
from collections import namedtuple


def makeDictFactory(cursor):
    """
    Return a dict of values after reading any LOB-like objects
    """
    col_names = [d[0].lower() for d in cursor.description]
    def createRow(*args):
        read_args = []
        for arg in args:
            if isinstance(cx_Oracle.LOB, cx_Oracle.CLOB):
                v = arg.read()
                try:
                    arg.close()
                    del arg
                except:
                    pass
            else:
                v = arg
            read_args.append(v)
        del args
        return dict(zip(col_names, read_args))
    return createRow

def makeNamedTupleFactory(cursor):
    """
    Return a named tuple of values after reading any LOB-like objects
    """
    fieldnames = [d[0].lower() for d in cursor.description]
    record = namedtuple('Record', field_names=fieldnames)
    def createRow(*args):
        read_args = []
        for arg in args:
            if isinstance(cx_Oracle.LOB, cx_Oracle.CLOB):
                v = arg.read()
                try:
                    arg.close()
                    del arg
                except:
                    pass
            else:
                v = arg
            read_args.append(v)
        del args
        return record._make(read_args)
    return createRow


class DB(object):
    """Database object"""

    def __init__(self, host, username, password, schema=None):
        self.host = host
        self.username = username
        self.password = password
        self.schema = schema

    def select(self, sql, binds=None, fetch=0, rowtype='dict'):
        db = cx_Oracle.connect(user=self.username,
                               password=self.password,
                               dsn=self.host)
        db.current_schema = self.schema
        c = db.cursor()

        try:
            if binds:
                c.execute(sql, binds)
            else:
                c.execute(sql)
        except Exception as e:
            c.close()
            db.close()
            raise
        if rowtype == 'dict':
            c.rowfactory = makeDictFactory(c)
        elif rowtype == 'namedtuple':
            c.rowfactory = makeNamedTupleFactory(c)

        try:
            if fetch:
                r = c.fetchmany(fetch)
            else:
                r = c.fetchall()
        except Exception as e:
            raise
        else:
            rs = copy.deepcopy(r)
            return rs
        finally:
            del r
            c.close()
            db.close()

    def select_iter(self, sql, binds=None, fetch_size=1000, max_rows=None):
        """
        Select records in chunks using a generator.

        Parameters
        ----------
        sql:        The sql to execute
        binds:      A tuple or dict of bind variables to use (default: None).
        fetch_size: The size of the chunks to yield on each iteration.
        max_rows:   The maximum number of rows to fetch. If None, all rows will
                    eventually be returned (default: None).

        Yields
        ------
        A list of dicts matching the result set. Each iteration will yield a
        new chunk of data according to the fetch_size given.
        """
        db = cx_Oracle.connect(self.username, self.password, self.host)
        c = db.cursor()

        try:
            if binds:
                c.execute(sql, binds)
            else:
                c.execute(sql)
        except Exception as e:
            c.close()
            db.close()
            raise

        c.rowfactory = makeDictFactory(c)

        try:
            if max_rows:
                i = 0
                while i <= max_rows:
                    results = c.fetchmany(fetch_size)
                    if not results:
                        break
                    else:
                        rs = copy.deepcopy(results)
                        del results
                        i += len(rs)
                        yield rs
            else:
                while True:
                    results = c.fetchmany(fetch_size)
                    if not results:
                        break
                    else:
                        rs = copy.deepcopy(results)
                        del results
                        yield rs
        except:
            raise
        finally:
            c.close()
            db.close()
