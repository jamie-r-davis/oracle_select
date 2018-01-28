import copy
import cx_Oracle


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


class DB(object):
    """Database object"""
    
    def __init__(self, host, username, password):
        self.host = host
        self.username = username
        self.password = password
        
    def select(self, sql, binds=None, fetch=0):
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
