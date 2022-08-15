import cx_Oracle

from typing import List, Optional
from pydantic import BaseModel
import threading


class db_config(BaseModel):
    USER_NAME: str
    PASSWORD: str
    HOST_NAME: str
    PORT_NUMBER: str
    SERVICE_NAME: str


class Pooling(BaseModel):
    min: int
    max: int
    increment: int
    threaded: bool
    getmode: cx_Oracle.SPOOL_ATTRVAL_WAIT


class PyOracle(object):
    def __init__(self, db_config: db_config, pooling: Pooling = None):
        self.db_config = db_config
        self.pooling = pooling
        self.dns_tns = cx_Oracle.makedsn(
            self.db_config.HOST_NAME,
            self.db_config.PORT_NUMBER,
            self.db_config.SERVICE_NAME,
        )
        print(cx_Oracle.version)

    def connect(self):
        self.conn = MyConnection(db_config=self.db_config, dns=self.dns_tns)
        print("Database version:", self.conn.version)
        print("Client version:", cx_Oracle.clientversion())

    def connect_pooling(self, use_drcp=True):
        if self.pooling is None:
            raise Exception("pooling을 설정해주세요")
        self.use_drcp = use_drcp
        if use_drcp:
            dns_tns = self.dns_tns + ":pooled"
        else:
            dns_tns = self.dns_tns
        self.pool = cx_Oracle.SessionPool(
            self.db_config.USER_NAME,
            self.db_config.PASSWORD,
            dns_tns,
            min=self.pooling.min,
            max=self.pooling.max,
            increment=self.pooling.increment,
            threaded=self.pooling.threaded,
            getmode=self.pooling.getmode,
        )

    def query_pool(self):
        if self.use_drcp:
            con = self.pool.acquire(cclass="PYTHONHOL", purity=cx_Oracle.ATTR_PURITY_SELF)
        else:
            con = self.pool.acquire()
        cur = self.conn.cursor()
        for i in range(4):
            cur.execute(self.pool_sql)
            (seqval,) = cur.fetchone()
            print("Thread", threading.current_thread().name, "fetched sequence =", seqval)

    def query_thread(self, sql: str, number_of_threads: int):
        threadArray = []
        self.pool_sql = sql
        for i in range(number_of_threads):
            thread = threading.Thread(name="#" + str(i), target=self.query_pool)
            threadArray.append(thread)
            thread.start()
        else:
            for t in threadArray:
                t.join()

    def query(self, sql: str, **kwargs):
        cur = self.conn.cursor()
        if "prefetchrows" in kwargs:
            cur.prefetchrows = kwargs["prefetchrows"]
        if "arraysize " in kwargs:
            cur.arraysize = kwargs["arraysize"]

        cur.execute(sql)
        return cur

    def fetch_all(self, cur):
        res = cur.fetchall()
        return res

    def fetch_one(self, cur):
        res = cur.fetchone()
        return res

    def fetch_many(self, cur, numRows=1):
        res = cur.fetchmany(numRows=numRows)
        return res

    def scroll_cursor(self, cur, value, mode):
        cur.scroll(value=value, mode=mode)
        return cur

    def close(self):
        self.conn.close()


class CustomConnection(cx_Oracle.Connection):
    def __init__(self, db_config: db_config, dns: str):
        print("Connecting to database")
        return super(CustomConnection, self).__init__(db_config.USER_NAME, db_config.PASSWORD, dns)

    def cursor(self):
        return CustomCursor(self)


class CustomCursor(cx_Oracle.Cursor):
    def execute(self, statement, args):
        print("Executing:", statement)
        print("Arguments:")
        for argIndex, arg in enumerate(args):
            print("  Bind", argIndex + 1, "has value", repr(arg))
            return super(CustomCursor, self).execute(statement, args)

    def fetchone(self):
        print("Fetchone()")
        return super(CustomCursor, self).fetchone()

    def fetchall(self):
        print("Fetchall()")
        return super(CustomCursor, self).fetchall()

    def fetchmany(self, numRows=1):
        print("Fetchmany()")
        return super(CustomCursor, self).fetchmany(numRows=numRows)

    def close(
        self,
    ):
        return super(CustomCursor, self).close()
