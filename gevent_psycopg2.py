import psycopg2.extensions
import psycopg2.pool
import sys

import gevent
import gevent.lock
from gevent.socket import wait_read, wait_write

def gevent_wait_callback(conn, timeout=None):
    """A wait callback for psycopg2 realized as gevent """

    while True:
        state = conn.poll()
        if state == psycopg2.extensions.POLL_OK:
            break
        elif state == psycopg2.extensions.POLL_READ:
            #print ('waiting for greenlet read')
            wait_read(conn.fileno(), timeout=timeout)
        elif state == psycopg2.extensions.POLL_WRITE:
            #print ('waiting for greenlet write')
            wait_write(conn.fileno(), timeout=timeout)
        else:
            print( "psycopg2_pool error %r"%state)

class gevent_psycopg2_pool(psycopg2.pool.AbstractConnectionPool):
    def __init__(self, minconn, maxconn, *args, **kwargs):
        self.semaphore = gevent.lock.Semaphore(maxconn)
        psycopg2.pool.AbstractConnectionPool.__init__(self, minconn, maxconn, *args, **kwargs)

    def getconn(self, *args, **kwargs):
        self.semaphore.acquire()
        return self._getconn(*args, **kwargs)

    def putconn(self, *args, **kwargs):
        self._putconn(*args, **kwargs)
        self.semaphore.release()

    close_all = psycopg2.pool.AbstractConnectionPool._closeall


def main(db_name):
    dsn =" host=127.0.0.1 dbname=postgres user=pg password=postgres "
    psycopg2.extensions.set_wait_callback(gevent_wait_callback)
    pool = gevent_psycopg2_pool(5, 40, dsn)

    def use_connection(count):
        conn = pool.getconn()
        cur = conn.cursor()
        gevent.sleep(5)
        cur.execute("SELECT 2 + 2")
        print '{}: {}'.format(count, cur.fetchone()[0])
        pool.putconn(conn)

    gevent.joinall([gevent.spawn(use_connection, i) for i in xrange(800)])

if __name__ == "__main__":
    dsn ="host=127.0.0.1 dbname=postgres user=pg password=postgres "
    main(dsn)

