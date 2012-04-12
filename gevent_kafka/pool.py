from gevent.queue import Queue
from gevent import socket


class ConnectionPool(object):

    def __init__(self, host, port, maxsize=10, factory=lambda x: x):
        if not isinstance(maxsize, (int, long)):
            raise TypeError('Expected integer, got %r' % (maxsize, ))
        self.maxsize = maxsize
        self.pool = Queue()
        self.size = 0
        self.host = host
        self.port = port
        self.factory = factory

    def get(self):
        pool = self.pool
        if self.size >= self.maxsize or pool.qsize():
            return pool.get()
        else:
            self.size += 1
            try:
                new_item = self.create_connection()
            except:
                self.size -= 1
                raise
            return new_item

    def put(self, item):
        self.pool.put(item)

    def lose(self, item):
        self.size -= 1

    def closeall(self):
        while not self.pool.empty():
            conn = self.pool.get_nowait()
            try:
                conn.close()
            except Exception:
                pass

    def create_connection(self):
        """Create connection to remote host."""
        return self.factory(socket.create_connection((self.host, self.port)))
