# Copyright 2012 Johan Rydberg.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from gevent_kafka.protocol import KafkaProtocol
from gevent_kafka import pool
from gevent import socket


LATEST = -1
EARLIEST = -2


class Broker(object):
    """Representation of a broker."""

    def __init__(self, bid, host, port):
        self.bid = bid
        self.pool = pool.ConnectionPool(host, port,
            factory=KafkaProtocol, connect_timeout=5, read_timeout=5)

    def _interaction(self, method, *args, **kw):
        conn = self.pool.get()
        try:
            value = getattr(conn, method)(*args, **kw)
        except Exception:
            self.pool.lose(conn)
            raise
        else:
            self.pool.put(conn)
            return value

    def offsets(self, topic, partition, time=-1, num_offsets=1):
        return self._interaction('offsets', topic, partition, time,
            num_offsets)

    def fetch(self, topic, partition, offset, max_size):
        return self._interaction('fetch', topic, partition, offset,
            max_size)

    def produce(self, topic, partition, messages):
        return self._interaction('produce', topic, partition,
            messages)


def broker_factory(hostport):
    _id, host, port = hostport.split(':')
    return Broker(_id, host, int(port))
