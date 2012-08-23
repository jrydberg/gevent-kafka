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

from collections import defaultdict
import logging
import json
import random
import time
import zookeeper

from gevent.queue import Queue
from gevent import socket
import gevent

from gevent_zookeeper.monitor import MonitorListener

from gevent_kafka.protocol import OffsetOutOfRangeError, InvalidMessageError
from gevent_kafka.broker import LATEST, EARLIEST
from gevent_kafka import broker


def sleep_interval(t0, t1, interval):
    dt = interval - (t1 - t0)
    if dt > 0:
        gevent.sleep(dt)


class Rebalancer(MonitorListener):
    """Zookeeper framework listener that rebalances a consumer when
    something changes.
    """

    def __init__(self, consumer):
        self.consumer = consumer

    def created(self, child, data):
        self.consumer.rebalance()

    def modified(self, child, data):
        self.consumer.rebalance()

    def deleted(self, child):
        self.consumer.rebalance()


class ConsumedTopic(object):
    """A consumed topic."""

    def __init__(self, framework, consumer, topic, polling_interval=2,
                 max_size=1048576, retries=3, time=time.time, drain=False):
        self.framework = framework
        self.consumer = consumer
        self.topic_name = topic
        self.partitions = {}
        self.owned = []
        self.reader = None
        self.offsets = {}
        self.readers = {}
        self.max_size = max_size
        self.polling_interval = polling_interval
        self.time = time
        self.rebalanceq = Queue()
        self.log = logging.getLogger('kafka.consumer.%s:%s' % (
                consumer.group_id, topic))
        self.retries = retries
        self.drain = drain

    def rebalance(self):
        """Request that the topic should be rebalanced."""
        self.rebalanceq.put(None)

    def _rebalance(self):
        # Queue handler that goes through the rebalance request queue
        # and calls do_rebalance.
        while True:
            item = self.rebalanceq.get()
            if item is Consumer._STOP_REQUEST:
                break
            for i in range(self.retries):
                if self.do_rebalance():
                    break
                self.log.info('failed to rebalance: will try again soon')
                gevent.sleep(2)
            else:
                self.log.error('Failed to rebalance')

    def do_rebalance(self):
        """Rebalance the group."""
        pt = ['%s-%s' % (bid, n) 
              for (bid, np) in self.partitions.items()
              for n in range(np)]
        cg = [cid for cid in self.consumer.clients.keys()]
        self.log.info('rebalance: pt=%r cg=%r' % (pt, cg))
        pt.sort()
        cg.sort()
        n = len(pt) / len(cg)
        e = len(pt) % len(cg)
        i = cg.index(self.consumer.consumer_id)
        start = n * i + min(i, e)
        stop = start + n + (0 if (i + 1) > e else 1)
        partitions = pt[start:stop]

        self.log.info('rebalance: won %r' % (partitions,))

        for to_remove in (set(self.owned) - set(partitions)):
            self.log.info("stop consuming %s" % (to_remove,))
            # Step 1. Stop consuming the topic.
            greenlet = self.readers.pop(to_remove, None)
            if greenlet is not None:
                gevent.kill(greenlet)

            # Step 2. Remove the owner node from the group.
            self.framework.delete().for_path(
                '/consumers/%s/owners/%s/%s' % (self.consumer.group_id,
                    self.topic_name, to_remove))
            self.owned.remove(to_remove)

            # Step 3. We remove the offsets entry so that we re-read
            # it if we ever get ownership of the partition again.
            self.offsets.pop(to_remove, None)

        # Iterate through the partitions that we just won and try to
        # create the "owner" node in zookeeper.  If we fail to create
        # at least one of them, make sure that False is returned so
        # that the process is restarted.
        fail = False
        for partition in (set(partitions) - set(self.owned)):
            try:
                self.framework.create().as_ephemeral().with_data(
                    self.consumer.consumer_id).parents_if_needed().for_path(
                        '/consumers/%s/owners/%s/%s' % (self.consumer.group_id,
                            self.topic_name, partition))
            except zookeeper.NodeExistsException:
                self.log.info('%s: failed to create ownership' % (partition,))
                fail = True
                continue

            self.owned.append(partition)

            if partition not in self.readers:
                broker_id, part_id = partition.split('-')
                self.readers[partition] = gevent.spawn(
                    self._reader, partition, self.consumer.brokers[broker_id],
                    int(part_id))

        return fail != True

    def update_offset(self, part, offset):
        """Write consumed offset for the given partition."""
        self.framework.set().parents_if_needed().create_if_needed().with_data(
            str(offset)).for_path('/consumers/%s/offsets/%s/%s' % (
                self.consumer.group_id, self.topic_name, part))

    def _reader(self, bpid, broker, partno):
        """Background greenlet for reading content from partitions."""
        # Try to figure out the last read position if it is not known
        # to us.  First we check if a previous consumer has written it
        # to the offsets node.  If not, we use the "offsets" call to
        # the broker to get the _latest_ message.
        if bpid not in self.offsets:
            try:
                data = self.framework.get().for_path(
                    '/consumers/%s/offsets/%s/%s' % (self.consumer.group_id,
                    self.topic_name, bpid))
                data = int(data or 0)
            except zookeeper.NoNodeException:
                offsets = broker.offsets(self.topic_name, partno, LATEST)
                data = offsets[-1]

            self.offsets[bpid] = data

        self.log.info('start consuming %s at %d' % (bpid, self.offsets[bpid]))

        # Keep looping and reading messages from the broker.  After
        # each interval we update the offsets record, if we consumed a
        # message.
        while True:
            t0 = self.time()
            try:
                messages, delta = broker.fetch(self.topic_name, partno,
                    self.offsets[bpid], self.max_size)
            except InvalidMessageError:
                offsets = broker.offsets(self.topic_name, partno, LATEST)
                self.offsets[bpid] = offsets[-1]
                continue
            except OffsetOutOfRangeError:
                offsets = broker.offsets(self.topic_name, partno, EARLIEST)
                self.offsets[bpid] = offsets[-1]
                continue
            except (socket.error, socket.timeout, socket.herror), e:
                self.log.exception("got exception while fetching messages")
            else:
                if messages:
                    self.callback(messages)
                    self.offsets[bpid] += delta
                    self.update_offset(bpid, self.offsets[bpid])
                else:
                    if self.drain:
                        self.drain = False

            sleep_interval(t0, self.time(),
                0 if self.drain else self.polling_interval)

    def start(self, callback):
        """Start consuming the topic."""
        self.callback = callback
        self.consumer._add_topic(self.topic_name, self)
        self.monitor = self.framework.monitor().children().using(
            Rebalancer(self)).store_into(self.partitions, int).for_path(
                '/brokers/topics/%s' % (self.topic_name,))
        self.rebalance_greenlet = gevent.spawn(self._rebalance)

    def close(self):
        """Stop consuming the topic."""
        self.monitor.close()
        self.consumer._remove_topic(self.topic_name, self)
        for owned in self.owned:
            self.framework.delete().for_path(
                '/consumers/%s/owners/%s/%s' % (self.consumer.group_id,
                    self.topic_name, owned))


class Consumer(object):
    """A consumer group.

    Each consumer group can subscribe to multiple topics.  Do this
    using the C{subscribe} method.  This returns a L{ConsumedTopic}
    that needs to be started like this:

        >>> topic = consumer.subscribe('test')
        >>> topic.start(my_callback)

    When you no longer wanna consume a topic, call C{clone} on the
    L{ConsumedTopic}.
    """

    _STOP_REQUEST = u'stop-request'

    def __init__(self, framework, group_id, consumer_id=None):
        self.framework = framework
        self.group_id = group_id
        if consumer_id is None:
            consumer_id = str(random.randint(0, 1000000))
        self.consumer_id = consumer_id
        self.topics = []
        self.clients = {}
        self.partitions = defaultdict(dict)
        self.subscribed = {}
        self.znode = None
        self.brokers = {}
        self.rebalanceq = Queue()

    def _rebalance(self):
        # The global rebalancer.
        #
        # This will rebalance all topics when consumers or brokers
        # enter or leave the group.
        while True:
            item = self.rebalanceq.get()
            if item is Consumer._STOP_REQUEST:
                break
            for topic in self.topics:
                topic.rebalance()

    def rebalance(self):
        # FIXME: do this after a short while?
        self.rebalanceq.put(None)

    def update_topics(self):
        self.framework.set().with_data(json.dumps(
                self.subscribed)).for_path(self.znode)

    def _add_topic(self, topic_name, topic):
        # Add topic and update stuff.
        self.topics.append(topic)
        self.subscribed[topic_name] = 1
        self.update_topics()

    def _remove_topic(self, topic_name, topic):
        self.topics.remove(topic)
        del self.subscribed[topic_name]
        self.update_topics()

    def close(self):
        pass

    def start(self):
        """Start consumer."""
        # Step 1. Create our consumer ID.
        self.znode = self.framework.create().parents_if_needed().with_data(
            json.dumps(self.subscribed)).as_ephemeral().for_path(
            '/consumers/%s/ids/%s' % (self.group_id, self.consumer_id))

        # Step 2: Start monitoring for consumers of this group.
        self.framework.monitor().children().using(Rebalancer(
                self)).store_into(self.clients, json.loads).for_path(
            '/consumers/%s/ids' % (self.group_id,))

        # Step 3: Start monitoring for brokers.
        self.framework.monitor().children().using(Rebalancer(
                self)).store_into(self.brokers, broker.broker_factory).for_path(
            '/brokers/ids')

        # Step 4: Start the global rebalance greenlet.
        self.rebalance_greenlet = gevent.spawn(self._rebalance)

    def subscribe(self, topic_name, polling_interval=2, max_size=1048576):
        """Subscribe to topic.

        Return a L{ConsumedTopic} that has to be started with C{start}.

        @param polling_interval: How often we should check with the brokers
           for new messages.
        @type polling_interval: C{float} (seconds)

        @param max_size: The maximum number of bytes to fetch.
        @type max_size: C{int}

        @return: a L{ConsumedTopic}.
        """
        return ConsumedTopic(self.framework, self, topic_name,
            polling_interval, max_size)
