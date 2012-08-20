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

import random

from gevent_kafka import broker


class NoBrokersAvailableError(Exception):
    """No brokers available."""


class NoPartitionerError(Exception):
    """No partitioner has been specified for the producer."""


NO_KEY = object()


class Producer(object):
    """A message producer for a topic.

    The producer has to be started using C{start}.

    @ivar partitioner: A callable with signature C{(key, num_parts)}
        that should return a number between 0 and C{num_parts - 1}
        that states to which partition the messages with C{key} should
        be sent.
    """

    def __init__(self, framework, topic, partitioner=None):
        self.framework = framework
        self.topic_name = topic
        self.brokers = {}
        self.topic_parts = {}
        self.partitioner = partitioner

    def start(self):
        """Start the producer."""
        self.broker_mon = self.framework.monitor().children().store_into(
            self.brokers, broker.broker_factory).for_path(
                    '/brokers/ids')
        self.topic_mon = self.framework.monitor().children().store_into(
            self.topic_parts, int).for_path('/brokers/topics/%s' % (
                self.topic_name))

    def close(self):
        """Stop the producer."""
        self.broker_mon.close()
        self.topic_mon.close()

    def send(self, messages, key=NO_KEY):
        """Send messages to the topic.

        @param messages: A sequence of messages that should be
            published to the topic.
        @param key: If specified, the partitioner will be used to
            decide which partition should receive the messages, based
            on the key.

        @raise NoBrokersAvailableError: If there are no brokers
            available.
        @raise NoPartitionerError: If a C{key} was specified, but the
            producer was created without a partitioner.
        """
        if not self.brokers:
            raise NoBrokersAvailableError()

        broker_id, part_no = self._select_partition(key)
        return self.brokers[broker_id].produce(self.topic_name, part_no,
            messages)

    def _select_partition(self, key):
        """Given a C{key} return a C{(broker-id, partition-num)} tuple
        that specifies where messages should be sent.
        """
        partitions = [(broker_id, part_id)
            for broker_id in sorted(self.brokers.keys())
            for part_id in range(self.topic_parts.get(
                    broker_id, 1))]

        if key is not NO_KEY:
            if not self.partitioner:
                raise NoPartitionerError()
            key_part_id = self.partitioner(key, len(partitions))
            if key_part_id < 0 or key_part_id >= len(partitions):
                raise ValueError(
                    "returned partition index is without bounds")

            broker_id, part_no = partitions[key_part_id]
        else:
            broker_id, part_no = random.choice(partitions)

        return broker_id, part_no
