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


class Producer(object):
    """A message producer for a topic.

    The producer has to be started using C{start}.
    """

    def __init__(self, framework, topic):
        self.framework = framework
        self.topic_name = topic
        self.brokers = {}
        self.topic_parts = {}

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

    def send(self, messages):
        """Send messages to the topic."""
        broker_id = random.choice(self.brokers.keys())
        part_no = random.randint(0, self.topic_parts.get(broker_id, 1) - 1)
        return self.brokers[broker_id].produce(self.topic_name, part_no,
            messages)
