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

import logging

from gevent_kafka import consumer, producer
from gevent_zookeeper.framework import ZookeeperFramework
import gevent


def consume(framework):
    def callback(messages):
        for message in messages:
            print message
    c = consumer.Consumer(framework, 'example-group')
    c.start()
    c.subscribe('test', 0.200).start(callback)
    while True:
        gevent.sleep(5)


def produce(framework):
    p = producer.Producer(framework, 'test')
    p.start()

    while True:
        p.send(["hello there on the other side"])
        gevent.sleep(2)


logging.basicConfig(level=logging.DEBUG)

framework = ZookeeperFramework('localhost:2181', 10)
framework.connect()

gevent.spawn(consume, framework)
gevent.spawn(produce, framework)

while True:
    gevent.sleep(10)
