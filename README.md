gevent-kafka is a client library for [Apache Kafka](http://incubator.apache.org/kafka/),
for the [gevent](http://gevent.org) framework.

You also need [gevent-zookeeper](https://github.com/jrydberg/gevent-zookeeper) installed.


# Producers #

The example below sends messages to the `test` topic:

    from gevent_kafka import producer

    p = producer.Producer(framework, 'test')
    p.start()

    while True:
        p.send(["hello there on the other side"])
        gevent.sleep(2)


# Consumers #

A consumer group is created using `consumer.Consumer`.  Using that,
you can subscribe to different topics:

    from gevent_kafka import consumer

    def callback(messages):
        for message in messages:
            print message
    c = consumer.Consumer(framework, 'example-group')
    c.start()
    c.subscribe('test', 0.200).start(callback)
    while True:
        gevent.sleep(5)


# Things remaining to do #

There are a lot of things to do:

* Unit tests
* Battle tests
* Multi-fetch operations

# License #

Apache License 2.0

# Author #

Written by Johan Rydberg <johan.rydberg@gmail.com>
