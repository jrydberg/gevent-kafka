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

"""Implementation of the wire format for Kafka.

According to https://cwiki.apache.org/confluence/display/KAFKA/Wire+Format
"""

import gevent
from gevent.queue import Queue
from gevent import socket
import struct
import zlib
import gzip
from cStringIO import StringIO


REQUEST_TYPE_PRODUCE = 0
REQUEST_TYPE_FETCH = 1
REQUEST_TYPE_MULTIFETCH = 2
REQUEST_TYPE_MULTIPRODUCE = 3
REQUEST_TYPE_OFFSETS = 4

ERROR_UNKNOWN = -1
ERROR_OFFSET_OUT_OF_RANGE = 1
ERROR_INVALID_MESSAGE = 2
ERROR_WRONG_PARTITION = 3
ERROR_INVALID_FETCH_SIZE = 4


class OffsetOutOfRangeError(Exception):
    """Offset requested is no longer available on the server."""


class InvalidMessageError(Exception):
    """A message you sent failed its checksum and is corrupt."""


class WrongPartitionError(Exception):
    """You tried to access a partition that doesn't exist."""


class InvalidFetchSizeError(Exception):
    """The size you requested for fetching is smaller than the message
    you're trying to fetch."""


_ERROR_CODE_EXCEPTIONS = {
    ERROR_OFFSET_OUT_OF_RANGE: OffsetOutOfRangeError,
    ERROR_INVALID_MESSAGE: InvalidMessageError,
    ERROR_WRONG_PARTITION: WrongPartitionError,
    ERROR_INVALID_FETCH_SIZE: InvalidFetchSizeError
    }


def _possibly_raise_exception(error_code):
    if error_code in _ERROR_CODE_EXCEPTIONS:
        raise _ERROR_CODE_EXCEPTIONS[error_code]()


def _encode_request(request_type, topic, partition, payload):
    return ''.join([
            struct.pack("!HH", request_type, len(topic)),
            topic,
            struct.pack("!I", partition),
            payload])


def _encode_produce_request(topic, partition, messages):
    messages = ''.join(messages)
    payload = struct.pack("!I", len(messages)) + messages
    return _encode_request(REQUEST_TYPE_PRODUCE, topic, partition,
                           payload)

def _encode_message(compression, payload):
    payload = struct.pack("!BBl", 1, compression,
        zlib.crc32(payload)) + payload
    return struct.pack("!I", len(payload)) + payload


def _encode_fetch_request(topic, partition, offset, max_size):
    return _encode_request(REQUEST_TYPE_FETCH, topic, partition,
                           struct.pack("!QI", offset, max_size))


def _encode_offsets_request(topic, partition, time, num_offsets):
    return _encode_request(REQUEST_TYPE_OFFSETS, topic, partition,
        struct.pack("!qI", time, num_offsets))


def _decode_response(data):
    error_code, data = struct.unpack("!h", data[:2])[0], data[2:]
    return error_code, data


def _decode_messages(data):
    length = len(data)
    messages = []

    while len(data) >= 10:
        msgsize, magic, compression, csum = struct.unpack("!IBBI",
            data[:4 + 2 + 4])
        # Excluding the length:
        msgsize = msgsize - 6

        payload, newdata = data[10:10 + msgsize], data[msgsize + 10:]
        if len(payload) != msgsize:
            if not messages:
                print "first message was cropped! max_size set too small?"
            break
        data = newdata

        # FIXME: rewrite this.
        if magic and compression == 1:
            fp = gzip.GzipFile(fileobj=StringIO(payload))
            decompressed = _decode_messages(fp.read())[0]
            for message in decompressed:
                messages.append(message)
        elif magic and compression != 0:
            raise InvalidMessageError("invalid message")
            print "unsupported compression codec", compression
        else:
            messages.append(payload)

    # Return the messages and the number of bytes that we have
    # consumed.  Any cropped messages will be left in "data".
    return messages, length - len(data)


def _decode_fetch_response(data):
    error_code, data = _decode_response(data)
    messages, length = _decode_messages(data)
    return error_code, messages, length


def _decode_offsets_response(data):
    error_code, data = _decode_response(data)
    num_offsets, data = struct.unpack("!I", data[:4])[0], data[4:]
    offsets = []
    for i in range(num_offsets):
        value = data[8 * i: 8 * (i + 1)]
        offsets.append(struct.unpack("!Q", value)[0])
    return error_code, offsets


class Int32Protocol(object):
    """A simple 32-bit prefixes protocol."""

    def __init__(self, socket):
        self.socket = socket
        self.sendq = Queue()
        self.readq = Queue()
        self.buf = ''
        self.start()
        self.closed = False

    def write(self, data):
        self.socket.sendall(''.join([struct.pack("!I", len(data)),
                                     data]))

    def read(self):
        data = self.socket.recv(4)
        if not data:
            raise socket.error('closed')

        size = struct.unpack("!I", data)[0]

        # FIXME: Rewrite, optimize!
        buf = ''
        while len(buf) != size:
            data = self.socket.recv(size - len(buf))
            buf += data

        return buf

    def start(self):
        """Start the protocol."""

    def close(self):
        """Close the protocol and socket."""
        self.socket.close()


class KafkaProtocol(Int32Protocol):
    """Protocol implementation for Kafka."""

    def __init__(self, socket):
        Int32Protocol.__init__(self, socket)

    def produce(self, topic, partition, messages):
        """Produce messages to the topic and partition."""
        messages = [_encode_message(0, message) for message in messages]
        self.write(_encode_produce_request(topic, partition, messages))

    def fetch(self, topic, partition, offset, max_size):
        """Fetch messages from broker."""
        self.write(_encode_fetch_request(topic, partition, offset, max_size))
        error_code, messages, length = _decode_fetch_response(self.read())
        _possibly_raise_exception(error_code)
        return messages, length

    def offsets(self, topic, partition, time, num_offsets):
        """Get message offsets."""
        self.write(_encode_offsets_request(topic, partition, time,
           num_offsets))
        error_code, offsets = _decode_offsets_response(self.read())
        _possibly_raise_exception(error_code)
        return offsets
