# -*- coding: UTF-8 -*-
#
# Copyright 2019-2020 Flavio Garcia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from abc import abstractmethod
import logging
import pika
from pika.adapters import tornado_connection
from .ampq import AMPQConnected
from . import get_version

logger = logging.getLogger(__name__)


# Based on https://gist.github.com/brimcfadden/2855520
class MessagingRpcClient(AMPQConnected):

    def connect(self):
        logger.info("Coelho version %s connecting %s RPC Client to the AMPQ "
                    "broker." % (get_version(), self._name))
        self._connecting = True
        tornado_connection.TornadoConnection(
            self.parameters,
            on_open_callback=self.on_connection_opened,
            on_open_error_callback=self.on_connection_failed,
            on_close_callback=self.on_connection_closed
        )

    def on_connection_opened(self, connection):
        from pika.exceptions import InvalidChannelNumber
        logger.info("%s connected to AMPQ Broker." % self._name)
        self._connection = connection
        try:
            self._connection.channel(
                on_open_callback=self.on_channel_opened
            )
            self._connecting = False
        except InvalidChannelNumber as icn:
            import sys
            logger.critical("Invalid channel number exception while connecting"
                            " %s to AMPQ Broker: %s" % (self._name, icn))
            sys.exit(2)

    def on_channel_opened(self, channel):
        logger.info("%s channel opened with AMPQ Broker." % self._name)
        self._channel = channel

        self._channel.exchange_declare(
            exchange=self._exchange,
            exchange_type="topic",
            callback=None
        )

        self._channel.queue_declare(
            queue=self._queue,
            callback=self.on_input_queue_declared
        )
        logger.info("%s component initialized." % self._name)

    def on_input_queue_declared(self, queue):
        logger.info("%s input queue declared on AMPQ Broker." % self._name)
        self._channel.queue_bind(
            exchange=self._exchange,
            queue=self._queue,
            routing_key="#",
            callback=None
        )

    def on_connection_closed(self, code, message):
        logger.info("%s disconnected from AMPQ Broker." % self._name)
        logger.warning("%s - %s" % (code, message))


# Based on: http://bit.ly/2eS1KYP
class RpcWorker(AMPQConnected):

    def __init__(self, name, **kwargs):
        super(RpcWorker, self).__init__(name, **kwargs)
        self._consumer_tag = None

    def connect(self):
        logger.info("Coelho version %s connecting %s RPC Worker to the AMPQ"
                    " broker." % (get_version(), self._name))
        self._connecting = True
        self._connection = pika.SelectConnection(
            self.parameters, self.on_connection_opened
        )
        self._connection.add_on_open_error_callback(self.on_connection_failed)
        self._connection.add_on_close_callback(self.on_connection_closed)

    def run(self):
        self.connect()
        self._connection.ioloop.start()

    # Step #2
    def on_connection_opened(self, connection):
        # Open a channel
        connection.channel(
            on_open_callback=self.on_channel_open
        )

    # Step #3
    def on_channel_open(self, channel):
        """Called when our channel has opened"""
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)
        self._channel.queue_declare(
            queue=self._queue,
            durable=True,
            exclusive=False,
            auto_delete=False,
            callback=self.on_worker_queue_declared
        )

    # Step #4
    def on_worker_queue_declared(self, frame):
        """Called when AMPQ Broker has told us our Queue has been declared,
        frame is the response from the broker"""
        self._consumer_tag = self._channel.basic_consume(
            self._queue, self.handle_workload)

    # From: https://bit.ly/2kjDIO7
    def stop_consuming(self):
        if self._channel:
            logger.info("Sending a Basic.Cancel RPC command to AMPQ Broker.")
            self._channel.basic_cancel(self._consumer_tag, self.on_cancelok)

    def on_cancelok(self, unused_frame):
        """This method is invoked by pika when AMPQ Broker acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method unused_frame: The Basic.CancelOk frame

        """
        logger.info(
            "AMPQ Broker acknowledged the cancellation of the consumer"
        )

    def on_connection_closed(self, connection, message):
        logger.info("Worker %s disconnected from AMPQ Broker" % self._name)
        logger.info("Connection closed with code %s and message \"%s\"." % (
            message.reply_code, message.reply_text
        ))
        self._connection.ioloop.stop()

    def on_channel_closed(self, channel, reason):
        logger.warning("Closing channel reason: %s" % reason)

    @abstractmethod
    def handle_workload(self, channel, method, properties, body):
        while False:
            yield None
