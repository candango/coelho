# -*- coding: UTF-8 -*-
#
# Copyright 2019 Flavio Garcia
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



