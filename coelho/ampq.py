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

from abc import ABC, abstractmethod
import logging
import pika.connection
from pika.exceptions import AMQPConnectionError

logger = logging.getLogger(__name__)


class AMPQConnected(ABC):

    def __init__(self, name, **kwargs):
        self._name = name
        self._channel = None
        self._connecting = False
        self._connecting_error = False
        self._connection = None

        self._exchange = None
        self._queue = None

        self._host = "localhost"
        self._port = 5672
        self._user = "guest"
        self._password = "guest"
        self._virtual_host = "/"

        if "exchange" in kwargs:
            self._exchange = kwargs['exchange']
        else:
            raise AMQPConnectionError("It is mandatory to define an exchange.")

        if "queue" in kwargs:
            self._queue = kwargs['queue']
        else:
            raise AMQPConnectionError("It is mandatory to define a queue.")

        if "host" in kwargs:
            self._host = kwargs['host']
        if "port" in kwargs:
            self._port = kwargs['port']
        if "user" in kwargs:
            self._user = kwargs['user']
        if "password" in kwargs:
            self._password = kwargs['password']
        if "virtual_host" in kwargs:
            self._virtual_host = kwargs['virtual_host']

    @abstractmethod
    def connect(self):
        while False:
            yield None

    def disconnect(self):
        if self._connection:
            logger.info("Disconnecting %s from AMPQ Broker." % self._name)
            self._connection.close()
        else:
            logger.warning("AMPQ Broker for %s not defined." % self._name)

    @abstractmethod
    def on_connection_opened(self):
        while False:
            yield None

    def on_connection_failed(self, connection, error_message):
        # TODO: Add personalized error handler action to this method
        # Sometimes we don't want to stop tornado
        self._connecting = False
        self._connecting_error = True
        if isinstance(error_message, AMQPConnectionError):
            error_message = error_message.__repr__()
        logger.error("%s was no able to connect to AMPQ Broker, terminating"
                     " the node. Cause: %s" % (self._name, error_message))
        self.on_connection_failed_action(connection, error_message)

    def on_connection_failed_action(self, connection, error_message):
        logger.debug("No connection failed action implemented for %s." %
                     self._name)

    @property
    def channel(self):
        return self._channel

    @property
    def connection(self):
        return self._connection

    @property
    def parameters(self):
        credentials = pika.PlainCredentials(self._user, self._password)
        return pika.ConnectionParameters(
            host=self._host,
            port=self._port,
            virtual_host=self._virtual_host,
            credentials=credentials
        )

    @property
    def state(self):
        if self._connection:
            return self._connection.connection_state
        return pika.connection.Connection.CONNECTION_CLOSED

    @abstractmethod
    def on_connection_closed(self):
        while False:
            yield None
