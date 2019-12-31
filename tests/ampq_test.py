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

import unittest
from coelho.ampq import AMPQConnected
from pika.exceptions import AMQPConnectionError


class BogusAMPQConnected(AMPQConnected):
    """Just a bogus concrete AMPQ connected to test the abstract class"""

    def connect(self):
        pass

    def on_connection_closed(self):
        pass

    def on_connection_opened(self):
        pass


class AMPQConnectedTest(unittest.TestCase):
    """ Test Case covering AMPQConnected """

    def test_constructor_no_exchange(self):
        """ Test creating without exchange to catch raise an
        AMQPConnectionError """
        try:
            BogusAMPQConnected("Test no exchange", queue="test")
        except AMQPConnectionError as ace:
            self.assertTrue(isinstance(ace, AMQPConnectionError))
            self.assertEqual("It is mandatory to define an exchange.",
                             ace.args[0])

    def test_constructor_no_queue(self):
        """ Test creating without queue will raise an AMQPConnectionError """
        try:
            BogusAMPQConnected("Test no queue", exchange="test")
        except AMQPConnectionError as ace:
            self.assertTrue(isinstance(ace, AMQPConnectionError))
            self.assertEqual("It is mandatory to define a queue.",
                             ace.args[0])

    def test_default_constructor(self):
        """ Test creating with only queue and exchange will set default
        parameters """
        client = BogusAMPQConnected(
            "Test default constructor",
            queue="test",
            exchange="test"
        )
        self.assertEqual("localhost", client.parameters._host)
        self.assertEqual(5672, client.parameters._port)
        self.assertEqual("guest", client.parameters._credentials.username)
        self.assertEqual("guest", client.parameters._credentials.password)
        self.assertEqual("/", client.parameters._virtual_host)

    def test_custom_constructor(self):
        """ Test creating with all parameters and check default values """
        client = BogusAMPQConnected(
            "Test custom constructor",
            queue="test",
            exchange="test",
            host="custom.host.local",
            port=5673,
            user="admin",
            password="pass",
            virtual_host="/vh1"
        )
        self.assertEqual("custom.host.local", client.parameters._host)
        self.assertEqual(5673, client.parameters._port)
        self.assertEqual("admin", client.parameters._credentials.username)
        self.assertEqual("pass", client.parameters._credentials.password)
        self.assertEqual("/vh1", client.parameters._virtual_host)
