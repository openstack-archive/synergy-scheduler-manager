# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import json

from datetime import datetime
from mock import create_autospec
from sqlalchemy.engine.base import Engine
from synergy_scheduler_manager.common.queue import Queue
from synergy_scheduler_manager.common.queue import QueueItem
from synergy_scheduler_manager.common.user import User
from synergy_scheduler_manager.tests.unit import base

import logging
LOG = logging.getLogger(__name__)


class TestQueueItem(base.TestCase):

    def setUp(self):
        super(TestQueueItem, self).setUp()
        self.qitem = QueueItem()
        self.qitem.setId(1)
        self.qitem.setPriority(1000)
        self.qitem.setUserId(100)
        self.qitem.setProjectId(1)
        self.qitem.setRetryCount(1)
        self.qitem.setData(1)

    def test_get_set_id(self):
        self.assertEqual(1, self.qitem.getId())
        self.qitem.setId(8)
        self.assertEqual(8, self.qitem.getId())

    def test_get_set_userid(self):
        self.assertEqual(100, self.qitem.getUserId())

        self.qitem.setUserId(13)
        self.assertEqual(13, self.qitem.getUserId())

    def test_get_set_projectid(self):
        self.assertEqual(1, self.qitem.getProjectId())

        self.qitem.setProjectId(12)
        self.assertEqual(12, self.qitem.getProjectId())

    def test_get_set_priority(self):
        self.assertEqual(1000, self.qitem.getPriority())

        self.qitem.setPriority(10)
        self.assertEqual(10, self.qitem.getPriority())

    def test_retry_count(self):
        self.assertEqual(1, self.qitem.getRetryCount())

        self.qitem.setRetryCount(10)
        self.assertEqual(10, self.qitem.getRetryCount())

        self.qitem.incRetryCount()
        self.assertEqual(11, self.qitem.getRetryCount())

    def test_get_set_creation_time(self):
        self.assertEqual(self.qitem.getCreationTime(),
                         self.qitem.getLastUpdate())

        now = datetime.now()
        self.qitem.setCreationTime(now)
        self.assertEqual(now, self.qitem.getCreationTime())

    def test_get_set_last_update(self):
        self.assertEqual(self.qitem.getCreationTime(),
                         self.qitem.getLastUpdate())

        now = datetime.now()
        self.qitem.setLastUpdate(now)
        self.assertEqual(now, self.qitem.getLastUpdate())

    def test_get_set_data(self):
        self.assertEqual(1, self.qitem.getData())

        self.qitem.setData(2)
        self.assertEqual(2, self.qitem.getData())


class TestQueue(base.TestCase):

    def setUp(self):
        super(TestQueue, self).setUp()

        # Create a Queue that mocks database interaction
        self.db_engine_mock = create_autospec(Engine)
        self.queue = Queue(name="test", db_engine=self.db_engine_mock)

    def test_get_name(self):
        self.assertEqual('test', self.queue.getName())

    def test_close(self):
        self.queue.close()
        self.assertEqual(True, self.queue.isClosed())

    def test_getSize(self):
        self.assertEqual(0, self.queue.getSize())

    def test_enqueue(self):
        self.assertEqual(0, self.queue.getSize())
        user = User()
        user.setId(2)
        user.setProjectId(100)
        user.getPriority().setValue(10)

        self.queue.enqueue(user=user, data="mydata")

        self.assertEqual(1, self.queue.getSize())

    def test_dequeue(self):
        self.assertEqual(0, self.queue.getSize())
        user = User()
        user.setId(2)
        user.setProjectId(100)
        user.getPriority().setValue(10)
        data = json.dumps("mydata")

        self.queue.enqueue(user=user, data=data)

        self.assertEqual(1, self.queue.getSize())

        # Mock the DB
        execute_mock = self.db_engine_mock.connect().execute
        execute_mock().lastrowid = 123
        fetchone_mock = execute_mock().fetchone
        fetchone_mock.return_value = [data]

        qitem = self.queue.dequeue()

        self.assertIsNotNone(qitem)
        self.assertEqual(2, qitem.getUserId())
        self.assertEqual(100, qitem.getProjectId())
        self.assertEqual("mydata", qitem.getData())

    def test_delete(self):
        self.assertEqual(0, self.queue.getSize())
        user = User()
        user.setId(2)
        user.setProjectId(100)
        user.getPriority().setValue(10)
        data = json.dumps("mydata")

        self.queue.enqueue(user=user, data=data)

        # Mock the DB
        execute_mock = self.db_engine_mock.connect().execute
        execute_mock().lastrowid = 123
        fetchone_mock = execute_mock().fetchone
        fetchone_mock.return_value = [data]
        qitem = self.queue.dequeue()

        execute_mock = self.db_engine_mock.connect().execute
        execute_mock().lastrowid = 123
        self.queue.delete(qitem)

    def test_insertItemDB(self):
        qitem = QueueItem()
        qitem.setPriority(1000)
        qitem.setUserId(100)
        qitem.setProjectId(1)
        qitem.setRetryCount(1)
        qitem.setData(1)

        # Check the db call of the item insert
        execute_mock = self.db_engine_mock.connect().execute
        execute_mock().lastrowid = 123
        self.queue._insertItemDB(qitem)
        self.assertEqual(123, qitem.getId())

    def test_updateItemDB(self):
        qitem = QueueItem()
        qitem.setPriority(1000)
        qitem.setUserId(100)
        qitem.setProjectId(1)
        qitem.setRetryCount(1)
        qitem.setData(1)
        lastUpdate = qitem.getLastUpdate()

        # Mock the DB update
        execute_mock = self.db_engine_mock.connect().execute
        execute_mock().lastrowid = 123

        self.queue._updateItemDB(qitem)
        self.assertNotEqual(lastUpdate, qitem.getLastUpdate())
