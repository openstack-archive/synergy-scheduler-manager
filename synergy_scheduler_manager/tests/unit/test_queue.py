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
import heapq

from datetime import datetime
from mock import call
from mock import create_autospec
from sqlalchemy.engine.base import Engine
from synergy_scheduler_manager.common.queue import PriorityQueue
from synergy_scheduler_manager.common.queue import QueueDB
from synergy_scheduler_manager.common.queue import QueueItem
from synergy_scheduler_manager.tests.unit import base


class TestQueueItem(base.TestCase):

    def setUp(self):
        super(TestQueueItem, self).setUp()
        self.qitem = QueueItem(id=1,
                               user_id=100,
                               prj_id=1,
                               priority=1000,
                               retry_count=1,
                               creation_time='now',
                               last_update='now',
                               data=1)

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
        self.assertEqual("now", self.qitem.getCreationTime())

        self.qitem.setCreationTime("later")
        self.assertEqual("later", self.qitem.getCreationTime())

    def test_get_set_last_update(self):
        self.assertEqual("now", self.qitem.getLastUpdate())

        self.qitem.setLastUpdate("later")
        self.assertEqual("later", self.qitem.getLastUpdate())

    def test_get_set_data(self):
        self.assertEqual(1, self.qitem.getData())

        self.qitem.setData(2)
        self.assertEqual(2, self.qitem.getData())


class TestPriorityQueue(base.TestCase):
    def setUp(self):
        super(TestPriorityQueue, self).setUp()
        self.pq = PriorityQueue()
        now = datetime.now()

        for i in range(0, 3):
            item = QueueItem(id=i,
                             user_id=100,
                             prj_id=1,
                             priority=1000,
                             retry_count=1,
                             creation_time=now,
                             last_update=now,
                             data=i)
            self.pq.put(i, item)

    def test_get(self):
        self.assertEqual(2, self.pq.get().getId())
        self.assertEqual(1, self.pq.get().getId())
        self.assertEqual(0, self.pq.get().getId())

    def test_size(self):
        self.assertEqual(3, self.pq.size())

    def test_items(self):
        self.assertEqual(3, len(self.pq.items()))

    def test_smallest(self):
        self.assertEqual(0, self.pq.smallest(1)[0].getId())

    def test_largest(self):
        self.assertEqual(2, self.pq.largest(1)[0].getId())


class TestQueueDB(base.TestCase):

    def setUp(self):
        super(TestQueueDB, self).setUp()

        # Create a Queue that mocks database interaction
        self.db_engine_mock = create_autospec(Engine)
        self.q = QueueDB(name="test", db_engine=self.db_engine_mock)

    def test_get_name(self):
        self.assertEqual('test', self.q.getName())

    def test_close(self):
        self.q.close()
        self.assertEqual(True, self.q.isClosed())

    def test_insert_item(self):

        self.q.insertItem(user_id=1, prj_id=2, priority=10, data="mydata")

        # Check the db call of the item insert
        insert_call = call.connect().execute(
            'insert into `test` (user_id, prj_id, priority, data) '
            'values(%s, %s, %s, %s)', [1, 2, 10, '"mydata"'])
        self.assertIn(insert_call, self.db_engine_mock.mock_calls)

        # Check the item existence and values in the in-memory queue
        priority, timestamp, item = heapq.heappop(self.q.pqueue._heap)
        self.assertEqual(-10, priority)
        self.assertEqual(item.getCreationTime(), timestamp)
        self.assertEqual(1, item.user_id)
        self.assertEqual(2, item.prj_id)
        self.assertEqual(10, item.priority)
        self.assertEqual(0, item.retry_count)
        self.assertIsNone(item.data)  # TODO(vincent): should it be "mydata"?

    def test_get_size(self):
        execute_mock = self.db_engine_mock.connect().execute
        execute_call = call('select count(*) from `test`')

        fetchone_mock = execute_mock().fetchone
        fetchone_mock.return_value = [3]

        # Check that getSize() uses the correct sqlalchemy method
        self.assertEqual(3, self.q.getSize())

        # Check that getSize() uses the correct SQL statement
        self.assertEqual(execute_call, execute_mock.call_args)

    def test_get_item(self):
        # Insert the item and mock its DB insertion
        execute_mock = self.db_engine_mock.connect().execute
        execute_mock().lastrowid = 123
        self.q.insertItem(user_id=1, prj_id=2, priority=10, data="mydata")

        # Mock the DB select by returning the same things we inserted before
        select_mock = self.db_engine_mock.connect().execute
        select_call = call("select user_id, prj_id, priority, retry_count, "
                           "creation_time, last_update, data from `test` "
                           "where id=%s", [123])
        fetchone_mock = select_mock().fetchone
        fetchone_mock.return_value = [1, 2, 10, 0, "now", "now", '"mydata"']

        item = self.q.getItem()
        self.assertEqual(select_call, select_mock.call_args)
        self.assertEqual(123, item.id)
        self.assertEqual(1, item.user_id)
        self.assertEqual(2, item.prj_id)
        self.assertEqual(10, item.priority)
        self.assertEqual(0, item.retry_count)
        self.assertEqual("now", item.creation_time)
        self.assertEqual("now", item.last_update)
        self.assertEqual("mydata", item.data)

    def test_delete_item(self):
        # Mock QueueItem to be deleted
        qitem = create_autospec(QueueItem)
        qitem.getId.return_value = 123

        # Mock the DB delete
        execute_mock = self.db_engine_mock.connect().execute
        execute_call = call("delete from `test` where id=%s", [123])

        self.q.deleteItem(qitem)
        self.assertEqual(execute_call, execute_mock.call_args)

    def test_update_item(self):
        # Mock QueueItem to be updated
        qitem = create_autospec(QueueItem)
        qitem.getPriority.return_value = 10
        qitem.getRetryCount.return_value = 20
        qitem.getLastUpdate.return_value = "right_now"
        qitem.getId.return_value = 123

        # Mock the DB update
        execute_mock = self.db_engine_mock.connect().execute
        execute_call = call("update `test` set priority=%s, retry_count=%s, "
                            "last_update=%s where id=%s",
                            [10, 20, "right_now", 123])

        # Check the DB call and that the new QueueItem is in the queue
        self.q.updateItem(qitem)
        self.assertEqual(execute_call, execute_mock.call_args)
        self.assertIn((-10, qitem.getCreationTime(), qitem),
                      self.q.pqueue._heap)
