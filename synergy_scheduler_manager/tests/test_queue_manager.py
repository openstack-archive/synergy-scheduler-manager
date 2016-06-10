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

from mock import call
from mock import create_autospec
from sqlalchemy.engine.base import Engine

from synergy_scheduler_manager.queue_manager import PriorityQueue
from synergy_scheduler_manager.queue_manager import Queue
from synergy_scheduler_manager.queue_manager import QueueItem
from synergy_scheduler_manager.tests import base


class TestQueueItem(base.TestCase):

    def test_get_set_id(self):
        qitem = QueueItem(id=1,
                          user_id=None,
                          prj_id=None,
                          priority=None,
                          retry_count=None,
                          creation_time=None,
                          last_update=None,
                          data=None)

        self.assertEqual(1, qitem.getId())

        qitem.setId(10)
        self.assertEqual(10, qitem.getId())

    def test_get_set_userid(self):
        qitem = QueueItem(id=None,
                          user_id=1,
                          prj_id=None,
                          priority=None,
                          retry_count=None,
                          creation_time=None,
                          last_update=None,
                          data=None)

        self.assertEqual(1, qitem.getUserId())

        qitem.setUserId(10)
        self.assertEqual(10, qitem.getUserId())

    def test_get_set_projectid(self):
        qitem = QueueItem(id=None,
                          user_id=None,
                          prj_id=1,
                          priority=None,
                          retry_count=None,
                          creation_time=None,
                          last_update=None,
                          data=None)

        self.assertEqual(1, qitem.getProjectId())

        qitem.setProjectId(10)
        self.assertEqual(10, qitem.getProjectId())

    def test_get_set_priority(self):
        qitem = QueueItem(id=None,
                          user_id=None,
                          prj_id=None,
                          priority=1,
                          retry_count=None,
                          creation_time=None,
                          last_update=None,
                          data=None)

        self.assertEqual(1, qitem.getPriority())

        qitem.setPriority(10)
        self.assertEqual(10, qitem.getPriority())

    def test_retry_count(self):
        qitem = QueueItem(id=None,
                          user_id=None,
                          prj_id=None,
                          priority=None,
                          retry_count=1,
                          creation_time=None,
                          last_update=None,
                          data=None)

        self.assertEqual(1, qitem.getRetryCount())

        qitem.setRetryCount(10)
        self.assertEqual(10, qitem.getRetryCount())

        qitem.incRetryCount()
        self.assertEqual(11, qitem.getRetryCount())

    def test_get_set_creation_time(self):
        qitem = QueueItem(id=None,
                          user_id=None,
                          prj_id=None,
                          priority=None,
                          retry_count=None,
                          creation_time="now",
                          last_update=None,
                          data=None)

        self.assertEqual("now", qitem.getCreationTime())

        qitem.setCreationTime("later")
        self.assertEqual("later", qitem.getCreationTime())

    def test_get_set_last_update(self):
        qitem = QueueItem(id=None,
                          user_id=None,
                          prj_id=None,
                          priority=None,
                          retry_count=None,
                          creation_time=None,
                          last_update="now",
                          data=None)

        self.assertEqual("now", qitem.getLastUpdate())

        qitem.setLastUpdate("later")
        self.assertEqual("later", qitem.getLastUpdate())

    def test_get_set_data(self):
        qitem = QueueItem(id=None,
                          user_id=None,
                          prj_id=None,
                          priority=None,
                          retry_count=None,
                          creation_time=None,
                          last_update=None,
                          data=1)

        self.assertEqual(1, qitem.getData())

        qitem.setData(2)
        self.assertEqual(2, qitem.getData())


class TestPriorityQueue(base.TestCase):

    def test_put(self):
        pq = PriorityQueue()
        pq.put(0, "a")
        pq.put(5, "b")
        pq.put(10, "c")

        self.assertIn((0, 0, "a"), pq.queue)
        self.assertIn((-5, 1, "b"), pq.queue)
        self.assertIn((-10, 2, "c"), pq.queue)

        self.assertEqual(3, pq._index)

        self.assertEqual((-10, 2, "c"), heapq.heappop(pq.queue))
        self.assertEqual((-5, 1, "b"), heapq.heappop(pq.queue))
        self.assertEqual((0, 0, "a"), heapq.heappop(pq.queue))

    def test_get(self):
        pq = PriorityQueue()
        pq.put(0, "a")
        pq.put(5, "b")

        self.assertEqual("b", pq.get())
        self.assertEqual("a", pq.get())

    def test_size(self):
        pq = PriorityQueue()
        pq.put(0, "a")
        pq.put(5, "b")
        pq.put(10, "c")

        self.assertEqual(3, pq.size())


class TestQueue(base.TestCase):

    def setUp(self):
        super(TestQueue, self).setUp()

        # Create a Queue that mocks database interaction
        self.db_engine_mock = create_autospec(Engine)
        self.q = Queue(name="test", db_engine=self.db_engine_mock)

    def test_insert_item(self):
        self.q.insertItem(user_id=1, prj_id=2, priority=10, data="mydata")

        # Check the db call of the item insert
        insert_call = call.connect().execute(
            'insert into `test` (user_id, prj_id, priority, data) '
            'values(%s, %s, %s, %s)', [1, 2, 10, '"mydata"'])
        self.assertIn(insert_call, self.db_engine_mock.mock_calls)

        # Check the item existence and values in the in-memory queue
        priority, index, item = heapq.heappop(self.q.pqueue.queue)
        self.assertEqual(-10, priority)
        self.assertEqual(0, index)
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

    def test_reinsert_item(self):
        # TODO(vincent): what is the purpose of this method?
        # It will lead to duplicates.
        pass

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
        pass

    def test_update_item(self):
        pass

    def test_update_priority(self):
        pass
