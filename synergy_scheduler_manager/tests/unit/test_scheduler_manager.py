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

from mock import call
from mock import create_autospec
from mock import MagicMock
from sqlalchemy.engine.base import Engine

from synergy_scheduler_manager.queue_manager import Queue
from synergy_scheduler_manager.queue_manager import QueueItem
from synergy_scheduler_manager.quota_manager import DynamicQuota
from synergy_scheduler_manager.scheduler_manager import Notifications
from synergy_scheduler_manager.scheduler_manager import Worker
from synergy_scheduler_manager.tests.unit import base


class TestNotifications(base.TestCase):

    def test_info_dynamic_quota(self):
        """Test that info() makes the correct call to DynamicQuota"""
        dynquota_mock = create_autospec(DynamicQuota)
        ns = Notifications(dynquota_mock)

        payload = {
            "state": "deleted",
            "instance_id": 1,
            "tenant_id": 2,
            "memory_mb": 3,
            "vcpus": 4}
        ns.info(ctxt=None,
                publisher_id=None,
                event_type="compute.instance.delete.end",
                payload=payload,
                metadata=None)

        self.assertEqual(call(1, 2, 4, 3), dynquota_mock.release.call_args)


class TestWorker(base.TestCase):

    def setUp(self):
        super(TestWorker, self).setUp()
        self.nova_manager_mock = MagicMock()
        db_engine_mock = create_autospec(Engine)
        self.worker = Worker(
            name="test",
            queue=Queue("testq", db_engine_mock),
            quota=DynamicQuota(),
            nova_manager=self.nova_manager_mock)

    def test_destroy(self):
        """An empty worker can be destroyed without raising an exception."""
        self.worker.destroy()

    def test_run_build_server(self):

        def nova_exec_side_effect(command, *args, **kwargs):
            """Mock nova.execute to do a successful build."""
            if command == "GET_SERVER":
                res = {"OS-EXT-STS:vm_state": "building",
                       "OS-EXT-STS:task_state": "scheduling"}
            elif command == "BUILD_SERVER":
                res = None
            else:
                raise TypeError("Wrong arguments to nova exec mock")
            return res

        # Mock queue.isClosed to do a 1-pass run of the worker
        is_closed_mock = create_autospec(self.worker.queue.isClosed)
        self.worker.queue.isClosed = is_closed_mock
        self.worker.queue.isClosed.side_effect = (False, True)

        # Mock QueueItem in the queue
        qitem_mock = create_autospec(QueueItem)
        get_item_mock = create_autospec(self.worker.queue.getItem)
        get_item_mock.return_value = qitem_mock
        self.worker.queue.getItem = get_item_mock

        # Mock nova "GET_SERVER" and "BUILD_SERVER" calls
        nova_exec = self.nova_manager_mock.execute
        nova_exec.side_effect = nova_exec_side_effect

        # Mock quota allocation
        quota_allocate_mock = create_autospec(self.worker.quota.allocate)
        quota_allocate_mock.return_value = True
        self.worker.quota.allocate = quota_allocate_mock

        # Delete item from the queue
        delete_item_mock = create_autospec(self.worker.queue.deleteItem)
        self.worker.queue.deleteItem = delete_item_mock

        # Check that we ask nova to BUILD_SERVER and the qitem is deleted
        self.worker.run()
        build_server_call = nova_exec.call_args_list[1]  # second call
        self.assertEqual(("BUILD_SERVER",), build_server_call[0])  # check args
        self.assertEqual(call(qitem_mock), delete_item_mock.call_args)
