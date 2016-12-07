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

from mock import create_autospec
from mock import MagicMock
from sqlalchemy.engine.base import Engine
from synergy_scheduler_manager.common.project import Project

from synergy_scheduler_manager.common.queue import QueueDB
from synergy_scheduler_manager.common.queue import QueueItem
from synergy_scheduler_manager.scheduler_manager import Notifications
from synergy_scheduler_manager.scheduler_manager import Worker
from synergy_scheduler_manager.tests.unit import base


class TestNotifications(base.TestCase):
    # TO COMPLETE
    def test_info_quota(self):

        project1 = Project()
        project1.setId(1)
        project1.setName("test1")

        project2 = Project()
        project2.setId(2)
        project2.setName("test2")

        prjDict = {1: project1, 2: project2}

        ns = Notifications(prjDict, None)
        payload = {
            "state": "deleted",
            "instance_type": "instance_type",
            "user_id": "user_id",
            "root_gb": "root_gb",
            "metadata": "metadata",
            "instance_id": 1,
            "tenant_id": 2,
            "memory_mb": 3,
            "vcpus": 4}

        ns.info(ctxt=None,
                publisher_id=None,
                event_type="compute.instance.delete.end",
                payload=payload,
                metadata=None)

        quota = ns.projects[2].getQuota()
        self.assertEqual(0, quota.getUsage("memory", private=False))
        self.assertEqual(0, quota.getUsage("vcpus", private=False))


class TestWorker(base.TestCase):

    # TO COMPLETE
    def setUp(self):
        super(TestWorker, self).setUp()
        self.nova_manager_mock = MagicMock()
        self.keystone_manager_mock = MagicMock()
        db_engine_mock = create_autospec(Engine)

        project1 = Project()
        project1.setId("1")
        project1.setName("test1")

        project2 = Project()
        project2.setId("2")
        project2.setName("test2")

        projects_list = {'1': project1, '2': project2}
        self.worker = Worker(
            name="test",
            queue=QueueDB("testq", db_engine_mock),
            projects=projects_list,
            nova_manager=self.nova_manager_mock,
            keystone_manager=self.keystone_manager_mock)

    def test_destroy(self):
        """An empty worker can be destroyed without raising an exception."""
        self.worker.destroy()

    def test_name(self):
        self.assertEqual('test', self.worker.getName())

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
        quota_allocate_mock = create_autospec(
            self.worker.projects['1'].getQuota().allocate)
        quota_allocate_mock.return_value = True
        self.worker.projects['1'].getQuota().allocate = quota_allocate_mock

        # Delete item from the queue
        delete_item_mock = create_autospec(self.worker.queue.deleteItem)
        self.worker.queue.deleteItem = delete_item_mock
