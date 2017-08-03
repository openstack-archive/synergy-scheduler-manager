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
from synergy_scheduler_manager.common.queue import Queue
from synergy_scheduler_manager.common.queue import QueueItem
from synergy_scheduler_manager.project_manager import ProjectManager
from synergy_scheduler_manager.scheduler_manager import Worker
from synergy_scheduler_manager.tests.unit import base

"""
class TestNotifications(base.TestCase):

    def test_info_quota(self):
        SharedQuota.setSize("vcpus", 20)
        SharedQuota.setSize("memory", 4096)
        SharedQuota.enable()

        self.assertEqual(20, SharedQuota.getSize('vcpus'))
        self.assertEqual(4096, SharedQuota.getSize('memory'))

        prj_a = Project()
        prj_a.setId(1)
        prj_a.setName("prj_a")

        prj_b = Project()
        prj_b.setId(2)
        prj_b.setName("prj_b")

        prjDict = {1: prj_a, 2: prj_b}

        quota = prjDict[1].getQuota()

        quota.setSize("vcpus", 10, private=True)
        quota.setSize("memory", 2048, private=True)

        self.assertEqual(10, quota.getSize('vcpus', private=True))
        self.assertEqual(2048, quota.getSize('memory', private=True))

        quota.setSize("vcpus",
                      SharedQuota.getSize('vcpus'),
                      private=False)
        quota.setSize("memory",
                      SharedQuota.getSize('memory'),
                      private=False)

        self.assertEqual(20, quota.getSize('vcpus', private=False))
        self.assertEqual(4096, quota.getSize('memory', private=False))

        flavor = Flavor()
        flavor.setVCPUs(2)
        flavor.setMemory(512)

        server = Server()
        server.setType("ephemeral")
        server.setId("server_id")
        server.setFlavor(flavor)

        self.assertEqual(True, server.isEphemeral())
        try:
            allocated = quota.allocate(server, blocking=False)
        except Exception as ex:
            print(ex)

        self.assertEqual(True, allocated)

        self.assertEqual(0, quota.getUsage('vcpus', private=True))
        self.assertEqual(0, quota.getUsage('memory', private=True))

        self.assertEqual(2, quota.getUsage('vcpus', private=False))
        self.assertEqual(512, quota.getUsage('memory', private=False))

        self.assertEqual(2, SharedQuota.getUsage('vcpus'))
        self.assertEqual(512, SharedQuota.getUsage('memory'))

        ns = Notifications(prjDict, None)

        payload = {
            "state": "deleted",
            "deleted_at": "2016-12-09T10:06:10.000000",
            "terminated_at": "2016-12-09T10:06:10.025305",
            "instance_type": "m1.tiny",
            "user_id": "user",
            "root_gb": "1",
            "metadata": {},
            "instance_id": "server_id",
            "tenant_id": 1,
            "memory_mb": 512,
            "vcpus": 2}

        ns.info(ctxt=None,
                publisher_id=None,
                event_type="compute.instance.delete.end",
                payload=payload,
                metadata=None)

        quota = prjDict[1].getQuota()

        self.assertEqual(0, quota.getUsage("vcpus", private=True))
        self.assertEqual(0, quota.getUsage("memory", private=True))

        self.assertEqual(0, quota.getUsage('vcpus', private=False))
        self.assertEqual(0, quota.getUsage('memory', private=False))

        self.assertEqual(0, SharedQuota.getUsage('vcpus'))
        self.assertEqual(0, SharedQuota.getUsage('memory'))
"""


class TestWorker(base.TestCase):

    # TO COMPLETE
    def setUp(self):
        super(TestWorker, self).setUp()
        self.db_engine_mock = create_autospec(Engine)
        self.nova_manager_mock = MagicMock()
        self.keystone_manager_mock = MagicMock()
        db_engine_mock = create_autospec(Engine)

        def my_side_effect(*args, **kwargs):
            project1 = Project()
            project1.setId(1)
            project1.setName("test_project1")
            project1.getShare().setValue(5)

            project2 = Project()
            project2.setId(2)
            project2.setName("test_project2")
            project2.getShare().setValue(55)

            if args[0] == 1:
                return project1
            elif args[0] == 2:
                return project2

        keystone_manager = MagicMock()
        keystone_manager.getProject.side_effect = my_side_effect

        self.project_manager = ProjectManager()
        self.project_manager.db_engine = MagicMock()
        self.project_manager.keystone_manager = keystone_manager
        self.project_manager.default_TTL = 10
        self.project_manager.default_share = 30
        self.project_manager._addProject(1, "test_project1", 10, 50)

        self.worker = Worker(
            name="test",
            queue=Queue("testq", db_engine=db_engine_mock),
            project_manager=self.project_manager,
            nova_manager=self.nova_manager_mock,
            keystone_manager=self.keystone_manager_mock)

    def test_destroy(self):
        """An empty worker can be destroyed without raising an exception."""
        self.worker.destroy()

    def test_name(self):
        self.assertEqual('test', self.worker.getName())

    def test_run_build_server(self):

        def nova_exec_side_effect(command, *args, **kwargs):
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
        get_item_mock = create_autospec(self.worker.queue.dequeue)
        get_item_mock.return_value = qitem_mock
        self.worker.queue.getItem = get_item_mock

        # Mock nova "GET_SERVER" and "BUILD_SERVER" calls
        nova_exec = self.nova_manager_mock.execute
        nova_exec.side_effect = nova_exec_side_effect

        project = self.project_manager.getProject(id=1)
        # Mock quota allocation
        quota_allocate_mock = create_autospec(project.getQuota().allocate)
        quota_allocate_mock.return_value = True
        project.getQuota().allocate = quota_allocate_mock

        # Delete item from the queue
        delete_item_mock = create_autospec(self.worker.queue.delete)
        self.worker.queue.deleteItem = delete_item_mock
