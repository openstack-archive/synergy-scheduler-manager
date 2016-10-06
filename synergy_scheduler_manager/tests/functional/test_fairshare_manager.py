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

from datetime import datetime

from mock import MagicMock
from mock import patch
from synergy_scheduler_manager.common.project import Project
from synergy_scheduler_manager.common.user import User
from synergy_scheduler_manager.fairshare_manager import FairShareManager
from synergy_scheduler_manager.keystone_manager import KeystoneManager
from synergy_scheduler_manager.queue_manager import QueueManager
from synergy_scheduler_manager.quota_manager import QuotaManager
from synergy_scheduler_manager.tests.unit import base


class TestFairshareManager(base.TestCase):

    def setUp(self):
        super(TestFairshareManager, self).setUp()
        self.fsmanager = FairShareManager()

        # NOTE(vincent): we cannot import NovaManager in our tests.
        # NovaManager depends on the "nova" package (not novaclient), but it is
        # not available on PyPI so the test runner will fail to install it.
        nova_manager_mock = MagicMock()

        self.fsmanager.managers = {
            'NovaManager': nova_manager_mock(),
            'QueueManager': QueueManager(),
            'QuotaManager': QuotaManager(),
            'KeystoneManager': KeystoneManager()}

        # Mock the configuration since it is initiliazed by synergy-service.
        with patch('synergy_scheduler_manager.fairshare_manager.CONF'):
            self.fsmanager.setup()

    def test_add_project(self):
        project = Project()
        project.setId(1)
        project.setName("test_project")
        prj_share = project.getShare()
        prj_share.setValue(5)
        self.fsmanager.addProject(project)

        self.assertEqual(1, self.fsmanager.projects[1].getId())
        self.assertEqual("test_project", self.fsmanager.projects[1].getName())
        self.assertEqual([], self.fsmanager.projects[1].getUsers())
        self.assertEqual(5, self.fsmanager.projects[1].getShare().getValue())

    def test_add_project_no_share(self):
        project = Project()
        project.setId(1)
        project.setName("test_project")
        self.fsmanager.addProject(project)

        self.assertEqual(1, self.fsmanager.projects[1].getId())
        self.assertEqual("test_project", self.fsmanager.projects[1].getName())
        self.assertEqual([], self.fsmanager.projects[1].getUsers())
        self.assertEqual(self.fsmanager.default_share,
                         self.fsmanager.projects[1].getShare().getValue())

    def test_get_project(self):
        project = Project()
        project.setId(1)
        project.setName("test_project")
        self.fsmanager.addProject(project)

        self.assertEqual(project, self.fsmanager.getProject(1))

    def test_get_projects(self):
        project1 = Project()
        project1.setId(1)
        project1.setName("test1")
        self.fsmanager.addProject(project1)

        project2 = Project()
        project2.setId(2)
        project2.setName("test2")
        self.fsmanager.addProject(project2)

        expected_projects = {
            1: project1,
            2: project2}
        self.assertEqual(expected_projects, self.fsmanager.getProjects())

    def test_remove_project(self):
        project = Project()
        project.setId(1)
        project.setName("test_project")
        self.fsmanager.addProject(project)

        self.assertIn(1, self.fsmanager.projects)
        self.fsmanager.removeProject(1)
        self.assertNotIn(1, self.fsmanager.projects)

    def test_calculate_priority_one_user(self):
        # self.fsmanager.addProject(prj_id=1, prj_name="test")
        project = Project()
        project.setId(1)
        project.setName("test_project")

        # Define values used for computing the priority
        age_weight = self.fsmanager.age_weight = 1.0
        vcpus_weight = self.fsmanager.vcpus_weight = 2.0
        memory_weight = self.fsmanager.memory_weight = 3.0
        datetime_start = datetime(year=2000, month=1, day=1, hour=0, minute=0)
        datetime_stop = datetime(year=2000, month=1, day=1, hour=2, minute=0)
        minutes = (datetime_stop - datetime_start).seconds / 60
        fairshare_cores = 10
        fairshare_ram = 50

        # Add a user to the project
        user = User()
        user.setId(22)
        user.setName("test_user")
        priority = user.getPriority()
        priority.setFairShare('vcpus', fairshare_cores)
        priority.setFairShare('memory', fairshare_ram)

        project.addUser(user)
        self.fsmanager.addProject(project)

        # Compute the expected priority given the previously defined values
        expected_priority = int(age_weight * minutes +
                                vcpus_weight * fairshare_cores +
                                memory_weight * fairshare_ram)

        with patch("synergy_scheduler_manager.fairshare_manager.datetime") \
                as datetime_mock:
            datetime_mock.utcnow.side_effect = (datetime_start, datetime_stop)
            priority = self.fsmanager.calculatePriority(user_id=22, prj_id=1)

        self.assertEqual(expected_priority, priority)

    def test_calculate_fairshare(self):
        # TODO(vincent)
        pass
