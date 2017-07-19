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
from synergy_scheduler_manager.project_manager import ProjectManager
from synergy_scheduler_manager.tests.unit import base


class TestFairshareManager(base.TestCase):

    def setUp(self):
        super(TestFairshareManager, self).setUp()
        self.fairshare_manager = FairShareManager()
        self.project_manager = ProjectManager()

        # NOTE(vincent): we cannot import NovaManager in our tests.
        # NovaManager depends on the "nova" package (not novaclient), but it is
        # not available on PyPI so the test runner will fail to install it.
        nova_manager_mock = MagicMock()

        self.fairshare_manager.managers = {
            'NovaManager': nova_manager_mock(),
            'ProjectManager': self.project_manager}

        # Mock the configuration since it is initiliazed by synergy-service.
        with patch('synergy_scheduler_manager.fairshare_manager.CONF'):
            self.fairshare_manager.setup()

    def test_calculate_priority_one_user(self):
        # self.fairshare_manager.addProject(prj_id=1, prj_name="test")
        project = Project()
        project.setId(1)
        project.setName("test_project")

        # Define values used for computing the priority
        age_weight = self.fairshare_manager.age_weight = 1.0
        vcpus_weight = self.fairshare_manager.vcpus_weight = 2.0
        memory_weight = self.fairshare_manager.memory_weight = 3.0
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
        self.project_manager.projects[project.getId()] = project

        # Compute the expected priority given the previously defined values
        expected_priority = int(age_weight * minutes +
                                vcpus_weight * fairshare_cores +
                                memory_weight * fairshare_ram)

        with patch("synergy_scheduler_manager.fairshare_manager.datetime") \
                as datetime_mock:
            datetime_mock.utcnow.side_effect = (datetime_start, datetime_stop)
            priority = self.fairshare_manager.calculatePriority(
                user.getId(), project.getId())

        self.assertEqual(expected_priority, priority)

    def test_calculate_fairshare(self):
        # TODO(vincent)
        pass
