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


from mock import MagicMock
from mock import patch
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

    def test_calculate_fairshare(self):
        # TODO(vincent)
        pass
