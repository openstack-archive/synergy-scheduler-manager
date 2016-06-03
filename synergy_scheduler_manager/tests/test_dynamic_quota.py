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

from synergy_scheduler_manager.quota_manager import DynamicQuota
from synergy_scheduler_manager.tests import base


class TestDynamicQuota(base.TestCase):

    def setUp(self):
        super(TestDynamicQuota, self).setUp()
        self.dyn_quota = DynamicQuota()

    def test_add_project(self):
        project_id = 1
        self.dyn_quota.addProject(project_id, "test_project")
        self.assertIn(project_id, self.dyn_quota.getProjects())
