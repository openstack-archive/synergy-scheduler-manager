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
from synergy_scheduler_manager.common.user import User
from synergy_scheduler_manager.tests.unit import base


class TestUser(base.TestCase):
    def setUp(self):
        super(TestUser, self).setUp()
        self.user = User()

    def test_get_Data(self):
        data = self.user.getData()
        data['data'] = 'data'
        self.assertEqual('data', data['data'])

    def test_set_get_ProjectId(self):
        self.user.setProjectId('project_id')
        self.assertEqual('project_id', self.user.getProjectId())

    def test_set_get_Role(self):
        self.user.setRole('role')
        self.assertEqual('role', self.user.getRole())

    def test_get_Priority(self):
        priority = self.user.getPriority()
        self.assertEqual(0, priority.getValue())
        self.assertNotEqual(datetime.utcnow(), priority.getLastUpdate())
        self.assertEqual(0.0, priority.getFairShare('vcpus'))
        self.assertEqual(0.0, priority.getFairShare('memory'))
        self.assertEqual(0.0, priority.getFairShare('disk'))

    def test_get_Share(self):
        share = self.user.getShare()
        self.assertEqual(0.0, share.getValue())
        self.assertEqual(0.00, share.getSiblingValue())
        self.assertEqual(0.000, share.getNormalizedValue())

    def test_set_isenable(self):
        self.user.setEnabled('true')
        self.assertEqual('true', self.user.isEnabled())
