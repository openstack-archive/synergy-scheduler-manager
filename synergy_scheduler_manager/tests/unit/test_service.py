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

from synergy_scheduler_manager.common.service import Service
from synergy_scheduler_manager.tests.unit import base


class TestService(base.TestCase):
    def setUp(self):
        super(TestService, self).setUp()
        self.service = Service()

    def test_set_get_Type(self):
        self.service.setType('type')
        self.assertEqual('type', self.service.getType())

    def test_get_Endpoints(self):
        self.assertEqual([], self.service.getEndpoints())

    def test_get_Endpoint(self):
        self.assertEqual(None, self.service.getEndpoint('interface'))

    def test_set_get_Description(self):
        self.service.setDescription('description')
        self.assertEqual('description', self.service.getDescription())

    def test_set_get_Enabled(self):
        self.service.setEnabled(True)
        self.assertEqual(True, self.service.isEnabled())

    def test_isEnable(self):
        self.assertEqual(False, self.service.isEnabled())
