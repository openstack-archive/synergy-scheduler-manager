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

from synergy_scheduler_manager.common.endpoint import Endpoint
from synergy_scheduler_manager.tests.unit import base


class TestEndpoint(base.TestCase):
    def setUp(self):
        super(TestEndpoint, self).setUp()
        self.endp = Endpoint()

    def test_set_get_interface(self):
        self.endp.setInterface('pippo')
        self.assertEqual('pippo', self.endp.getInterface())

    def test_set__get_region(self):
        self.endp.setRegion('region')
        self.assertEqual('region', self.endp.getRegion())

    def test_set_get_region_id(self):
        self.endp.setRegionId('region_id')
        self.assertEqual('region_id', self.endp.getRegionId())

    def test_set_get_service_id(self):
        self.endp.setServiceId('service_id')
        self.assertEqual('service_id', self.endp.getServiceId())

    def test_set_get_URL(self):
        self.endp.setURL('URL')
        self.assertEqual('URL', self.endp.getURL())

    def test_set_isenable(self):
        self.endp.setEnabled('true')
        self.assertEqual('true', self.endp.isEnabled())
