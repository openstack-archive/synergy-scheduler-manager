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

from synergy_scheduler_manager.common.compute import Compute
from synergy_scheduler_manager.tests.unit import base


class TestCompute(base.TestCase):
    def setUp(self):
        super(TestCompute, self).setUp()
        self.comp = Compute()

    def test_set_get_Host(self):
        self.comp.setHost('host')
        self.assertEqual('host', self.comp.getHost())

    def test_set_get_NodeName(self):
        self.comp.setNodeName('node_name')
        self.assertEqual('node_name', self.comp.getNodeName())

    def test_set_get_Limits(self):
        self.comp.setLimits('limits')
        self.assertEqual('limits', self.comp.getLimits())
