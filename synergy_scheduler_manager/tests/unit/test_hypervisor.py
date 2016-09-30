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

from synergy_scheduler_manager.common.hypervisor import Hypervisor
from synergy_scheduler_manager.tests.unit import base


class TestHypervisor(base.TestCase):
    def setUp(self):
        super(TestHypervisor, self).setUp()
        self.hyp = Hypervisor()

    def test_set_get_IP(self):
        self.hyp.setIP('ip')
        self.assertEqual('ip', self.hyp.getIP())

    def test_set_get_State(self):
        self.hyp.setState('state')
        self.assertEqual('state', self.hyp.getState())

    def test_set_get_Workload(self):
        self.assertEqual(0, self.hyp.getWorkload())
        self.hyp.setWorkload(8)
        self.assertEqual(8, self.hyp.getWorkload())

    def test_set_get_VMs(self):
        self.assertEqual(0, self.hyp.getVMs())
        self.hyp.setVMs(3)
        self.assertEqual(3, self.hyp.getVMs())

    def test_set_get_VCPUs(self):
        self.assertEqual(0, self.hyp.getVCPUs(False))
        self.assertEqual(0, self.hyp.getVCPUs(True))
        self.hyp.setVCPUs(3, False)
        self.hyp.setVCPUs(8, True)
        self.assertEqual(3, self.hyp.getVCPUs(False))
        self.assertEqual(8, self.hyp.getVCPUs(True))

    def test_set_get_Memory(self):
        self.assertEqual(0, self.hyp.getMemory(False))
        self.assertEqual(0, self.hyp.getMemory(True))
        self.hyp.setMemory(1, False)
        self.hyp.setMemory(2, True)
        self.assertEqual(1, self.hyp.getMemory(False))
        self.assertEqual(2, self.hyp.getMemory(True))
