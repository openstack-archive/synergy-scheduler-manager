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

from synergy_scheduler_manager.common.flavor import Flavor
from synergy_scheduler_manager.tests.unit import base


class TestFlavor(base.TestCase):
    def setUp(self):
        super(TestFlavor, self).setUp()
        self.flav = Flavor()

    def test_set_get_VCPUs(self):
        self.flav.setVCPUs(2)
        self.assertEqual(2, self.flav.getVCPUs())

    def test_set__get_Memory(self):
        self.flav.setMemory('memory')
        self.assertEqual('memory', self.flav.getMemory())

    def test_set_get_Storage(self):
        self.flav.setMemory('storage')
        self.assertEqual('storage', self.flav.getMemory())
