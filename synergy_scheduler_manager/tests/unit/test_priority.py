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
from synergy_scheduler_manager.common.priority import Priority
from synergy_scheduler_manager.tests.unit import base


class TestPriority(base.TestCase):
    def setUp(self):
        super(TestPriority, self).setUp()
        self.priority = Priority()

    def test_set_get_Value(self):
        self.priority.setValue(1)
        self.assertEqual(1, self.priority.getValue())

    def test_set_get_LastUpdate(self):
        self.assertNotEqual(datetime.utcnow(), self.priority.getLastUpdate())

    def test_set_get_FairShare(self):
        self.assertEqual(0.00, self.priority.getFairShare('vcpus'))
        self.assertEqual(0.0, self.priority.getFairShare('memory'))
        self.assertEqual(0.0, self.priority.getFairShare('disk'))
        self.priority.setFairShare('vcpus', 2)
        self.assertEqual(2, self.priority.getFairShare('vcpus'))
        self.priority.setFairShare('memory', 5.6)
        self.assertEqual(5.6, self.priority.getFairShare('memory'))
        self.priority.setFairShare('disk', 0.1)
        self.assertEqual(0.1, self.priority.getFairShare('disk'))
