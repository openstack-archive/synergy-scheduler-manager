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


from synergy_scheduler_manager.queue import Queue
from synergy_scheduler_manager.tests import base


class TestQueue(base.TestCase):

    def setUp(self):
        self.queue = Queue(name="dummy_q", pool=None)

    def test_name(self):
        self.assertEqual(self.queue.name, "dummy_q")
        self.assertEqual(1, 1)
