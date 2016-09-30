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

from synergy_scheduler_manager.common.user import Share
from synergy_scheduler_manager.tests.unit import base


class TestShare(base.TestCase):
    def setUp(self):
        super(TestShare, self).setUp()
        self.share = Share()

    def test_set_get_Value(self):
        self.share.setValue('value')
        self.assertEqual('value', self.share.getValue())

    def test_set_get_SiblingValue(self):
        self.share.setSiblingValue('sibling_value')
        self.assertEqual('sibling_value', self.share.getSiblingValue())

    def test_set_get_NormalizedValue(self):
        self.share.setNormalizedValue('normalized_value')
        self.assertEqual('normalized_value', self.share.getNormalizedValue())
