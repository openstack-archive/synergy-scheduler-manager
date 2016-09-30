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

from synergy_scheduler_manager.common.trust import Trust

from synergy_scheduler_manager.tests.unit import base


class TestTrust(base.TestCase):

    def setUp(self):
        super(TestTrust, self).setUp()
        # fake data
        data = {"id": "id", "trustor_user_id": "trustor_user_id",
                "impersonation": "impersonation",
                "trustee_user_id": "trustee_user_id",
                "links": [],
                "roles": [],
                "remaining_uses": "remaining_uses",
                "expires_at": None,
                "keystone_url": None,
                "project_id": "project_id"
                }
        self.trust = Trust(data)

    def test_get_Id(self):
        self.assertEqual('id', self.trust.getId())

    def test_isImpersonations(self):
        self.assertEqual('impersonation', self.trust.isImpersonations())

    def getRolesLinks(self):
        pass

    def test_get_trustorUserId(self):
        self.assertEqual('trustor_user_id', self.trust.getTrustorUserId())

    def test_get_trusteeUserId(self):
        self.assertEqual('trustee_user_id', self.trust.getTrusteeUserId())

    def test_get_roles(self):
        self.assertEqual([], self.trust.getRoles())

    def test_get_links(self):
        self.assertEqual([], self.trust.getlinks())

    def test_get_remainingUses(self):
        self.assertEqual("remaining_uses", self.trust.getRemainingUses())

    def test_get_expiration(self):
        self.assertEqual(None, self.trust.getExpiration())

    def test_isExpired(self):
        self.assertEqual(False, self.trust.isExpired())
