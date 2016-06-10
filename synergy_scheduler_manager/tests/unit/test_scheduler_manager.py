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

from mock import call
from mock import create_autospec

from synergy_scheduler_manager.quota_manager import DynamicQuota
from synergy_scheduler_manager.scheduler_manager import Notifications
from synergy_scheduler_manager.tests.unit import base


class TestNotifications(base.TestCase):

    def test_info_dynamic_quota(self):
        """Test that info() makes the correct call to DynamicQuota"""
        dynquota_mock = create_autospec(DynamicQuota)
        ns = Notifications(dynquota_mock)

        payload = {
            "state": "deleted",
            "instance_id": 1,
            "tenant_id": 2,
            "memory_mb": 3,
            "vcpus": 4}
        ns.info(ctxt=None,
                publisher_id=None,
                event_type="compute.instance.delete.end",
                payload=payload,
                metadata=None)

        self.assertEqual(call(1, 2, 4, 3), dynquota_mock.release.call_args)
