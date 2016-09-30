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
from synergy_scheduler_manager.common.server import Server
from synergy_scheduler_manager.tests.unit import base


class TestServer(base.TestCase):
    def setUp(self):
        super(TestServer, self).setUp()
        self.server = Server()

    def test_set_get_Type(self):
        self.server.setType('type')
        self.assertEqual('type', self.server.getType())

    def test_set_get_State(self):
        self.server.setState('state')
        self.assertEqual('state', self.server.getState())

    def test_set_get_Flavor(self):
        self.server.setFlavor('flavor')
        self.assertEqual('flavor', self.server.getFlavor())

    def test_set_get_KeyName(self):
        self.server.setKeyName('keyname')
        self.assertEqual('keyname', self.server.getKeyName())

    def test_set_get_Metadata(self):
        self.server.setMetadata('metadata')
        self.assertEqual('metadata', self.server.getMetadata())

    def test_set_get_UserId(self):
        self.server.setUserId('user_id')
        self.assertEqual('user_id', self.server.getUserId())

    def test_set_get_ProjectId(self):
        self.server.setProjectId('project_id')
        self.assertEqual('project_id', self.server.getProjectId())

    def test_set_get_CreatedAt(self):
        self.server.setCreatedAt('2015-02-10T13:00:10Z')
        str_date = '2015-02-10T13:00:10Z'
        datetime_date = datetime.strptime(str_date, "%Y-%m-%dT%H:%M:%SZ")
        self.assertEqual(datetime_date, self.server.getCreatedAt())

    def test_set_get_LaunchedAt(self):
        self.server.setLaunchedAt('2015-02-10T13:00:10Z')
        str_date = '2015-02-10T13:00:10Z'
        datetime_date = datetime.strptime(str_date, "%Y-%m-%dT%H:%M:%SZ")
        self.assertEqual(datetime_date, self.server.getLaunchedAt())

    def test_set_get_UpdatedAt(self):
        self.server.setUpdatedAt('2015-02-10T13:00:10Z')
        str_date = '2015-02-10T13:00:10Z'
        datetime_date = datetime.strptime(str_date, "%Y-%m-%dT%H:%M:%SZ")
        self.assertEqual(datetime_date, self.server.getUpdatedAt())

    def test_set_get_TerminatedAt(self):
        self.server.setTerminatedAt('2015-02-10T13:00:10Z')
        str_date = '2015-02-10T13:00:10Z'
        datetime_date = datetime.strptime(str_date, "%Y-%m-%dT%H:%M:%SZ")
        self.assertEqual(datetime_date, self.server.getTerminatedAt())

    def test_is_Ephemeral(self):
        self.assertEqual(False, self.server.isEphemeral())

    def test_is_Permanent(self):
        self.assertEqual(True, self.server.isPermanent())
