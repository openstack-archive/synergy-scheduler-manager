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
from synergy_scheduler_manager.common.project import Project
from synergy_scheduler_manager.common.token import Token
from synergy_scheduler_manager.common.user import User
from synergy_scheduler_manager.tests.unit import base


class TestToken(base.TestCase):

    def setUp(self):
        super(TestToken, self).setUp()
        self.token = Token()

    def test_get_Services(self):
        self.assertEqual([], self.token.getServices())

    def test_set_get_Creation(self):
        self.token.setCreation('issue_at')
        self.assertEqual('issue_at', self.token.getCreation())

    def test_set_get_Expiration(self):
        self.token.setExpiration('expires_at')
        self.assertEqual('expires_at', self.token.getExpiration())

    def test_set_get_Extras(self):
        self.assertEqual({}, self.token.getExtras())

    def test_set_get_Project(self):
        prj = Project()
        self.token.setProject(prj)
        self.assertEqual(0.0, self.token.getProject().getTTL())

    def test_get_Roles(self):
        self.assertEqual({}, self.token.getExtras())

    def test_set_get_User(self):
        user = User()
        self.token.setUser(user)
        self.assertEqual({}, self.token.getUser().getData())

    def test_isAdmin(self):
        self.assertEqual(False, self.token.isAdmin())

    def test_issuedAt(self):
        self.token.setCreation('now')
        self.assertEqual('now', self.token.issuedAt())

    def test_isExpired(self):
        str_date = '2015-02-10T13:00:10Z'
        datetime_date = datetime.strptime(
            str_date, "%Y-%m-%dT%H:%M:%SZ")
        self.token.setExpiration(datetime_date)
        self.assertEqual(True, self.token.isExpired())

    def test_parse(self):
        tok = {
            u'token': {
                u'methods': [u'password'],
                u'roles': [{u'id': u'1efd5e18c8414086889ac93d8b11c411',
                            u'name': u'admin'}],
                u'expires_at': u'2016-09-02T12:07:45.814651Z',
                u'project': {u'domain': {u'id': u'default',
                                         u'name': u'Default'},
                             u'id': u'1a6edd87f9ec41d8aa64c8f23d719c2a',
                             u'name': u'admin'},
                u'catalog': [{
                    u'endpoints': [{
                        u'url': u'http://10.64.31.19:8774/v2/1a6edd8a',
                        u'interface': u'internal',
                        u'region': u'RegionOne',
                        u'region_id': u'RegionOne',
                        u'id': u'ac178ac5b9f34647b9fce954966470c9'},
                        {u'url': u'http://10.64.31.19:8774/v2/1a6edd2a',
                         u'interface': u'admin',
                         u'region': u'RegionOne',
                                    u'region_id': u'RegionOne',
                         u'id': u'b81a3518d24d433495985a31740a5b84'},
                        {u'url': u'http://10.64.31.19:8774/v2/1a6ec2a',
                         u'interface': u'public',
                         u'region': u'RegionOne',
                         u'region_id': u'RegionOne',
                         u'id': u'db03d5f72d89461582d4d87e7f82302a'}],
                    u'type': u'compute',
                    u'id': u'27dd094445aa42a9b05ca08c1f094d28',
                    u'name': u'nova'},
                    {
                    u'endpoints': [{
                        u'url': u'http://10.64.31.19:9292',
                        u'interface': u'internal',
                        u'region': u'RegionOne',
                        u'region_id': u'RegionOne',
                        u'id': u'2f5458cf516147ffa1be04fabfbec9f8'},
                        {u'url': u'http://10.64.31.19:9292',
                         u'interface': u'admin',
                         u'region': u'RegionOne',
                                    u'region_id': u'RegionOne',
                                    u'id': u'468db3a59f644d5d8cea849a'},
                        {u'url': u'http://10.64.31.19:9292',
                         u'interface': u'public',
                         u'region': u'RegionOne',
                                    u'region_id': u'RegionOne',
                                    u'id': u'a13689dba17c432aa4c1f5b'}],
                    u'type': u'image',
                    u'id': u'6d80825e9f4544ea9a2d0a2d952b2e7c',
                    u'name': u'glance'},
                    {u'endpoints': [{u'url': u'http://10.64.31.19:9696',
                                     u'interface': u'admin',
                                     u'region': u'RegionOne',
                                     u'region_id': u'RegionOne',
                                     u'id': u'567ed76f7161484f9c2556'},
                                    {u'url': u'http://10.64.31.19:9696',
                                     u'interface': u'internal',
                                     u'region': u'RegionOne',
                                     u'region_id': u'RegionOne',
                                     u'id': u'90373f0a87bc41cc99095'},
                                    {u'url': u'http://10.64.31.19:9696',
                                     u'interface': u'public',
                                     u'region': u'RegionOne',
                                     u'region_id': u'RegionOne',
                                     u'id': u'a95ad02ab '}],
                     u'type': u'network',
                              u'id': u'8f4c00132ccb422cbc0b6139266ad1df',
                              u'name': u'neutron'},
                    {u'endpoints': [{u'url': u'http://10.64.31.19:50v3',
                                     u'interface': u'internal',
                                     u'region': u'RegionOne',
                                     u'region_id': u'RegionOne',
                                     u'id': u'494ccc932f0a6f8a86e629'},
                                    {u'url': u'http://10.64.31.19:5/v3',
                                     u'interface': u'public',
                                     u'region': u'RegionOne',
                                     u'region_id': u'RegionOne',
                                     u'id': u'5503912c691c2b25e1855f9'},
                                    {u'url': u'http://10.64.31.19357/v3',
                                     u'interface': u'admin',
                                     u'region': u'RegionOne',
                                     u'region_id': u'RegionOne',
                                     u'id': u'6d54bb99a2994ea8f527'}],
                     u'type': u'identity',
                              u'id': u'af19f5adf97c4f3fb1aa9d4fe499dd9',
                              u'name': u'keystone'},
                    {u'endpoints': [{u'url': u'http://10.64.31.19:8051',
                                     u'interface': u'public',
                                     u'region': u'RegionOne',
                                     u'region_id': u'RegionOne',
                                     u'id': u'68d6e8128cf84037138a3d'},
                                    {u'url': u'http://10.64.31.19:8051',
                                     u'interface': u'admin',
                                     u'region': u'RegionOne',
                                     u'region_id': u'RegionOne',
                                     u'id': u'bc0bb63fd58044387a0e61b'},
                                    {u'url': u'http://10.64.31.19:8051',
                                     u'interface': u'internal',
                                     u'region': u'RegionOne',
                                     u'region_id': u'RegionOne',
                                     u'id': u'f1bc92a85cc80bac0d'}],
                     u'type': u'management',
                              u'id': u'b1c0d32b48934ac380f6a231dce448f0',
                              u'name': u'synergy'}],
                u'extras': {},
                u'user': {u'domain': {u'id': u'default',
                                      u'name': u'Default'},
                          u'id': u'9364456de70b46e59ba4dbd74498ccb0',
                          u'name': u'admin'},
                u'audit_ids': [u'2XDA6g5nQeuuSg-vvhPzfw'],
                u'issued_at': u'2016-09-02T11:57:45.814689Z'}}

        self.tk = Token.parse('tok_id', tok)
        self.assertEqual({}, self.tk.getExtras())

        str_date = '2016-09-02T12:07:45.814651Z'
        datetime_date = datetime.strptime(str_date,
                                          "%Y-%m-%dT%H:%M:%S.%fZ")
        self.assertEqual(datetime_date, self.tk.getExpiration())
