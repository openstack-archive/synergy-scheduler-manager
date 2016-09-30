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

from synergy_scheduler_manager.common.project import Project
from synergy_scheduler_manager.common.user import User
from synergy_scheduler_manager.tests.unit import base


class TestProject(base.TestCase):
    def setUp(self):
        super(TestProject, self).setUp()
        self.prj = Project()

    def test_set_get_Data(self):
        self.assertEqual({}, self.prj.getData())

    def test_get_Quota(self):
        sq = self.prj.getQuota()
        sq.setSize('memory', 1024, True)
        sq.setSize('vcpus', 100.00, True)
        sq.setSize('instances', 10, True)

        self.assertEqual(1024, sq.getSize('memory'))
        self.assertEqual(100.0, sq.getSize('vcpus'))
        self.assertEqual(10, sq.getSize('instances'))

    def test_get_Share(self):
        share = self.prj.getShare()
        self.assertEqual(0.0, share.getValue())
        self.assertEqual(0.00, share.getSiblingValue())
        self.assertEqual(0.000, share.getNormalizedValue())

    def test_set_get_TTL(self):
        self.prj.setTTL(0.2)
        self.assertEqual(0.2, self.prj.getTTL())

    def test_set_is_Enable(self):
        self.prj.setEnabled('True')
        self.assertEqual('True', self.prj.isEnabled())

    def test_get_add_User(self):
        user1 = User()
        user1.setId('id1')
        user1.setName('name1')
        self.prj.addUser(user1)

        user2 = User()
        user2.setId('id2')
        user2.setName('name2')
        self.prj.addUser(user2)

        self.assertEqual('id1', self.prj.getUser('id1').getId())
        self.assertEqual('name1', self.prj.getUser('id1').getName())
        self.assertEqual('id2', self.prj.getUser('id2').getId())
        self.assertEqual('name2', self.prj.getUser('id2').getName())

    def test_get_Users(self):
        user = User()
        user.setId('id1')
        user.setName('name')
        self.prj.addUser(user)
        self.assertEqual('name', self.prj.getUser('id1').getName())
