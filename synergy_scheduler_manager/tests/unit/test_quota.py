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
from synergy_scheduler_manager.common.quota import Quota
from synergy_scheduler_manager.common.quota import SharedQuota
from synergy_scheduler_manager.common.server import Server
from synergy_scheduler_manager.tests.unit import base


class TestQuota(base.TestCase):
    def setUp(self):
        super(TestQuota, self).setUp()
        self.quota = Quota()

    def test_get_set_Type(self):
        self.quota.setType('Type')
        self.assertEqual('Type', self.quota.getType())

    def test_get_Servers(self):
        self.assertEqual([], self.quota.getServers('active', private=True))
        self.assertEqual([], self.quota.getServers('building', private=True))
        self.assertEqual([], self.quota.getServers('error', private=True))

        self.assertEqual([], self.quota.getServers('active', private=False))
        self.assertEqual([], self.quota.getServers('building', private=False))
        self.assertEqual([], self.quota.getServers('error', private=False))

    def test_Reset(self):
        self.quota.reset()
        self.assertEqual(0.0, self.quota.get('private')[
                         'resources']['memory']['used'])
        self.assertEqual(0.0, self.quota.get('private')[
                         'resources']['memory']['used'])
        self.assertEqual(0.0, self.quota.get('private')[
                         'resources']['memory']['used'])

    def test_set_get_Size(self):
        self.quota.setSize('memory', 1024, private=True)
        self.quota.setSize('vcpus', 100.00, private=True)
        self.quota.setSize('instances', 10, private=True)

        self.assertEqual(1024, self.quota.getSize('memory', private=True))
        self.assertEqual(100.0, self.quota.getSize('vcpus', private=True))
        self.assertEqual(10, self.quota.getSize('instances', private=True))

        self.quota.setSize('memory', 1, private=False)
        self.quota.setSize('vcpus', 11, private=False)
        self.quota.setSize('instances', 111, private=False)

        self.assertEqual(1, self.quota.getSize('memory', private=False))
        self.assertEqual(11, self.quota.getSize('vcpus', private=False))
        self.assertEqual(111, self.quota.getSize('instances', private=False))

    def test_set_get_Usage(self):
        self.quota.setUsage('memory', 1024, private=True)
        self.quota.setUsage('vcpus', 100.00, private=True)
        self.quota.setUsage('instances', 10, private=True)

        self.assertEqual(1024, self.quota.getUsage('memory', private=True))
        self.assertEqual(100.0, self.quota.getUsage('vcpus', private=True))
        self.assertEqual(10, self.quota.getUsage('instances', private=True))

        self.quota.setUsage('memory', 1024, private=False)
        self.quota.setUsage('vcpus', 100.00, private=False)
        self.quota.setUsage('instances', 10, private=False)

        self.assertEqual(1024, self.quota.getUsage('memory', private=False))
        self.assertEqual(100.0, self.quota.getUsage('vcpus', private=False))
        self.assertEqual(10, self.quota.getUsage('instances', private=False))

        self.quota.reset()
        self.assertEqual(0, self.quota.getUsage('memory', private=True))
        self.assertEqual(0, self.quota.getUsage('vcpus', private=True))
        self.assertEqual(0, self.quota.getUsage('instances', private=True))

    def test_Allocate(self):
        self.quota.setSize("vcpus", 10, private=True)
        self.quota.setSize("memory", 2048, private=True)

        flavor = Flavor()
        flavor.setVCPUs(3)
        flavor.setMemory(1024)

        server = Server()
        server.setId("test_id")
        server.setFlavor(flavor)
        found = self.quota.allocate(server, blocking=False)
        self.assertEqual(3, self.quota.getUsage('vcpus', private=True))
        self.assertEqual(1024, self.quota.getUsage('memory', private=True))
        self.assertEqual(True, found)

    def test_release(self):
        self.quota.setSize("vcpus", 10, private=True)
        self.quota.setSize("memory", 2048, private=True)

        flavor = Flavor()
        flavor.setVCPUs(3)
        flavor.setMemory(1024)

        server = Server()
        server.setId("test_id")
        server.setFlavor(flavor)
        self.quota.release(server)
        self.assertEqual(0, self.quota.getUsage('vcpus', True))
        self.assertEqual(0, self.quota.getUsage('memory', True))


class TestSharedQuota(base.TestCase):

    def setUp(self):
        super(TestSharedQuota, self).setUp()
        self.quota = Quota()

    def test_enabled_disable(self):
        SharedQuota.enable()
        self.assertEqual(True, SharedQuota.isEnabled())
        SharedQuota.disable()
        self.assertEqual(False, SharedQuota.isEnabled())

    def test_set_get_Size(self):
        SharedQuota.setSize("vcpus", 1024)
        SharedQuota.setSize("memory", 2048)
        SharedQuota.setSize("instances", 10)

        self.assertEqual(1024, SharedQuota.getSize('vcpus'))
        self.assertEqual(2048, SharedQuota.getSize('memory'))
        self.assertEqual(10, SharedQuota.getSize('instances'))

    def test_set_get_Usage(self):
        SharedQuota.setUsage('memory', 1024)
        SharedQuota.setUsage('vcpus', 30)
        self.assertEqual(1024, SharedQuota.getUsage('memory'))
        self.assertEqual(30, SharedQuota.getUsage('vcpus'))

    def test_Allocate_Release(self):
        SharedQuota.enable()
        SharedQuota.setSize("vcpus", 20)
        SharedQuota.setSize("memory", 4086)

        self.quota.setSize("vcpus", 10)
        self.quota.setSize("memory", 2048)

        flavor1 = Flavor()
        flavor1.setVCPUs(2)
        flavor1.setMemory(2)

        server1 = Server()
        server1.setId("test_id1")
        server1.setType("ephemeral")

        server1.setFlavor(flavor1)
        self.quota.allocate(server1, blocking=False)

        self.assertEqual(2, SharedQuota.getUsage('memory'))
        self.assertEqual(2, SharedQuota.getUsage('vcpus'))
        self.assertEqual(0, self.quota.getUsage("memory", private=True))
        self.assertEqual(0, self.quota.getUsage("vcpus", private=True))

        flavor2 = Flavor()
        flavor2.setVCPUs(3)
        flavor2.setMemory(3)

        server2 = Server()
        server2.setId("test_id2")
        server2.setType("permanent")

        server2.setFlavor(flavor2)

        self.quota.allocate(server2, blocking=False)

        self.assertEqual(2, SharedQuota.getUsage('memory'))
        self.assertEqual(2, SharedQuota.getUsage('vcpus'))
        self.assertEqual(3, self.quota.getUsage("memory", private=True))
        self.assertEqual(3, self.quota.getUsage("vcpus", private=True))

        self.assertEqual(2, self.quota.getUsage("memory", private=False))
        self.assertEqual(2, self.quota.getUsage("vcpus", private=False))

        self.quota.release(server1)

        self.assertEqual(0, SharedQuota.getUsage('memory'))
        self.assertEqual(0, SharedQuota.getUsage('vcpus'))

        self.assertEqual(0, self.quota.getUsage("vcpus", private=False))
        self.assertEqual(0, self.quota.getUsage("vcpus", private=False))

        self.quota.release(server2)

        self.assertEqual(0, self.quota.getUsage("vcpus", private=True))
        self.assertEqual(0, self.quota.getUsage("vcpus", private=True))
