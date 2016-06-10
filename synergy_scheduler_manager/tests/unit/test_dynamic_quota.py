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

from synergy_scheduler_manager.quota_manager import DynamicQuota
from synergy_scheduler_manager.tests.unit import base


class TestDynamicQuota(base.TestCase):

    def setUp(self):
        super(TestDynamicQuota, self).setUp()
        self.dyn_quota = DynamicQuota()

    def test_get_add_project_no_usage(self):
        self.dyn_quota.addProject(prj_id=1, prj_name="test_project")

        project = self.dyn_quota.getProject(1)
        self.assertEqual("test_project", project["name"])
        self.assertEqual(0, project["cores"])
        self.assertEqual(0, project["ram"])
        self.assertEqual({"active": [], "pending": []}, project["instances"])
        self.assertEqual(0, project["TTL"])

    def test_get_add_project_with_usage(self):
        fake_usage = {"cores": 5, "ram": 12, "instances": ["a", "b"]}
        self.dyn_quota.addProject(prj_id=1, prj_name="test", usage=fake_usage)

        project = self.dyn_quota.getProject(1)
        self.assertEqual("test", project["name"])
        self.assertEqual(5, project["cores"])
        self.assertEqual(12, project["ram"])
        self.assertEqual({"active": ["a", "b"], "pending": []},
                         project["instances"])
        self.assertEqual(0, project["TTL"])
        self.assertEqual(12, self.dyn_quota.ram["in_use"])
        self.assertEqual(5, self.dyn_quota.cores["in_use"])

    def test_get_size(self):
        size = self.dyn_quota.getSize()
        self.assertEqual(0, size["cores"])
        self.assertEqual(0, size["ram"])

    def test_set_size(self):
        self.dyn_quota.setSize(cores=10, ram=20)
        self.assertEqual(10, self.dyn_quota.cores["limit"])
        self.assertEqual(20, self.dyn_quota.ram["limit"])

    def test_get_projects(self):
        self.assertEqual(self.dyn_quota.projects, self.dyn_quota.getProjects())

    def test_remove_project(self):
        self.dyn_quota.addProject(prj_id=1, prj_name="test")

        self.assertIn(1, self.dyn_quota.projects)

        self.dyn_quota.removeProject(1)
        self.assertNotIn(1, self.dyn_quota.projects)

    def test_allocate_single_instance(self):
        self.dyn_quota.setSize(cores=20, ram=100)
        self.dyn_quota.addProject(prj_id=1, prj_name="test")

        self.dyn_quota.allocate("a", prj_id=1, cores=5, ram=10)

        project = self.dyn_quota.getProject(1)
        self.assertIn("a", project["instances"]["active"])
        self.assertEqual(5, project["cores"])
        self.assertEqual(10, project["ram"])
        self.assertEqual(5, self.dyn_quota.cores["in_use"])
        self.assertEqual(10, self.dyn_quota.ram["in_use"])

    def test_allocate_multiple_instances(self):
        self.dyn_quota.setSize(cores=30, ram=100)
        self.dyn_quota.addProject(prj_id=1, prj_name="test")

        self.dyn_quota.allocate("a", prj_id=1, cores=5, ram=10)
        self.dyn_quota.allocate("b", prj_id=1, cores=7, ram=20)
        self.dyn_quota.allocate("c", prj_id=1, cores=10, ram=20)

        project = self.dyn_quota.getProject(1)
        self.assertIn("a", project["instances"]["active"])
        self.assertIn("b", project["instances"]["active"])
        self.assertIn("c", project["instances"]["active"])
        self.assertEqual(22, project["cores"])
        self.assertEqual(50, project["ram"])
        self.assertEqual(22, self.dyn_quota.cores["in_use"])
        self.assertEqual(50, self.dyn_quota.ram["in_use"])

    def test_allocate_multiple_projects(self):
        self.dyn_quota.setSize(cores=20, ram=100)
        self.dyn_quota.addProject(prj_id=1, prj_name="project_A")
        self.dyn_quota.addProject(prj_id=2, prj_name="project_B")

        # TODO(vincent): can we allocate the same instance to 2 projects?
        self.dyn_quota.allocate("a", prj_id=1, cores=3, ram=10)
        self.dyn_quota.allocate("a", prj_id=2, cores=5, ram=15)

        project_a = self.dyn_quota.getProject(1)
        project_b = self.dyn_quota.getProject(2)
        self.assertIn("a", project_a["instances"]["active"])
        self.assertIn("a", project_b["instances"]["active"])
        self.assertEqual(3, project_a["cores"])
        self.assertEqual(10, project_a["ram"])
        self.assertEqual(5, project_b["cores"])
        self.assertEqual(15, project_b["ram"])
        self.assertEqual(8, self.dyn_quota.cores["in_use"])
        self.assertEqual(25, self.dyn_quota.ram["in_use"])
