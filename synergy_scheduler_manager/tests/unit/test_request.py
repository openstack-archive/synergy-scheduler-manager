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

from synergy_scheduler_manager.common.request import Request
from synergy_scheduler_manager.tests.unit import base


class TestRequest(base.TestCase):

    def setUp(self):
        super(TestRequest, self).setUp()

        req_dict = {
            'legacy_bdm': True,
            'requested_networks': None,
            'injected_files': [],
            'block_device_mapping': None,
            'image': {
                u'status': u'active',
                u'created_at': u'2016-03-23T16:47:10.000000',
                u'name': u'cirros',
                u'deleted': False,
                u'container_format': u'bare',
                u'min_ram': 0,
                u'disk_format': u'qcow2',
                u'updated_at': u'2016-03-23T16:47:10.000000',
                u'properties': {},
                u'owner': u'1a6edd87f9ec41d8aa64c8f23d719c2a',
                u'checksum': u'ee1eca47dc88f4879d8a229cc70a07c6',
                u'min_disk': 0,
                u'is_public': True,
                u'deleted_at': None,
                u'id': u'5100f480-a40c-46cd-b8b6-ea2e5e7bf09e',
                u'size': 13287936},
            'filter_properties': {
                u'instance_type': {
                    u'nova_object.version': u'1.1',
                    u'nova_object.name': u'Flavor',
                    u'nova_object.namespace': u'nova',
                    u'nova_object.data': {
                        u'memory_mb': 512,
                        u'root_gb': 1,
                        u'deleted_at': None,
                        u'name': u'm1.tiny',
                        u'deleted': False,
                        u'created_at': None,
                        u'ephemeral_gb': 0,
                        u'updated_at': None,
                        u'disabled': False,
                        u'vcpus': 1,
                        u'extra_specs': {},
                        u'swap': 0,
                        u'rxtx_factor': 1.0,
                        u'is_public': True,
                        u'flavorid': u'1',
                        u'vcpu_weight': 0,
                        u'id': 2}},
                u'scheduler_hints': {}},
            'instance': {
                u'nova_object.version': u'2.0',
                u'nova_object.name': u'Instance',
                u'nova_object.namespace': u'nova',
                u'nova_object.data': {
                    u'vm_state': u'building',
                    u'pci_requests': {
                        u'nova_object.version': u'1.1',
                        u'nova_object.name': u'InstancePCIRequests',
                        u'nova_object.namespace': u'nova',
                        u'nova_object.data': {
                            u'instance_uuid': u'e3a7770a-8875e2ccc68b',
                            u'requests': []}},
                    u'availability_zone': None,
                    u'terminated_at': None,
                    u'ephemeral_gb': 0,
                    u'instance_type_id': 2,
                    u'user_data': None,
                    u'numa_topology': None,
                    u'cleaned': False,
                    u'vm_mode': None,
                    u'flavor': {
                        u'nova_object.version': u'1.1',
                        u'nova_object.name': u'Flavor',
                        u'nova_object.namespace': u'nova',
                        u'nova_object.data': {
                            u'memory_mb': 512,
                            u'root_gb': 1,
                            u'deleted_at': None,
                            u'name': u'm1.tiny',
                            u'deleted': False,
                            u'created_at': None,
                            u'ephemeral_gb': 0,
                            u'updated_at': None,
                            u'disabled': False,
                            u'vcpus': 1,
                            u'extra_specs': {},
                            u'swap': 0,
                            u'rxtx_factor': 1.0,
                            u'is_public': True,
                            u'flavorid': u'1',
                            u'vcpu_weight': 0,
                            u'id': 2}},
                    u'deleted_at': None,
                    u'reservation_id': u'r-s9v032d0',
                    u'id': 830,
                    u'security_groups': {
                        u'nova_object.version': u'1.0',
                        u'nova_object.name': u'SecurityGroupList',
                        u'nova_object.namespace': u'nova',
                        u'nova_object.data': {
                            u'objects': []}},
                    u'disable_terminate': False,
                    u'root_device_name': None,
                    u'display_name': u'user_b1',
                    u'uuid': u'e3a7770a-dbf6-4b63-8f9a-8875e2ccc68b',
                    u'default_swap_device': None,
                    u'info_cache': {
                        u'nova_object.version': u'1.5',
                        u'nova_object.name': u'InstanceInfoCache',
                        u'nova_object.namespace': u'nova',
                        u'nova_object.data': {
                            u'instance_uuid': u'e3a7-8875e2ccc68b',
                            u'deleted': False,
                            u'created_at': u'2016-09-02T14:01:39Z',
                            u'updated_at': None,
                            u'network_info': u'[]',
                            u'deleted_at': None}},
                    u'hostname': u'user-b1',
                    u'launched_on': None,
                    u'display_description': u'user_b1',
                    u'key_data': None,
                    u'deleted': False,
                    u'power_state': 0,
                    u'key_name': None,
                    u'default_ephemeral_device': None,
                    u'progress': 0,
                    u'project_id': u'd20ac1ffa60841a78a865da63b2399de',
                    u'launched_at': None,
                    u'metadata': {
                        u'quota': u'dynamic',
                        u'persistent': u'False'},
                    u'node': None,
                    u'ramdisk_id': u'',
                    u'access_ip_v6': None,
                    u'access_ip_v4': None,
                    u'kernel_id': u'',
                    u'old_flavor': None,
                    u'updated_at': None,
                    u'host': None,
                    u'root_gb': 1,
                    u'user_id': u'4cb9f71a47914d0c8b78a471fd8f7015',
                    u'system_metadata': {
                        u'image_min_disk': u'1',
                        u'image_min_ram': u'0',
                        u'image_disk_format': u'qcow2',
                        u'image_base_image_ref': u'5100f480-a25e7bf09e',
                        u'image_container_format': u'bare'},
                    u'task_state': u'scheduling',
                    u'shutdown_terminate': False,
                    u'cell_name': None,
                    u'ephemeral_key_uuid': None,
                    u'locked': False,
                    u'created_at': u'2016-09-02T14:01:39Z',
                    u'locked_by': None,
                    u'launch_index': 0,
                    u'memory_mb': 512,
                    u'vcpus': 1,
                    u'image_ref': u'a40c-46cd-b8b6-ea2e5e7bf09e',
                    u'architecture': None,
                    u'auto_disk_config': False,
                    u'os_type': None,
                    u'config_drive': u'',
                    u'new_flavor': None}},
            'admin_password': u'URijD456Cezi',
            'context': {
                u'domain': None,
                u'project_domain': None,
                u'auth_token': u'f9d8458ef4ae454dad75f4636304079c',
                u'resource_uuid': None,
                u'read_only': False,
                u'user_id': u'4cb9f71a47914d0c8b78a471fd8f7015',
                u'user_identity': u'fa60841a78a865da63b2399de - - -',
                u'tenant': u'd20ac1ffa60841a78a865da63b2399de',
                u'instance_lock_checked': False,
                u'project_id': u'd20ac1ffa60841a78a865da63b2399de',
                u'user_name': u'user_b1',
                u'project_name': u'prj_b',
                u'timestamp': u'2016-09-02T14:01:39.284558',
                u'remote_address': u'10.64.31.19',
                u'quota_class': None,
                u'is_admin': False,
                u'user': u'4cb9f71a47914d0c8b78a471fd8f7015',
                u'service_catalog': [],
                u'read_deleted': u'no',
                u'show_deleted': False,
                u'roles': [u'user'],
                u'request_id': u'req-69c9e7e6-62b2fee1d6e8',
                u'user_domain': None},
            'security_groups': [u'default']}

        self.req = Request.fromDict(req_dict)

    def test_get_AdminPassword(self):
        self.assertEqual(u'URijD456Cezi', self.req.getAdminPassword())

    def test_get_Id(self):
        self.assertEqual(
            u'e3a7770a-dbf6-4b63-8f9a-8875e2ccc68b',
            self.req.getId())

    def test_get_Instance(self):
        ist = {
            u'nova_object.data': {
                u'access_ip_v4': None,
                u'access_ip_v6': None,
                u'architecture': None,
                u'auto_disk_config': False,
                u'availability_zone': None,
                u'cell_name': None,
                u'cleaned': False,
                u'config_drive': u'',
                u'created_at': u'2016-09-02T14:01:39Z',
                u'default_ephemeral_device': None,
                u'default_swap_device': None,
                u'deleted': False,
                u'deleted_at': None,
                u'disable_terminate': False,
                u'display_description': u'user_b1',
                u'display_name': u'user_b1',
                u'ephemeral_gb': 0,
                u'ephemeral_key_uuid': None,
                u'flavor': {
                    u'nova_object.data': {
                        u'created_at': None,
                        u'deleted': False,
                        u'deleted_at': None,
                        u'disabled': False,
                        u'ephemeral_gb': 0,
                        u'extra_specs': {},
                        u'flavorid': u'1',
                        u'id': 2,
                        u'is_public': True,
                        u'memory_mb': 512,
                        u'name': u'm1.tiny',
                        u'root_gb': 1,
                        u'rxtx_factor': 1.0,
                        u'swap': 0,
                        u'updated_at': None,
                        u'vcpu_weight': 0,
                        u'vcpus': 1},
                    u'nova_object.name': u'Flavor',
                    u'nova_object.namespace': u'nova',
                    u'nova_object.version': u'1.1'},
                u'host': None,
                u'hostname': u'user-b1',
                u'id': 830,
                u'image_ref': u'a40c-46cd-b8b6-ea2e5e7bf09e',
                u'info_cache': {
                    u'nova_object.data': {
                        u'created_at': u'2016-09-02T14:01:39Z',
                        u'deleted': False,
                        u'deleted_at': None,
                        u'instance_uuid': u'e3a7-8875e2ccc68b',
                        u'network_info': u'[]',
                        u'updated_at': None},
                    u'nova_object.name': u'InstanceInfoCache',
                    u'nova_object.namespace': u'nova',
                    u'nova_object.version': u'1.5'},
                u'instance_type_id': 2,
                u'kernel_id': u'',
                u'key_data': None,
                u'key_name': None,
                u'launch_index': 0,
                u'launched_at': None,
                u'launched_on': None,
                u'locked': False,
                u'locked_by': None,
                u'memory_mb': 512,
                u'metadata': {
                    u'persistent': u'False',
                    u'quota': u'dynamic'},
                u'new_flavor': None,
                u'node': None,
                u'numa_topology': None,
                u'old_flavor': None,
                u'os_type': None,
                u'pci_requests': {
                    u'nova_object.data': {
                        u'instance_uuid': u'e3a7770a-8875e2ccc68b',
                        u'requests': []},
                    u'nova_object.name': u'InstancePCIRequests',
                    u'nova_object.namespace': u'nova',
                    u'nova_object.version': u'1.1'},
                u'power_state': 0,
                u'progress': 0,
                u'project_id': u'd20ac1ffa60841a78a865da63b2399de',
                u'ramdisk_id': u'',
                u'reservation_id': u'r-s9v032d0',
                u'root_device_name': None,
                u'root_gb': 1,
                u'security_groups': {
                    u'nova_object.data': {
                        u'objects': []},
                    u'nova_object.name': u'SecurityGroupList',
                    u'nova_object.namespace': u'nova',
                    u'nova_object.version': u'1.0'},
                u'shutdown_terminate': False,
                u'system_metadata': {
                    u'image_base_image_ref': u'5100f480-a25e7bf09e',
                    u'image_container_format': u'bare',
                    u'image_disk_format': u'qcow2',
                    u'image_min_disk': u'1',
                    u'image_min_ram': u'0'},
                u'task_state': u'scheduling',
                u'terminated_at': None,
                u'updated_at': None,
                u'user_data': None,
                u'user_id': u'4cb9f71a47914d0c8b78a471fd8f7015',
                u'uuid': u'e3a7770a-dbf6-4b63-8f9a-8875e2ccc68b',
                u'vcpus': 1,
                u'vm_mode': None,
                u'vm_state': u'building'},
            u'nova_object.name': u'Instance',
            u'nova_object.namespace': u'nova',
            u'nova_object.version': u'2.0'}

        self.assertEqual(ist, self.req.getInstance())

    def test_get_Image(self):
        im = {u'checksum': u'ee1eca47dc88f4879d8a229cc70a07c6',
              u'container_format': u'bare',
              u'created_at': u'2016-03-23T16:47:10.000000',
              u'deleted': False,
              u'deleted_at': None,
              u'disk_format': u'qcow2',
              u'id': u'5100f480-a40c-46cd-b8b6-ea2e5e7bf09e',
              u'is_public': True,
              u'min_disk': 0,
              u'min_ram': 0,
              u'name': u'cirros',
              u'owner': u'1a6edd87f9ec41d8aa64c8f23d719c2a',
              u'properties': {},
              u'size': 13287936,
              u'status': u'active',
                         u'updated_at': u'2016-03-23T16:47:10.000000'}

        self.assertEqual(im, self.req.getImage())

    def test_get_Context(self):
        cont = {u'auth_token': u'f9d8458ef4ae454dad75f4636304079c',
                u'domain': None,
                u'instance_lock_checked': False,
                u'is_admin': False,
                u'project_domain': None,
                u'project_id': u'd20ac1ffa60841a78a865da63b2399de',
                u'project_name': u'prj_b',
                u'quota_class': None,
                u'read_deleted': u'no',
                u'read_only': False,
                u'remote_address': u'10.64.31.19',
                u'request_id': u'req-69c9e7e6-62b2fee1d6e8',
                u'resource_uuid': None,
                u'roles': [u'user'],
                u'service_catalog': [],
                u'show_deleted': False,
                u'tenant': u'd20ac1ffa60841a78a865da63b2399de',
                u'timestamp': u'2016-09-02T14:01:39.284558',
                u'user': u'4cb9f71a47914d0c8b78a471fd8f7015',
                u'user_domain': None,
                u'user_id': u'4cb9f71a47914d0c8b78a471fd8f7015',
                u'user_identity': u'fa60841a78a865da63b2399de - - -',
                u'user_name': u'user_b1'}

        self.assertEqual(cont, self.req.getContext())

    def test_get_FilterProperties(self):
        filt = {u'instance_type': {
            u'nova_object.data': {u'created_at': None,
                                  u'deleted': False,
                                  u'deleted_at': None,
                                  u'disabled': False,
                                  u'ephemeral_gb': 0,
                                  u'extra_specs': {},
                                  u'flavorid': u'1',
                                  u'id': 2,
                                  u'is_public': True,
                                  u'memory_mb': 512,
                                  u'name': u'm1.tiny',
                                  u'root_gb': 1,
                                  u'rxtx_factor': 1.0,
                                  u'swap': 0,
                                  u'updated_at': None,
                                  u'vcpu_weight': 0,
                                  u'vcpus': 1},
            u'nova_object.name': u'Flavor',
            u'nova_object.namespace': u'nova',
            u'nova_object.version': u'1.1'},
            u'scheduler_hints': {}}

        self.assertEqual(filt, self.req.getFilterProperties())

    def test_get_InjectedFiles(self):
        self.assertEqual([], self.req.getInjectedFiles())

    def test_get_RequestedNetworks(self):
        self.assertEqual(None, self.req.getRequestedNetworks())

    def test_get_SecurityGroups(self):
        self.assertEqual([u'default'], self.req.getSecurityGroups())

    def test_get_BlockDeviceMapping(self):
        self.assertEqual(None, self.req.getBlockDeviceMapping())

    def test_get_LegacyBDM(self):
        self.assertEqual(True, self.req.getLegacyBDM())

    def test_get_Server(self):
        prjId = self.req.getServer().getProjectId()
        self.assertEqual('d20ac1ffa60841a78a865da63b2399de', prjId)

    def test_from_to_Dict(self):
        rq_dict = self.req.toDict()
        self.assertEqual(True, rq_dict['legacy_bdm'])
