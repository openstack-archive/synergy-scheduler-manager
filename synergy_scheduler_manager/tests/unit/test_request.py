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
from synergy_scheduler_manager.common.request import Request
from synergy_scheduler_manager.tests.unit import base


class TestRequest(base.TestCase):

    def setUp(self):
        super(TestRequest, self).setUp()
        request_specs = [{
            'nova_object.version': '1.8',
            'nova_object.namespace': 'nova',
            'nova_object.name': u'RequestSpec',
            'nova_object.data': {
                'requested_destination': None,
                'instance_uuid': '999',
                'retry': {
                    'num_attempts': 1,
                    'hosts': []},
                'num_instances': 1,
                'pci_requests': {
                    'nova_object.version': '1.1',
                    'nova_object.namespace': 'nova',
                    'nova_object.name': 'InstancePCIRequests',
                    'nova_object.data': {'requests': []},
                    'nova_object.changes': ['requests']},
                'limits': {
                    'nova_object.version': '1.0',
                    'nova_object.namespace': 'nova',
                    'nova_object.name': 'SchedulerLimits',
                    'nova_object.data': {
                        'vcpu': None,
                        'memory_mb': None,
                        'numa_topology': None,
                        'disk_gb': None},
                    'nova_object.changes': [
                        'vcpu', 'memory_mb', 'numa_topology', 'disk_gb']},
                'availability_zone': 'nova',
                'force_nodes': None,
                'image': {
                    'nova_object.version': '1.8',
                    'nova_object.namespace': 'nova',
                    'nova_object.name': 'ImageMeta',
                    'nova_object.data': {
                        'status': 'active',
                        'properties': {
                            'nova_object.version': '1.16',
                            'nova_object.name': 'ImageMetaProps',
                            'nova_object.namespace': 'nova',
                            'nova_object.data': {}},
                        'name': 'cirros',
                        'container_format': 'bare',
                        'created_at': '2017-05-19T12:18:46Z',
                        'disk_format': 'qcow2',
                        'updated_at': '2017-05-19T12:18:47Z',
                        'id': '03d54ef8-f0ac-4ad2-92a0-95835d77d2b5',
                        'owner': u'01ab8de5387547d093aa8ae6b85bd8b1',
                        'checksum': 'f8ab98ff5e73ebab884d80c9dc9c7290',
                        'min_disk': 0,
                        'min_ram': 0,
                        'size': 13267968},
                    'nova_object.changes': [
                        'status', 'name', 'container_format', 'created_at',
                        'disk_format', 'updated_at', 'properties', 'owner',
                        'min_ram', 'checksum', 'min_disk', 'id', 'size']},
                'instance_group': None,
                'force_hosts': None,
                'numa_topology': None,
                'scheduler_hints': {},
                'flavor': {
                    'nova_object.version': '1.1',
                    'nova_object.name': 'Flavor',
                    'nova_object.namespace': 'nova',
                    'nova_object.data': {
                        'memory_mb': 512,
                        'root_gb': 1,
                        'deleted_at': None,
                        'name': 'm1.tiny',
                        'deleted': False,
                        'created_at': '2017-05-23T09:36:21Z',
                        'ephemeral_gb': 0,
                        'updated_at': None,
                        'disabled': False,
                        'vcpus': 1,
                        'extra_specs': {},
                        'swap': 0,
                        'rxtx_factor': 1.0,
                        'is_public': True,
                        'flavorid': '5cdecdda',
                        'vcpu_weight': 0,
                        'id': 1}},
                'project_id': u'01ab8de5387547d093aa8ae6b85bd8b1',
                'id': 126,
                'security_groups': {
                    'nova_object.version': '1.0',
                    'nova_object.namespace': 'nova',
                    'nova_object.name': 'SecurityGroupList',
                    'nova_object.data': {
                        'objects': [{
                            'nova_object.version': '1.2',
                            'nova_object.namespace': 'nova',
                            'nova_object.name': 'SecurityGroup',
                            'nova_object.data': {'uuid': 'f5b58bc9'},
                            'nova_object.changes': ['uuid']}]},
                    'nova_object.changes': ['objects']},
                'ignore_hosts': None},
            'nova_object.changes': ['limits', 'image', 'pci_requests']}]

        build_requests = [{
            'nova_object.version': '1.2',
            'nova_object.namespace': 'nova',
            'nova_object.name': 'BuildRequest',
            'nova_object.data': {
                'instance_uuid': '999',
                'created_at': '2017-07-20T12:09:27Z',
                'updated_at': None,
                'instance': {
                    'nova_object.version': '2.3',
                    'nova_object.name': 'Instance',
                    'nova_object.namespace': 'nova',
                    'nova_object.data': {
                        'vm_state': 'building',
                        'keypairs': {
                            'nova_object.version': '1.3',
                            'nova_object.name': 'KeyPairList',
                            'nova_object.namespace': 'nova',
                            'nova_object.data': {'objects': []}},
                        'pci_requests': {
                            'nova_object.version': '1.1',
                            'nova_object.name': 'InstancePCIRequests',
                            'nova_object.namespace': 'nova',
                            'nova_object.data': {'requests': []}},
                        'availability_zone': 'nova',
                        'terminated_at': None,
                        'ephemeral_gb': 0,
                        'old_flavor': None,
                        'updated_at': None,
                        'numa_topology': None,
                        'vm_mode': None,
                        'flavor': {
                            'nova_object.version': '1.1',
                            'nova_object.name': 'Flavor',
                            'nova_object.namespace': 'nova',
                            'nova_object.data': {
                                'memory_mb': 512,
                                'root_gb': 1,
                                'deleted_at': None,
                                'name': 'm1.tiny',
                                'deleted': False,
                                'created_at': '2017-05-23T09:36:21Z',
                                'ephemeral_gb': 0,
                                'updated_at': None,
                                'disabled': False,
                                'vcpus': 1,
                                'extra_specs': {},
                                'swap': 0,
                                'rxtx_factor': 1.0,
                                'is_public': True,
                                'flavorid': '5cdecdda',
                                'vcpu_weight': 0,
                                'id': 1}},
                        'reservation_id': 'r-uavrcsgg',
                        'security_groups': {
                            'nova_object.version': '1.0',
                            'nova_object.name': 'SecurityGroupList',
                            'nova_object.namespace': 'nova',
                            'nova_object.data': {'objects': []}},
                        'disable_terminate': False,
                        'user_id': '4469ff06d1e',
                        'uuid': '999',
                        'new_flavor': None,
                        'info_cache': {
                            'nova_object.version': '1.5',
                            'nova_object.name': 'InstanceInfoCache',
                            'nova_object.namespace': 'nova',
                            'nova_object.data': {
                                'instance_uuid': '999',
                                'network_info': '[]'}},
                        'hostname': 'lisa',
                        'launched_on': None,
                        'display_description': 'Lisa',
                        'key_data': None,
                        'deleted': False,
                        'power_state': 0,
                        'progress': 0,
                        'project_id': '1111111',
                        'launched_at': None,
                        'config_drive': '',
                        'node': None,
                        'ramdisk_id': '',
                        'access_ip_v6': None,
                        'access_ip_v4': None,
                        'kernel_id': '',
                        'key_name': 'mykey',
                        'user_data': None,
                        'host': None,
                        'ephemeral_key_uuid': None,
                        'architecture': None,
                        'display_name': 'Lisa',
                        'system_metadata': {
                            'image_min_disk': '1',
                            'image_min_ram': '0',
                            'image_disk_format': 'qcow2',
                            'image_base_image_ref': '03d54ef8',
                            'image_container_format': 'bare'},
                        'task_state': 'scheduling',
                        'shutdown_terminate': False,
                        'tags': {
                            'nova_object.version': '1.1',
                            'nova_object.name': 'TagList',
                            'nova_object.namespace': 'nova',
                            'nova_object.data': {u'objects': []}},
                        'cell_name': None,
                        'root_gb': 1,
                        'locked': False,
                        'instance_type_id': 1,
                        'locked_by': None,
                        'launch_index': 0,
                        'memory_mb': 512,
                        'vcpus': 1,
                        'image_ref': '03d54ef8',
                        'root_device_name': None,
                        'auto_disk_config': True,
                        'os_type': None,
                        'metadata': {'quota': 'shared'},
                        'created_at': '2017-07-20T12:09:27Z'}},
                'project_id': '1111111',
                'id': 63},
            'nova_object.changes': [u'block_device_mappings']}]

        data = {"build_requests": build_requests,
                "request_specs": request_specs}

        req = {"context": {},
               "data": data,
               "action": "schedule_and_build_instances"}

        self.req = Request.fromDict(req)

    def test_request(self):
        self.assertIsNotNone(self.req)

    def test_getId(self):
        self.assertEqual('999', self.req.getId())

    def test_getUserId(self):
        self.assertEqual('4469ff06d1e', self.req.getUserId())

    def test_getProjectId(self):
        self.assertEqual('1111111', self.req.getProjectId())

    def test_getAction(self):
        self.assertEqual('schedule_and_build_instances', self.req.getAction())

    def test_getContext(self):
        self.assertEqual({}, self.req.getContext())

    def test_getRetry(self):
        self.assertEqual({'num_attempts': 1, 'hosts': []}, self.req.getRetry())

    def test_getServer(self):
        server = self.req.getServer()
        self.assertIsNotNone(server)
        self.assertEqual('999', server.getId())
        self.assertEqual(datetime(2017, 7, 20, 12, 9, 27),
                         server.getCreatedAt())
        self.assertEqual('4469ff06d1e', server.getUserId())
        self.assertEqual('1111111', server.getProjectId())
        self.assertEqual('mykey', server.getKeyName())
        self.assertEqual({'quota': 'shared'}, server.getMetadata())
        self.assertFalse(server.isPermanent())

        flavor = server.getFlavor()
        self.assertEqual('5cdecdda', flavor.getId())
        self.assertEqual('m1.tiny', flavor.getName())
        self.assertEqual(512, flavor.getMemory())
        self.assertEqual(1, flavor.getVCPUs())
        self.assertEqual(1, flavor.getStorage())

    def test_toDict(self):
        rq_dict = self.req.toDict()
        self.assertIn("action", rq_dict)
        self.assertIn("data", rq_dict)
        self.assertIn("context", rq_dict)
