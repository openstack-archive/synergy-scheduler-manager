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

from synergy_scheduler_manager.common.block_device import BlockDeviceMapping
from synergy_scheduler_manager.tests.unit import base


class TestBlockDeviceMapping(base.TestCase):
    def setUp(self):
        super(TestBlockDeviceMapping, self).setUp()
        self.bdp = BlockDeviceMapping('id')

    def test_set_get_Id(self):
        self.assertEqual('id', self.bdp.getId())

    def test_set_get_ImageId(self):
        self.bdp.setImageId('im_id')
        self.assertEqual('im_id', self.bdp.getImageId())

    def test_set_get_InstaceId(self):
        self.bdp.setInstanceId('inst_id')
        self.assertEqual('inst_id', self.bdp.getInstanceId())

    def test_set_get_SnapshotId(self):
        self.bdp.setSnapshotId('snap_id')
        self.assertEqual('snap_id', self.bdp.getSnapshotId())

    def test_set_get_VolumeId(self):
        self.bdp.setVolumeId('vol_id')
        self.assertEqual('vol_id', self.bdp.getVolumeId())

    def test_set_get_VolumeSize(self):
        self.bdp.setVolumeSize('vol_size')
        self.assertEqual('vol_size', self.bdp.getVolumeSize())

    def test_set_get_BootIndex(self):
        self.bdp.setBootIndex('boot_index')
        self.assertEqual('boot_index', self.bdp.getBootIndex())

    def test_set_get_CreatedAt(self):
        self.bdp.setCreatedAt('now')
        self.assertEqual('now', self.bdp.getCreatedAt())

    def test_set_get_UpdatedAt(self):
        self.bdp.setUpdatedAt('now')
        self.assertEqual('now', self.bdp.getUpdatedAt())

    def test_set_get_DeletedAt(self):
        self.bdp.setDeletedAt('now')
        self.assertEqual('now', self.bdp.getDeletedAt())

    def test_set_get_DeviceName(self):
        self.bdp.setDeviceName('name')
        self.assertEqual('name', self.bdp.getDeviceName())

    def test_set_get_NoDevice(self):
        self.bdp.setNoDevice('no_dev')
        self.assertEqual('no_dev', self.bdp.getNoDevice())

    def test_set_get_ConnectionInfo(self):
        self.bdp.setConnectionInfo('con_info')
        self.assertEqual('con_info', self.bdp.getConnectionInfo())

    def test_set_get_DestinationType(self):
        self.bdp.setDestinationType('dest_type')
        self.assertEqual('dest_type', self.bdp.getDestinationType())

    def test_set_get_SourceType(self):
        self.bdp.setSourceType('source_type')
        self.assertEqual('source_type', self.bdp.getSourceType())

    def test_set_get_DiskBus(self):
        self.bdp.setDiskBus('disk_bus')
        self.assertEqual('disk_bus', self.bdp.getDiskBus())

    def test_set_get_GuestFormat(self):
        self.bdp.setGuestFormat('guest_format')
        self.assertEqual('guest_format', self.bdp.getGuestFormat())

    def test_DeleteOnTermination(self):
        self.bdp.setDeleteOnTermination('del_term')
        self.assertEqual('del_term', self.bdp.isDeleteOnTermination())

    def test_Deleted(self):
        self.bdp.setDeleted('deleted')
        self.assertEqual('deleted', self.bdp.isDeleted())

    def test_serialize(self):
        res = self.bdp.serialize()
        expected = {'boot_index': None,
                    'connection_info': None,
                    'created_at': None,
                    'delete_on_termination': None,
                    'deleted': None,
                    'deleted_at': None,
                    'destination_type': None,
                    'device_name': None,
                    'device_type': None,
                    'disk_bus': None,
                    'guest_format': None,
                    'id': 'id',
                    'image_id': None,
                    'instance_uuid': None,
                    'no_device': None,
                    'snapshot_id': None,
                    'source_type': None,
                    'updated_at': None,
                    'volume_id': None,
                    'volume_size': None
                    }

        self.assertEqual('1.15', res['nova_object.version'])
        self.assertEqual('nova', res['nova_object.namespace'])
        self.assertEqual(["device_name"], res["nova_object.changes"])
        self.assertEqual("BlockDeviceMapping", res["nova_object.name"])
        self.assertEqual(expected, res["nova_object.data"])
