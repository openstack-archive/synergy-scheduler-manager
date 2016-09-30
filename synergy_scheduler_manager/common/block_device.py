__author__ = "Lisa Zangrando"
__email__ = "lisa.zangrando[AT]pd.infn.it"
__copyright__ = """Copyright (c) 2015 INFN - INDIGO-DataCloud
All Rights Reserved

Licensed under the Apache License, Version 2.0;
you may not use this file except in compliance with the
License. You may obtain a copy of the License at:

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied.
See the License for the specific language governing
permissions and limitations under the License."""

"""
from nova.objects.base import NovaObject
from nova.objects.base import NovaObjectDictCompat

class BlockDeviceMapping(NovaObject, NovaObjectDictCompat):
    fields = {
    }


    def __init__(self, *args, **kwargs):
        super(BlockDeviceMapping, self).__init__(*args, **kwargs)
"""


class BlockDeviceMapping(object):

    def __init__(self, id):
        self.id = id
        self.created_at = None
        self.updated_at = None
        self.deleted_at = None
        self.device_name = None
        self.delete_on_termination = None
        self.snapshot_id = None
        self.volume_id = None
        self.volume_size = None
        self.no_device = None
        self.connection_info = None
        self.instance_uuid = None
        self.deleted = None
        self.source_type = None
        self.destination_type = None
        self.guest_format = None
        self.device_type = None
        self.disk_bus = None
        self.boot_index = None
        self.image_id = None

    def getId(self):
        return self.id

    def getImageId(self):
        return self.image_id

    def setImageId(self, image_id):
        self.image_id = image_id

    def getInstanceId(self):
        return self.instance_uuid

    def setInstanceId(self, instance_uuid):
        self.instance_uuid = instance_uuid

    def getSnapshotId(self):
        return self.snapshot_id

    def setSnapshotId(self, snapshot_id):
        self.snapshot_id = snapshot_id

    def getVolumeId(self):
        return self.volume_id

    def setVolumeId(self, volume_id):
        self.volume_id = volume_id

    def getVolumeSize(self):
        return self.volume_size

    def setVolumeSize(self, volume_size):
        self.volume_size = volume_size

    def getBootIndex(self):
        return self.boot_index

    def setBootIndex(self, boot_index):
        self.boot_index = boot_index

    def getCreatedAt(self):
        return self.created_at

    def setCreatedAt(self, created_at):
        self.created_at = created_at

    def getUpdatedAt(self):
        return self.updated_at

    def setUpdatedAt(self, updated_at):
        self.updated_at = updated_at

    def getDeletedAt(self):
        return self.deleted_at

    def setDeletedAt(self, deleted_at):
        self.deleted_at = deleted_at

    def getDeviceName(self):
        return self.device_name

    def setDeviceName(self, device_name):
        self.device_name = device_name

    def getNoDevice(self):
        return self.no_device

    def setNoDevice(self, no_device):
        self.no_device = no_device

    def getConnectionInfo(self):
        return self.connection_info

    def setConnectionInfo(self, connection_info):
        self.connection_info = connection_info

    def getDestinationType(self):
        return self.destination_type

    def setDestinationType(self, destination_type):
        self.destination_type = destination_type

    def getDeviceType(self):
        return self.device_type

    def setDeviceType(self, device_type):
        self.device_type = device_type

    def getSourceType(self):
        return self.source_type

    def setSourceType(self, source_type):
        self.source_type = source_type

    def getDiskBus(self):
        return self.disk_bus

    def setDiskBus(self, disk_bus):
        self.disk_bus = disk_bus

    def getGuestFormat(self):
        return self.guest_format

    def setGuestFormat(self, guest_format):
        self.guest_format = guest_format

    def isDeleteOnTermination(self):
        return self.delete_on_termination

    def setDeleteOnTermination(self, delete_on_termination):
        self.delete_on_termination = delete_on_termination

    def isDeleted(self):
        return self.deleted

    def setDeleted(self, deleted):
        self.deleted = deleted

    def serialize(self):
        data = {}
        data["id"] = self.id
        data["created_at"] = self.created_at
        data["updated_at"] = self.updated_at
        data["deleted_at"] = self.deleted_at
        data["device_name"] = self.device_name
        data["device_type"] = self.device_type
        data["delete_on_termination"] = self.delete_on_termination
        data["snapshot_id"] = self.snapshot_id
        data["volume_id"] = self.volume_id
        data["volume_size"] = self.volume_size
        data["no_device"] = self.no_device
        data["connection_info"] = self.connection_info
        data["instance_uuid"] = self.instance_uuid
        data["deleted"] = self.deleted
        data["source_type"] = self.source_type
        data["destination_type"] = self.destination_type
        data["guest_format"] = self.guest_format
        data["disk_bus"] = self.disk_bus
        data["boot_index"] = self.boot_index
        data["image_id"] = self.image_id

        blockDeviceMap = {}
        blockDeviceMap["nova_object.version"] = "1.15"
        blockDeviceMap["nova_object.namespace"] = "nova"
        blockDeviceMap["nova_object.changes"] = ["device_name"]
        blockDeviceMap["nova_object.name"] = "BlockDeviceMapping"
        blockDeviceMap["nova_object.data"] = data

        return blockDeviceMap
