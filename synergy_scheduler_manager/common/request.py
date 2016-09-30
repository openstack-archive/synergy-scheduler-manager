import utils

from datetime import datetime
from flavor import Flavor
from server import Server

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


class Request(object):

    def __init__(self):
        self.context = None
        self.instance = None
        self.image = None
        self.filter_properties = None
        self.admin_password = None
        self.injected_files = None
        self.requested_networks = None
        self.security_groups = None
        self.block_device_mapping = None
        self.legacy_bdm = None

    def getAdminPassword(self):
        return self.admin_password

    def getId(self):
        if self.instance:
            return self.instance["nova_object.data"]["uuid"]

        return None

    def getInstance(self):
        return self.instance

    def getServer(self):
        server = None

        if self.instance:
            instance_data = self.instance["nova_object.data"]
            flavor_data = instance_data["flavor"]["nova_object.data"]

            flavor = Flavor()
            flavor.setId(flavor_data["flavorid"])
            flavor.setName(flavor_data["name"])
            flavor.setMemory(flavor_data["memory_mb"])
            flavor.setVCPUs(flavor_data["vcpus"])
            flavor.setStorage(flavor_data["root_gb"])

            server = Server()
            server.setFlavor(flavor)
            server.setId(instance_data["uuid"])
            server.setUserId(instance_data["user_id"])
            server.setProjectId(instance_data["project_id"])
            server.setCreatedAt(instance_data["created_at"])
            server.setMetadata(instance_data["metadata"])
            server.setKeyName(instance_data["key_name"])

            if "user_data" in instance_data:
                user_data = instance_data["user_data"]
                if user_data:
                    server.setUserData(utils.decodeBase64(user_data))

        return server

    def getImage(self):
        return self.image

    def getUserId(self):
        if self.instance:
            return self.instance["nova_object.data"]["user_id"]

        return None

    def getProjectId(self):
        if self.instance:
            return self.instance["nova_object.data"]["project_id"]

        return None

    def getContext(self):
        return self.context

    def getCreatedAt(self):
        if self.instance:
            created_at = self.instance["nova_object.data"]["created_at"]
            timestamp = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ")
            return timestamp

        return 0

    def getMetadata(self):
        if self.instance:
            return self.instance["nova_object.data"]["metadata"]

        return None

    def getRetry(self):
        if self.filter_properties:
            return self.filter_properties.get("retry", None)

        return None

    def getFilterProperties(self):
        return self.filter_properties

    def getInjectedFiles(self):
        return self.injected_files

    def getRequestedNetworks(self):
        return self.requested_networks

    def getSecurityGroups(self):
        return self.security_groups

    def getBlockDeviceMapping(self):
        return self.block_device_mapping

    def getLegacyBDM(self):
        return self.legacy_bdm

    def toDict(self):
        request = {}
        request['context'] = self.context
        request['instance'] = self.instance
        request['image'] = self.image
        request['filter_properties'] = self.filter_properties
        request['admin_password'] = self.admin_password
        request['injected_files'] = self.injected_files
        request['requested_networks'] = self.requested_networks
        request['security_groups'] = self.security_groups
        request['block_device_mapping'] = self.block_device_mapping
        request['legacy_bdm'] = self.legacy_bdm

        return request

    @classmethod
    def fromDict(cls, request_dict):
        request = Request()
        request.context = request_dict['context']
        request.instance = request_dict['instance']
        request.image = request_dict['image']
        request.filter_properties = request_dict['filter_properties']
        request.admin_password = request_dict['admin_password']
        request.injected_files = request_dict['injected_files']
        request.requested_networks = request_dict['requested_networks']
        request.security_groups = request_dict['security_groups']
        request.block_device_mapping = request_dict['block_device_mapping']
        request.legacy_bdm = request_dict['legacy_bdm']

        return request

    @classmethod
    def build(cls, context, instance, image, filter_properties,
              admin_password, injected_files, requested_networks,
              security_groups, block_device_mapping=None, legacy_bdm=True):
        request = Request()
        request.context = context
        request.instance = instance
        request.image = image
        request.filter_properties = filter_properties
        request.admin_password = admin_password
        request.injected_files = injected_files
        request.requested_networks = requested_networks
        request.security_groups = security_groups
        request.block_device_mapping = block_device_mapping
        request.legacy_bdm = legacy_bdm

        return request
