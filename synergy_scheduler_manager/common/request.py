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
        self.id = None
        self.user_id = None
        self.project_id = None
        self.action = None
        self.data = None
        self.context = None
        self.server = None
        self.retry = None
        self.created_at = None

    def getAction(self):
        return self.action

    def getContext(self):
        return self.context

    def getCreatedAt(self):
        return self.created_at

    def getData(self):
        return self.data

    def getId(self):
        return self.id

    def getServer(self):
        return self.server

    def getUserId(self):
        return self.user_id

    def getProjectId(self):
        return self.project_id

    def getRetry(self):
        return self.retry

    def toDict(self):
        request = {}
        request['action'] = self.action
        request['context'] = self.context
        request['data'] = self.data

        return request

    @classmethod
    def fromDict(cls, request_dict):
        request = Request()
        request.data = request_dict["data"]
        request.action = request_dict["action"]
        request.context = request_dict["context"]

        if "instances" in request.data:
            instance = request.data["instances"][0]
        else:
            build_request = request.data["build_requests"][0]
            instance = build_request["nova_object.data"]["instance"]

        instance_data = instance["nova_object.data"]

        request.id = instance_data["uuid"]
        request.user_id = instance_data["user_id"]
        request.project_id = instance_data["project_id"]

        created_at = instance_data["created_at"]
        request.created_at = datetime.strptime(created_at,
                                               "%Y-%m-%dT%H:%M:%SZ")

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
        server.setUserData(instance_data["user_data"])
        server.setKeyName(instance_data["key_name"])
        server.setType()

        request.server = server

        if "filter_properties" in request.data:
            filter_properties = request.data["filter_properties"]
            request.retry = filter_properties.get("retry", {})
        else:
            request_spec = request.data["request_specs"][0]
            nova_object = request_spec["nova_object.data"]
            request.retry = nova_object.get("retry", {})

        if not request.retry:
            request.retry = {}

        return request
