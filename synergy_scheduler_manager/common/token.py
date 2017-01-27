import json
import os.path

from datetime import datetime
from endpoint import Endpoint
from project import Project
from role import Role
from service import Service
from synergy.common.serializer import SynergyObject
from synergy_scheduler_manager.common.user import User


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


class Token(SynergyObject):

    def __init__(self):
        super(Token, self).__init__()

        self.set("extras", {})
        self.set("roles", [])
        self.set("catalog", [])

    @classmethod
    def parse(cls, id, data):
        token = Token()

        token.setId(id)

        data = data["token"]
        issued_at = None
        expires_at = None

        try:
            issued_at = datetime.strptime(data["issued_at"],
                                          "%Y-%m-%dT%H:%M:%S.%fZ")
        except Exception:
            issued_at = datetime.strptime(data["issued_at"],
                                          "%Y-%m-%dT%H:%M:%S.%f")

        try:
            expires_at = datetime.strptime(data["expires_at"],
                                           "%Y-%m-%dT%H:%M:%S.%fZ")
        except Exception:
            expires_at = datetime.strptime(data["expires_at"],
                                           "%Y-%m-%dT%H:%M:%S.%f")

        token.setCreation(issued_at)
        token.setExpiration(expires_at)

        project = Project()
        project.setId(data["project"]["id"])
        project.setName(data["project"]["name"])

        token.setProject(project)

        user = User()
        user.setId(data["user"]["id"])
        user.setName(data["user"]["name"])
        user.setProjectId(data["project"]["id"])

        token.setUser(user)

        if "extras" in data:
            token.getExtras().update(data["extras"])

        for info in data["roles"]:
            role = Role()
            role.setId(info["id"])
            role.setName(info["name"])

            token.getRoles().append(role)

        for service_info in data["catalog"]:
            service = Service()
            service.setId(service_info["id"])
            service.setType(service_info["type"])
            service.setName(service_info["name"])

            for endpoint_info in service_info["endpoints"]:
                endpoint = Endpoint()
                endpoint.setId(endpoint_info["id"])
                endpoint.setInterface(endpoint_info["interface"])
                endpoint.setRegion(endpoint_info["region"])
                endpoint.setRegionId(endpoint_info["region_id"])
                endpoint.setURL(endpoint_info["url"])

                service.getEndpoints().append(endpoint)

            token.getServices().append(service)

        return token

    def getServices(self):
        return self.get("catalog")

    def getService(self, name):
        for service in self.get("catalog"):
            if service.getName() == name:
                return service

        return None

    def getCreation(self):
        return self.get("issued_at")

    def setCreation(self, issued_at):
        self.set("issued_at", issued_at)

    def getExpiration(self):
        return self.get("expires_at")

    def setExpiration(self, expires_at):
        self.set("expires_at", expires_at)

    def getExtras(self):
        return self.get("extras")

    def getProject(self):
        return self.get("project")

    def setProject(self, project):
        self.set("project", project)

    def getRoles(self):
        return self.get("roles")

    def getUser(self):
        return self.get("user")

    def setUser(self, user):
        self.set("user", user)

    def isAdmin(self):
        for role in self.get("roles"):
            if role.getName() == "admin":
                return True

        return False

    def issuedAt(self):
        return self.get("issued_at")

    def isExpired(self):
        return self.getExpiration() < datetime.utcnow()

    def save(self, filename):
        # save to file
        with open(filename, 'w') as f:
            json.dump(self.serialize(), f)

    @classmethod
    def load(cls, filename):
        if not os.path.isfile(filename):
            return None

        # load from file:
        with open(filename, 'r') as f:
            try:
                data = json.load(f)
                return Token.deserialize(data)
            # if the file is empty the ValueError will be thrown
            except ValueError as ex:
                raise ex

    def isotime(self, at=None, subsecond=False):
        """Stringify time in ISO 8601 format."""
        if not at:
            at = datetime.utcnow()

        if not subsecond:
            st = at.strftime('%Y-%m-%dT%H:%M:%S')
        else:
            st = at.strftime('%Y-%m-%dT%H:%M:%S.%f')

        if at.tzinfo:
            tz = at.tzinfo.tzname(None)
        else:
            tz = 'UTC'

        st += ('Z' if tz == 'UTC' else tz)
        return st
