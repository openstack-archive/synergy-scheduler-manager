import utils

from datetime import datetime
from synergy.common.serializer import SynergyObject


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


class Server(SynergyObject):

    def __init__(self):
        super(Server, self).__init__()
        self.setType("permanent")

    def __getDateTime(self, date):
        if not date:
            return None
        elif isinstance(date, basestring):
            try:
                return datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                return datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%f")
        elif isinstance(date, datetime):
            return date
        else:
            raise Exception("not valid date format")

    def getType(self):
        return self.get("type")

    def setType(self, type):
        self.set("type", type)

    def getState(self):
        return self.get("state")

    def setState(self, state):
        self.set("state", state)

    def getHost(self):
        return self.get("host")

    def setHost(self, host):
        self.set("host", host)

    def getFlavor(self):
        return self.get("flavor")

    def setFlavor(self, flavor):
        self.set("flavor", flavor)

    def getKeyName(self):
        return self.get("key_name")

    def setKeyName(self, key_name):
        self.set("key_name", key_name)

    def getMetadata(self):
        return self.get("metadata")

    def setMetadata(self, metadata):
        self.set("metadata", metadata)

        if "quota" in metadata:
            if metadata["quota"] == "shared":
                self.setType("ephemeral")
            else:
                self.setType("permanent")

    def getUserData(self):
        return self.get("userdata")

    def setUserData(self, userdata):
        self.set("userdata", userdata)

        if userdata:
            try:
                quota = utils.getConfigParameter(userdata, "quota", "synergy")

                if quota is None or quota == "private":
                    self.setType("permanent")
                elif quota == "shared":
                    self.setType("ephemeral")
                else:
                    self.setType("permanent")
            except Exception:
                self.setType("permanent")

    def getUserId(self):
        return self.get("user_id")

    def setUserId(self, user_id):
        self.set("user_id", user_id)

    def getProjectId(self):
        return self.get("project_id")

    def setProjectId(self, project_id):
        self.set("project_id", project_id)

    def getCreatedAt(self):
        return self.get("created_at")

    def setCreatedAt(self, created_at):
        self.set("created_at", self.__getDateTime(created_at))

    def getLaunchedAt(self):
        return self.get("launched_at")

    def setLaunchedAt(self, launched_at):
        self.set("launched_at", self.__getDateTime(launched_at))

    def getUpdatedAt(self):
        return self.get("updated_at")

    def setUpdatedAt(self, updated_at):
        self.set("updated_at", self.__getDateTime(updated_at))

    def getTerminatedAt(self):
        return self.get("terminated_at")

    def setTerminatedAt(self, terminated_at):
        self.set("terminated_at", self.__getDateTime(terminated_at))

    def getDeletedAt(self):
        return self.get("deleted_at")

    def setDeletedAt(self, deleted_at):
        self.set("deleted_at", self.__getDateTime(deleted_at))

    def isEphemeral(self):
        return self.get("type") == "ephemeral"

    def isPermanent(self):
        return self.get("type") == "permanent"
