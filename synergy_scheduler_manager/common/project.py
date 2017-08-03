from quota import Quota
from share import Share
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


class Project(SynergyObject):

    def __init__(self):
        super(Project, self).__init__()

        self.set("data", {})
        self.set("users", {})
        self.set("share", Share())
        self.set("quota", Quota())
        self.set("TTL", float(0))
        self.set("enabled", False)

    def getData(self):
        return self.get("data")

    def getQuota(self):
        return self.get("quota")

    def setQueue(self, queue):
        self.set("queue", queue)

    def getQueue(self):
        return self.get("queue")

    def getShare(self):
        return self.get("share")

    def getTTL(self):
        return self.get("TTL")

    def setTTL(self, TTL):
        self.set("TTL", TTL)

    def addUser(self, user):
        if self.get("users").get(user.getId(), None):
            raise Exception("user %r already exists!" % (user.getId()))

        self.get("users")[user.getId()] = user

    def getUser(self, id=None, name=None):
        if id:
            return self.get("users").get(id, None)
        elif name:
            for user in self.get("users").values():
                if name == user.getName():
                    return user
        return None

    def getUsers(self):
        return self.get("users").values()

    def isEnabled(self):
        return self.get("enabled")

    def removeUser(self, user_id):
        return self.get("users").pop(user_id, None)

    def setEnabled(self, enabled=True):
        self.set("enabled", enabled)

    def serialize(self):
        queue = self.getQueue()
        usage = queue.getUsage(self.getId())

        self.getData()["queue_usage"] = usage
        self.getData()["queue_size"] = queue.getSize()

        return super(Project, self).serialize()
