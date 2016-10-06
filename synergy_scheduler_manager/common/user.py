from priority import Priority
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


class User(SynergyObject):

    def __init__(self):
        super(User, self).__init__()

        self.set("data", {})
        self.set("priority", Priority())
        self.set("share", Share())
        self.set("role", None)
        self.set("enabled", False)

    def getData(self):
        return self.get("data")

    def getProjectId(self):
        return self.get("project_id")

    def setProjectId(self, project_id):
        self.set("project_id", project_id)

    def getPriority(self):
        return self.get("priority")

    def getRole(self):
        return self.get("role")

    def setRole(self, role):
        self.set("role", role)

    def getShare(self):
        return self.get("share")

    def isEnabled(self):
        return self.get("enabled")

    def setEnabled(self, enabled=True):
        self.set("enabled", enabled)
