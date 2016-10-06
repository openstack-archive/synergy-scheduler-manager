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


class Service(SynergyObject):

    def __init__(self):
        super(Service, self).__init__()

        self.set("enabled", False)
        self.set("endpoints", [])

    def getType(self):
        return self.get("type")

    def setType(self, type):
        self.set("type", type)

    def getEndpoints(self):
        return self.get("endpoints")

    def getEndpoint(self, interface):
        for endpoint in self.get("endpoints"):
            if endpoint.getInterface() == interface:
                return endpoint

        return None

    def getDescription(self):
        return self.get("description")

    def setDescription(self, description):
        self.set("description", description)

    def isEnabled(self):
        return self.get("enabled")

    def setEnabled(self, enabled=True):
        self.set("enabled", enabled)
