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


class Endpoint(SynergyObject):

    def __init__(self):
        super(Endpoint, self).__init__()

        self.set("enabled", False)

    def getInterface(self):
        return self.get("interface")

    def setInterface(self, interface):
        self.set("interface", interface)

    def getRegion(self):
        return self.get("region")

    def setRegion(self, region):
        self.set("region", region)

    def getRegionId(self):
        return self.get("region_id")

    def setRegionId(self, region_id):
        self.set("region_id", region_id)

    def getServiceId(self):
        return self.get("service_id")

    def setServiceId(self, service_id):
        self.set("service_id", service_id)

    def getURL(self):
        return self.get("url")

    def setURL(self, url):
        self.set("url", url)

    def isEnabled(self):
        return self.get("enabled")

    def setEnabled(self, enabled=True):
        self.set("enabled", enabled)
