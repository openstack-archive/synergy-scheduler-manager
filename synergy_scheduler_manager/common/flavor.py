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


class Flavor(SynergyObject):

    def __init__(self):
        super(Flavor, self).__init__()

        self.set("vcpus", 0)
        self.set("memory", 0)
        self.set("storage", 0)

    def getVCPUs(self):
        return self.get("vcpus")

    def setVCPUs(self, vcpus):
        self.set("vcpus", vcpus)

    def getMemory(self):
        return self.get("memory")

    def setMemory(self, memory):
        self.set("memory", memory)

    def getStorage(self):
        return self.get("storage")

    def setStorage(self, storage):
        self.set("storage", storage)
