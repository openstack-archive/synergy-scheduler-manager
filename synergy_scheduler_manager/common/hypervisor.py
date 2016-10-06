from service import Service


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


class Hypervisor(Service):

    def __init__(self):
        super(Hypervisor, self).__init__()

        self.set("vcpus", 0)
        self.set("vcpus_used", 0)
        self.set("memory", 0)
        self.set("memory_used", 0)
        self.set("storage", 0)
        self.set("storage_used", 0)
        self.set("vms", 0)
        self.set("workload", 0)

    def getIP(self):
        return self.get("ip")

    def setIP(self, ip):
        self.set("ip", ip)

    def getState(self):
        return self.get("state")

    def setState(self, state):
        self.set("state", state)

    def getStatus(self):
        return self.get("status")

    def setStatus(self, status):
        self.set("status", status)

    def getWorkload(self):
        return self.get("workload")

    def setWorkload(self, workload):
        self.set("workload", workload)

    def getVMs(self):
        return self.get("vms")

    def setVMs(self, vms):
        self.set("vms", vms)

    def getVCPUs(self, used=False):
        if used:
            return self.get("vcpus_used")
        else:
            return self.get("vcpus")

    def setVCPUs(self, vcpus, used=False):
        if used:
            self.set("vcpus_used", vcpus)
        else:
            self.set("vcpus", vcpus)

    def getMemory(self, used=False):
        if used:
            return self.get("memory_used")
        else:
            return self.get("memory")

    def setMemory(self, memory, used=False):
        if used:
            self.set("memory_used", memory)
        else:
            self.set("memory", memory)

    def getStorage(self, used=False):
        if used:
            return self.get("storage_used")
        else:
            return self.get("storage")

    def setStorage(self, storage, used=False):
        if used:
            self.set("storage_used", storage)
        else:
            self.set("storage", storage)
