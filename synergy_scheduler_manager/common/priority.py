import threading

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


class Priority(SynergyObject):

    def __init__(self):
        super(Priority, self).__init__()

        self.condition = threading.Condition()

        self.set("value", 0)
        self.set("last_update", datetime.utcnow())
        self.set("fairshare", {"vcpus": float(0),
                               "memory": float(0),
                               "disk": float(0)})

    def getValue(self):
        return self.get("value")

    def setValue(self, value):
        self.set("value", value)
        self.set("last_update", datetime.utcnow())

    def getLastUpdate(self):
        return self.get("last_update")

    def getFairShare(self, resource):
        fairshare = self.get("fairshare")

        if resource not in fairshare:
            raise Exception("wrong resource %r" % resource)

        return fairshare[resource]

    def setFairShare(self, resource, value=0):
        fairshare = self.get("fairshare")

        if resource not in fairshare:
            raise Exception("wrong resource %r" % resource)

        with self.condition:
            fairshare[resource] = value
            self.condition.notifyAll()
