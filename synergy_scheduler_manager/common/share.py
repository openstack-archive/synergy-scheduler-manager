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


class Share(SynergyObject):

    def __init__(self):
        super(Share, self).__init__()

        self.set("value", float(0))
        self.set("sibling_value", float(0))
        self.set("normalized_value", float(0))

    def getValue(self):
        return self.get("value")

    def setValue(self, value):
        self.set("value", value)

    def getSiblingValue(self):
        return self.get("sibling_value")

    def setSiblingValue(self, value):
        self.set("sibling_value", value)

    def getNormalizedValue(self):
        return self.get("normalized_value")

    def setNormalizedValue(self, value):
        self.set("normalized_value", value)
