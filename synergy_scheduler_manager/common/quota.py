import logging
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


LOG = logging.getLogger("quota")


class Quota(SynergyObject):

    def __init__(self):
        super(Quota, self).__init__()

        private = {}
        private["servers"] = {"active": [], "building": [], "error": []}
        private["resources"] = {
            "memory": {"used": float(0), "size": float(0)},
            "vcpus": {"used": float(0), "size": float(0)},
            "instances": {"used": float(0), "size": float(0)}}

        shared = {}
        shared["servers"] = {"active": [], "building": [], "error": []}
        shared["resources"] = {
            "memory": {"used": float(0), "size": float(0)},
            "vcpus": {"used": float(0), "size": float(0)},
            "instances": {"used": float(0), "size": float(0)}}

        self.set("shared", shared)
        self.set("private", private)
        self.condition = threading.Condition()

    def getType(self):
        return self.get("type")

    def setType(self, type):
        self.set("type", type)

    def getServers(self, status, private=True):
        if private:
            servers = self.get("private")["servers"]
        else:
            servers = self.get("shared")["servers"]

        if status not in servers:
            raise Exception("wrong status %r" % status)

        return servers[status]

    def reset(self):
        with self.condition:
            for resource in self.get("private")["resources"].values():
                resource["used"] = 0

            self.condition.notifyAll()

    def getSize(self, resource, private=True):
        if private:
            resources = self.get("private")["resources"]
        else:
            resources = self.get("shared")["resources"]

        if resource not in resources:
            raise Exception("wrong resource %r" % resource)

        return resources[resource]["size"]

    def setSize(self, resource, value=0, private=True):
        if private:
            resources = self.get("private")["resources"]
        else:
            resources = self.get("shared")["resources"]

        if resource not in resources:
            raise Exception("wrong resource %r" % resource)

        with self.condition:
            resources[resource]["size"] = value
            self.condition.notifyAll()

    def getUsage(self, resource, private=True):
        if private:
            resources = self.get("private")["resources"]
        else:
            resources = self.get("shared")["resources"]

        if resource not in resources:
            raise Exception("wrong resource %r" % resource)

        return resources[resource]["used"]

    def setUsage(self, resource, value=0, private=True):
        if private:
            resources = self.get("private")["resources"]
        else:
            resources = self.get("shared")["resources"]

        if resource not in resources:
            raise Exception("wrong resource %r" % resource)

        with self.condition:
            resources[resource]["used"] = value
            self.condition.notifyAll()

    def allocate(self, server, blocking=True):
        server_id = server.getId()
        state = server.getState()
        flavor = server.getFlavor()
        vcpus = flavor.getVCPUs()
        memory = flavor.getMemory()
        found = False

        if server.isEphemeral():
            if SharedQuota.allocate(server, blocking):
                shared = self.get("shared")
                servers = shared["servers"]
                resources = shared["resources"]

                with self.condition:
                    resources["vcpus"]["used"] += vcpus
                    resources["memory"]["used"] += memory
                    resources["instances"]["used"] += 1

                    servers["active"].append(server_id)

                    self.condition.notifyAll()
                return True
            else:
                return False

        private = self.get("private")
        servers = private["servers"]
        resources = private["resources"]

        with self.condition:
            if vcpus > resources["vcpus"]["size"] or \
                    memory > resources["memory"]["size"]:
                self.condition.notifyAll()
                raise Exception("the required resources for server %r "
                                "exceed the quota size" % server_id)

            if server_id in servers["active"]:
                self.condition.notifyAll()
                raise Exception("resources for server %r already allocated"
                                % server_id)
            elif server_id in servers["building"]:
                self.condition.notifyAll()
                raise Exception("resources for server %r waiting "
                                "to be allocated" % server_id)
            elif state:
                resources["vcpus"]["used"] += vcpus
                resources["memory"]["used"] += memory
                resources["instances"]["used"] += 1

                servers[state].append(server_id)

                found = True
            elif not blocking:
                servers["building"].append(server_id)

            while (not found and server_id in servers["building"]):
                vcpus_size = resources["vcpus"]["size"]
                vcpus_used = resources["vcpus"]["used"]
                memory_size = resources["memory"]["size"]
                memory_used = resources["memory"]["used"]

                LOG.debug("allocating server_id=%s vcpus=%s "
                          "memory=%s [vcpus in use %s of %s; "
                          "memory in use %s of %s]"
                          % (server_id,
                             vcpus,
                             memory,
                             vcpus_used,
                             vcpus_size,
                             memory_used,
                             memory_size))

                if (vcpus_size - vcpus_used >= vcpus) and \
                   (memory_size - memory_used >= memory):
                    found = True

                    resources["vcpus"]["used"] += vcpus
                    resources["memory"]["used"] += memory
                    resources["instances"]["used"] += 1

                    servers["active"].append(server_id)
                    servers["building"].remove(server_id)

                    LOG.info("allocated server_id=%s vcpus=%s memory"
                             "=%s [vcpus in use %s of %s; "
                             "memory in use %s of %s]"
                             % (server_id,
                                vcpus,
                                memory,
                                resources["vcpus"]["used"],
                                resources["vcpus"]["size"],
                                resources["memory"]["used"],
                                resources["memory"]["size"]))
                elif blocking:
                    LOG.info("allocate wait!!!")
                    self.condition.wait()
                else:
                    break

            self.condition.notifyAll()

        if not found:
            servers["building"].remove(server_id)

        return found

    def release(self, server):
        server_id = server.getId()
        flavor = server.getFlavor()
        vcpus = flavor.getVCPUs()
        memory = flavor.getMemory()

        if SharedQuota.release(server):
            shared = self.get("shared")
            servers = shared["servers"]
            resources = shared["resources"]

            with self.condition:
                if server_id in servers["building"]:
                    servers["building"].remove(server_id)
                elif server_id in servers["active"]:
                    resources["vcpus"]["used"] -= vcpus
                    resources["memory"]["used"] -= memory
                    resources["instances"]["used"] -= 1

                    if server_id in servers["active"]:
                        servers["active"].remove(server_id)
                    else:
                        servers["error"].remove(server_id)

                self.condition.notifyAll()
            return True

        private = self.get("private")
        servers = private["servers"]
        resources = private["resources"]
        found = False
        with self.condition:
            LOG.debug("releasing server_id=%s vcpus=%s memory=%s "
                      "[vcpu in use %s of %s; memory in use %s of %s]"
                      % (server_id,
                         vcpus,
                         memory,
                         resources["vcpus"]["used"],
                         resources["vcpus"]["size"],
                         resources["memory"]["used"],
                         resources["memory"]["size"]))

            if server_id in servers["building"]:
                servers["building"].remove(server_id)
                found = True
            elif server_id in servers["active"]:
                if resources["vcpus"]["used"] - vcpus < 0:
                    resources["vcpus"]["used"] = 0
                else:
                    resources["vcpus"]["used"] -= vcpus

                if resources["memory"]["used"] - memory < 0:
                    resources["memory"]["used"] = 0
                else:
                    resources["memory"]["used"] -= memory

                resources["instances"]["used"] -= 1

                if server_id in servers["active"]:
                    servers["active"].remove(server_id)
                else:
                    servers["error"].remove(server_id)

                LOG.info("released server_id=%s vcpus=%s memory=%s "
                         "[vcpu in use %s of %s; memory in use %s of %s]"
                         % (server_id,
                            vcpus,
                            memory,
                            resources["vcpus"]["used"],
                            resources["vcpus"]["size"],
                            resources["memory"]["used"],
                            resources["memory"]["size"]))
                found = True
            else:
                LOG.debug("release: instance %r not found!" % (server_id))

            self.condition.notifyAll()
        return found


class SharedQuota(SynergyObject):
    resources = {}
    resources["memory"] = {"used": float(0), "size": float(0)}
    resources["vcpus"] = {"used": float(0), "size": float(0)}
    resources["instances"] = {"used": float(0), "size": float(-1)}
    servers = {"active": [], "building": [], "error": []}
    condition = threading.Condition()
    lastAllocationTime = datetime.now()
    lastReleaseTime = datetime.now()
    enabled = False
    total = 0

    def __init__(self):
        super(SharedQuota, self).__init__()

        self.set("servers", SharedQuota.servers)
        self.set("resources", SharedQuota.resources)
        self.set("enabled", SharedQuota.enabled)
        self.set("lastAllocationTime", SharedQuota.lastAllocationTime)
        self.set("lastReleaseTime", SharedQuota.lastReleaseTime)
        self.set("total", SharedQuota.total)

    @classmethod
    def isEnabled(cls):
        return cls.enabled

    @classmethod
    def enable(cls):
        with cls.condition:
            cls.enabled = True
            cls.condition.notifyAll()

    @classmethod
    def disable(cls):
        with cls.condition:
            cls.enabled = False
            cls.condition.notifyAll()

    @classmethod
    def getSize(cls, resource):
        if resource not in cls.resources:
            raise Exception("wrong resource %r" % resource)

        return cls.resources[resource]["size"]

    @classmethod
    def setSize(cls, resource, value):
        if resource not in cls.resources:
            raise Exception("wrong resource %r" % resource)

        with cls.condition:
            cls.resources[resource]["size"] = value
            cls.condition.notifyAll()

    @classmethod
    def getUsage(cls, resource):
        if resource not in cls.resources:
            raise Exception("wrong resource %r" % resource)

        return cls.resources[resource]["used"]

    @classmethod
    def setUsage(cls, resource, value=0):
        if resource not in cls.resources:
            raise Exception("wrong resource %r" % resource)

        with cls.condition:
            cls.resources[resource]["used"] = value
            cls.condition.notifyAll()

    @classmethod
    def getLastAllocationTime(cls):
        return cls.lastAllocationTime

    @classmethod
    def getLastReleaseTime(cls):
        return cls.lastReleaseTime

    @classmethod
    def wait(cls):
        with cls.condition:
            cls.condition.wait()

    @classmethod
    def allocate(cls, server, blocking=True):
        server_id = server.getId()
        state = server.getState()
        flavor = server.getFlavor()
        vcpus = flavor.getVCPUs()
        memory = flavor.getMemory()
        found = False

        with cls.condition:
            if not cls.enabled:
                if blocking:
                    cls.condition.wait()
                else:
                    cls.condition.notifyAll()
                    return False

            if vcpus > cls.resources["vcpus"]["size"] or \
                    memory > cls.resources["memory"]["size"]:
                cls.condition.notifyAll()
                raise Exception("the required resources for server %r "
                                "exceed the quota size" % server_id)

            if server_id in cls.servers["active"]:
                cls.condition.notifyAll()
                raise Exception("resources for server %r already allocated"
                                % server_id)
            elif server_id in cls.servers["building"]:
                cls.condition.notifyAll()
                raise Exception("resources for server %r already waiting "
                                "to be allocated" % server_id)
            elif state:
                cls.resources["vcpus"]["used"] += vcpus
                cls.resources["memory"]["used"] += memory
                cls.resources["instances"]["used"] += 1

                cls.servers[state].append(server_id)
                cls.total += 1

                found = True
            else:
                cls.servers["building"].append(server_id)

            while (cls.enabled and not found and
                   server_id in cls.servers["building"]):
                vcpus_size = cls.resources["vcpus"]["size"]
                vcpus_used = cls.resources["vcpus"]["used"]
                memory_size = cls.resources["memory"]["size"]
                memory_used = cls.resources["memory"]["used"]

                LOG.debug("allocating server_id=%s vcpus=%s "
                          "memory=%s [vcpus in use %s of %s; "
                          "memory in use %s of %s]"
                          % (server_id,
                             vcpus,
                             memory,
                             vcpus_used,
                             vcpus_size,
                             memory_used,
                             memory_size))

                if (vcpus_size - vcpus_used >= vcpus) and \
                   (memory_size - memory_used >= memory):
                    found = True

                    cls.resources["vcpus"]["used"] += vcpus
                    cls.resources["memory"]["used"] += memory
                    cls.resources["instances"]["used"] += 1

                    cls.servers["active"].append(server_id)
                    cls.servers["building"].remove(server_id)

                    LOG.info("allocated server_id=%s vcpus=%s memory"
                             "=%s [vcpus in use %s of %s; "
                             "memory in use %s of %s]"
                             % (server_id,
                                vcpus,
                                memory,
                                cls.resources["vcpus"]["used"],
                                cls.resources["vcpus"]["size"],
                                cls.resources["memory"]["used"],
                                cls.resources["memory"]["size"]))

                    cls.lastAllocationTime = datetime.now()
                    cls.total += 1
                elif blocking:
                    LOG.info("allocate wait!!!")
                    cls.condition.wait()
                else:
                    break

            cls.condition.notifyAll()

        if not found:
            cls.servers["building"].remove(server_id)

        return found

    @classmethod
    def release(cls, server):
        server_id = server.getId()
        flavor = server.getFlavor()
        vcpus = flavor.getVCPUs()
        memory = flavor.getMemory()
        found = False

        with cls.condition:
            LOG.debug("releasing server_id=%s vcpus=%s memory=%s "
                      "[vcpu in use %s of %s; memory in use %s of %s]"
                      % (server_id,
                         vcpus,
                         memory,
                         cls.resources["vcpus"]["used"],
                         cls.resources["vcpus"]["size"],
                         cls.resources["memory"]["used"],
                         cls.resources["memory"]["size"]))

            if server_id in cls.servers["building"]:
                cls.servers["building"].remove(server_id)
                found = True
            elif server_id in cls.servers["active"] or \
                    server_id in cls.servers["error"]:
                if cls.resources["vcpus"]["used"] - vcpus < 0:
                    cls.resources["vcpus"]["used"] = 0
                else:
                    cls.resources["vcpus"]["used"] -= vcpus

                if cls.resources["memory"]["used"] - memory < 0:
                    cls.resources["memory"]["used"] = 0
                else:
                    cls.resources["memory"]["used"] -= memory

                if server_id in cls.servers["active"]:
                    cls.servers["active"].remove(server_id)
                else:
                    cls.servers["error"].remove(server_id)

                cls.resources["instances"]["used"] -= 1

                LOG.info("released server_id=%s vcpus=%s memory=%s "
                         "[vcpu in use %s of %s; memory in use %s of %s]"
                         % (server_id,
                            vcpus,
                            memory,
                            cls.resources["vcpus"]["used"],
                            cls.resources["vcpus"]["size"],
                            cls.resources["memory"]["used"],
                            cls.resources["memory"]["size"]))

                cls.lastReleaseTime = datetime.now()
                found = True
            else:
                LOG.debug("release: instance %r not found!" % (server_id))

            cls.condition.notifyAll()
        return found

    @classmethod
    def deserialize(cls, entity):
        quota = super(SharedQuota, cls).deserialize(entity)

        cls.resources = entity["resources"]
        cls.enabled = entity["enabled"]
        cls.total = entity["total"]

        if isinstance(entity["lastAllocationTime"], datetime):
            cls.lastAllocationTime = entity["lastAllocationTime"]
        else:
            cls.lastAllocationTime = datetime.strptime(
                entity["lastAllocationTime"], "%Y-%m-%dT%H:%M:%S.%f")

        if isinstance(entity["lastReleaseTime"], datetime):
            cls.lastReleaseTime = entity["lastReleaseTime"]
        else:
            cls.lastReleaseTime = datetime.strptime(
                entity["lastReleaseTime"], "%Y-%m-%dT%H:%M:%S.%f")

        return quota
