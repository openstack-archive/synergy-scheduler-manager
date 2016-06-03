import ConfigParser
import logging
import threading

try:
    from oslo_config import cfg
except ImportError:
    from oslo.config import cfg

from synergy.common.manager import Manager


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


CONF = cfg.CONF
CONFIG = ConfigParser.SafeConfigParser()
LOG = logging.getLogger(__name__)


class DynamicQuota(object):

    def __init__(self):
        self.exit = False
        self.projects = {}
        self.ram = {"in_use": 0, "limit": 0}
        self.cores = {"in_use": 0, "limit": 0}
        self.condition = threading.Condition()

    def setSize(self, cores, ram):
        self.ram["limit"] = ram
        self.cores["limit"] = cores

    def getSize(self):
        return {"cores": self.cores["limit"], "ram": self.ram["limit"]}

    def getProjects(self):
        return self.projects

    def getProject(self, prj_id):
        return self.projects.get(prj_id, None)

    def addProject(self, prj_id, prj_name, usage=None):
        if prj_id not in self.projects:
            with self.condition:
                project = {"name": prj_name,
                           "cores": 0,
                           "ram": 0,
                           "instances": {"active": [], "pending": []},
                           "TTL": 0}

                if usage is not None:
                    project["cores"] = usage["cores"]
                    project["ram"] = usage["ram"]
                    project["instances"]["active"].extend(usage["instances"])

                    self.ram["in_use"] += project["ram"]
                    self.cores["in_use"] += project["cores"]

                self.projects[prj_id] = project
                self.condition.notifyAll()

    def removeProject(self, prj_id):
        if prj_id in self.projects:
            with self.condition:
                project = self.projects[prj_id]

                self.ram["in_use"] -= project["ram"]
                self.cores["in_use"] -= project["cores"]

                del self.projects[prj_id]
                self.condition.notifyAll()

            return True
        return False

    def close(self):
        self.exit = True

    def allocate(self, instance_id, prj_id, cores, ram, blocking=True):
        if prj_id not in self.projects:
            return

        project = self.projects[prj_id]

        if project is None:
            return

        found = False

        with self.condition:
            if instance_id in project["instances"]["active"]:
                found = True
            elif instance_id in project["instances"]["pending"]:
                found = True
            else:
                project["instances"]["pending"].append(instance_id)

            while (not self.exit and not found and
                   instance_id in project["instances"]["pending"]):

                LOG.debug("allocate instance_id=%s project=%s cores=%s "
                          "ram=%s [vcpu in use %s of %s; ram in use %s of %s]"
                          % (instance_id,
                             project["name"],
                             cores,
                             ram,
                             self.cores["in_use"],
                             self.cores["limit"],
                             self.ram["in_use"],
                             self.ram["limit"]))

                if (self.cores["limit"] - self.cores["in_use"] >= cores) and \
                   (self.ram["limit"] - self.ram["in_use"] >= ram):
                    self.cores["in_use"] += cores
                    self.ram["in_use"] += ram
                    project["cores"] += cores
                    project["ram"] += ram

                    found = True
                    project["instances"]["active"].append(instance_id)
                    project["instances"]["pending"].remove(instance_id)

                    LOG.info("allocated instance_id=%s project=%s cores=%s ram"
                             "=%s [vcpu in use %s of %s; ram in use %s of %s]"
                             % (instance_id,
                                project["name"],
                                cores,
                                ram,
                                self.cores["in_use"],
                                self.cores["limit"],
                                self.ram["in_use"],
                                self.ram["limit"]))
                elif blocking:
                    LOG.info("allocate wait!!!")
                    self.condition.wait()

            self.condition.notifyAll()

        return found

    def release(self, instance_id, prj_id, cores, ram):
        if prj_id not in self.projects:
            return

        project = self.projects[prj_id]

        LOG.debug("release instance_id=%s project=%s cores=%s "
                  "ram=%s [vcpu in use %s of %s; ram in use %s of %s]"
                  % (instance_id,
                     project["name"],
                     cores,
                     ram,
                     self.cores["in_use"],
                     self.cores["limit"],
                     self.ram["in_use"],
                     self.ram["limit"]))

        with self.condition:
            if instance_id in project["instances"]["pending"]:
                project["instances"]["pending"].remove(instance_id)
            elif instance_id in instance_id in project["instances"]["active"]:
                if self.cores["in_use"] - cores < 0:
                    self.cores["in_use"] = 0
                else:
                    self.cores["in_use"] -= cores

                if self.ram["in_use"] - ram < 0:
                    self.ram["in_use"] = 0
                else:
                    self.ram["in_use"] -= ram

                if project["cores"] - cores < 0:
                    project["cores"] = 0
                else:
                    project["cores"] -= cores

                if project["ram"] - ram < 0:
                    project["ram"] = 0
                else:
                    project["ram"] -= ram

                project["instances"]["active"].remove(instance_id)

                LOG.info("released instance_id=%s project=%s cores=%s "
                         "ram=%s [vcpu in use %s of %s; ram in use %s of %s]"
                         % (instance_id,
                            project["name"],
                            cores,
                            ram,
                            self.cores["in_use"],
                            self.cores["limit"],
                            self.ram["in_use"],
                            self.ram["limit"]))
            else:
                LOG.debug("release: instance '%s' not found!" % (instance_id))

            self.condition.notifyAll()

    def toDict(self):
        quota = {}
        quota["ram"] = self.ram
        quota["cores"] = self.cores
        quota["projects"] = self.projects

        return quota


class QuotaManager(Manager):

    def __init__(self):
        super(QuotaManager, self).__init__(name="QuotaManager")

    def setup(self):
        try:
            self.dynamic_quota = DynamicQuota()

            if self.getManager("NovaManager") is None:
                raise Exception("NovaManager not found!")

            if self.getManager("KeystoneManager") is None:
                raise Exception("KeystoneManager not found!")

            self.nova_manager = self.getManager("NovaManager")
            self.keystone_manager = self.getManager("KeystoneManager")
            self.listener = None
        except Exception as ex:
            LOG.error("Exception has occured", exc_info=1)
            LOG.error(ex)

    def destroy(self):
        LOG.info("destroy invoked!")
        self.dynamic_quota.close()

    def execute(self, command, *args, **kargs):
        if command == "ADD_PROJECT":
            return self.addProject(*args, **kargs)
        elif command == "GET_PROJECT":
            return self.getProject(*args, **kargs)
        elif command == "REMOVE_PROJECT":
            return self.removeProject(*args, **kargs)
        elif command == "GET_DYNAMIC_QUOTA":
            return self.dynamic_quota
        else:
            raise Exception("command=%r not supported!" % command)

    def task(self):
        try:
            self.updateDynamicQuota()
            self.deleteExpiredServices()
        except Exception as ex:
            LOG.error(ex)

    def getProject(self, prj_id):
        return self.dynamic_quota.getProject(prj_id)

    def addProject(self, prj_id, prj_name):
        try:
            quota = {"cores": -1, "ram": -1, "instances": -1}
            self.nova_manager.execute("UPDATE_QUOTA", prj_id, quota)

            usage = self.nova_manager.execute("GET_PROJECT_USAGE", prj_id)
            self.dynamic_quota.addProject(prj_id, prj_name, usage)

            self.updateDynamicQuota()
        except Exception as ex:
            LOG.error(ex)
            raise ex

    def removeProject(self, prj_id, destroy=False):
        project = self.dynamic_quota.getProject(prj_id)
        if project is None:
            return

        try:
            if destroy:
                ids = []
                ids.extend(project["instances"]["active"])
                ids.extend(project["instances"]["pending"])

                for instance_id in ids:
                    self.nova_manager.execute("DELETE_SERVER", instance_id)

            quota = self.nova_manager.execute("GET_QUOTA", defaults=True)
            self.nova_manager.execute("UPDATE_QUOTA", prj_id, quota)

            self.dynamic_quota.removeProject(prj_id)

            self.updateDynamicQuota()
        except Exception as ex:
            LOG.error(ex)
            raise ex

    def deleteExpiredServices(self):
        for prj_id, project in self.dynamic_quota.projects.items():
            instance_ids = project["instances"]["active"]
            TTL = project["TTL"]

            if project["TTL"] == 0:
                continue

            try:
                expired_ids = self.nova_manager.execute("GET_EXPIRED_SERVERS",
                                                        prj_id=prj_id,
                                                        instances=instance_ids,
                                                        expiration=TTL)

                for instance_id in expired_ids:
                    self.nova_manager.execute("DELETE_SERVER", instance_id)
            except Exception as ex:
                LOG.error(ex)
                raise ex

    def updateDynamicQuota(self):
        # calculate the the total limit per cores and ram
        total_ram = float(0)
        total_cores = float(0)
        static_ram = float(0)
        static_cores = float(0)
        dynamic_ram = float(0)
        dynamic_cores = float(0)

        try:
            cpu_ratio = self.nova_manager.execute("GET_PARAMETER",
                                                  name="cpu_allocation_ratio",
                                                  default=float(16))

            ram_ratio = self.nova_manager.execute("GET_PARAMETER",
                                                  name="ram_allocation_ratio",
                                                  default=float(16))

            quota_default = self.nova_manager.execute("GET_QUOTA",
                                                      defaults=True)

            hypervisors = self.nova_manager.execute("GET_HYPERVISORS")

            for hypervisor in hypervisors:
                if hypervisor["status"] == "enabled" and \
                   hypervisor["state"] == "up":
                    info = self.nova_manager.execute("GET_HYPERVISOR",
                                                     hypervisor["id"])

                    total_ram += info["memory_mb"]
                    total_cores += info["vcpus"]

            total_ram *= float(ram_ratio)
            total_cores *= float(cpu_ratio)

            # LOG.info("total_ram=%s total_cores=%s"
            # % (total_ram, total_cores))

            kprojects = self.keystone_manager.execute("GET_PROJECTS")

            for project in kprojects:
                prj_id = project["id"]
                # prj_name = str(project["name"])

                if self.dynamic_quota.getProject(prj_id) is None:
                    quota = self.nova_manager.execute("GET_QUOTA", prj_id)

                    if quota["cores"] == -1 and quota["ram"] == -1:
                        quota["cores"] = quota_default["cores"]
                        quota["ram"] = quota_default["ram"]

                        try:
                            self.nova_manager.execute("UPDATE_QUOTA",
                                                      prj_id,
                                                      quota_default)
                        except Exception as ex:
                            LOG.error(ex)

                    static_cores += quota["cores"]
                    static_ram += quota["ram"]

            enabled = False

            if total_cores < static_cores:
                LOG.warn("dynamic quota: the total statically "
                         "allocated cores (%s) is greater than the total "
                         "amount of cores allowed (%s)"
                         % (static_cores, total_cores))
            else:
                enabled = True
                dynamic_cores = total_cores - static_cores

            if total_ram < static_ram:
                enabled = False
                LOG.warn("dynamic quota: the total statically "
                         "allocated ram (%s) is greater than the total "
                         "amount of ram allowed (%s)"
                         % (static_ram, total_ram))
            else:
                enabled = True
                dynamic_ram = total_ram - static_ram

            if enabled:
                LOG.info("dynamic quota: cores=%s ram=%s"
                         % (dynamic_cores, dynamic_ram))

            self.dynamic_quota.setSize(dynamic_cores, dynamic_ram)

            """
            LOG.info("cpu_ratio=%s, ram_ratio=%s" % (cpu_ratio, ram_ratio))
            LOG.info("total_cores=%s total_ram=%s" % (total_cores, total_ram))
            LOG.info("static cores=%s ram=%s" % (static_cores, static_ram))
            LOG.info("dynamic cores=%s ram=%s" % (dynamic_cores, dynamic_ram))
            """
            LOG.debug("dynamic quota %s" % self.dynamic_quota.toDict())
        except Exception as ex:
            LOG.error(ex)
            raise ex
