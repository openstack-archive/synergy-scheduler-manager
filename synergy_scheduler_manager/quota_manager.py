import ConfigParser
import logging

from common.quota import SharedQuota
from oslo_config import cfg
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


class QuotaManager(Manager):

    def __init__(self):
        super(QuotaManager, self).__init__("QuotaManager")

    def setup(self):
        self.projects = {}

        if self.getManager("NovaManager") is None:
            raise Exception("NovaManager not found!")

        if self.getManager("KeystoneManager") is None:
            raise Exception("KeystoneManager not found!")

        self.nova_manager = self.getManager("NovaManager")
        self.keystone_manager = self.getManager("KeystoneManager")
        self.listener = None

    def destroy(self):
        LOG.info("destroy invoked!")
        SharedQuota.disable()

    def execute(self, command, *args, **kargs):
        if command == "show":
            prj_id = kargs.get("project_id", None)
            prj_name = kargs.get("project_name", None)
            all_projects = kargs.get("all_projects", None)

            if prj_id:
                project = self.projects.get(prj_id, None)

                if project:
                    return project

                raise Exception("project (id=%r) not found!" % prj_id)
            elif prj_name:
                for project in self.projects.values():
                    if prj_name == project.getName():
                        return project

                raise Exception("project (name=%r) not found!" % prj_name)
            elif all_projects:
                return self.projects.values()
            else:
                return SharedQuota()
        elif command == "GET_SHARED_QUOTA":
            return SharedQuota()
        elif command == "GET_PROJECTS":
            return self.projects.values()
        elif command == "GET_PROJECT":
            return self.getProject(*args, **kargs)
        else:
            raise Exception("command=%r not supported!" % command)

    def task(self):
        try:
            self.updateSharedQuota()
            self.deleteExpiredServers()
        except Exception as ex:
            LOG.error(ex)

    def getProject(self, prj_id):
        return self.projects.get(prj_id, None)

    def getProjects(self):
        return self.projects

    def addProject(self, project):
        if self.projects.get(project.getId(), None):
            raise Exception("project %r already exists!" % (project.getId()))

        try:
            quota = self.nova_manager.getQuota(project.getId())

            if quota.getSize("vcpus") > 0 and \
                    quota.getSize("memory") > 0 and \
                    quota.getSize("instances") > 0:

                self.nova_manager.updateQuota(quota, is_class=True)
                quota.setSize("vcpus", -1)
                quota.setSize("memory", -1)
                quota.setSize("instances", -1)

                self.nova_manager.updateQuota(quota)

            class_quota = self.nova_manager.getQuota(
                project.getId(), is_class=True)

            quota = project.getQuota()
            quota.setSize("vcpus", class_quota.getSize("vcpus"))
            quota.setSize("memory", class_quota.getSize("memory"))
            quota.setSize("instances", class_quota.getSize("instances"))

            quota.setSize(
                "vcpus", SharedQuota.getSize("vcpus"), private=False)
            quota.setSize(
                "memory", SharedQuota.getSize("memory"), private=False)
            quota.setSize(
                "instances", SharedQuota.getSize("instances"), private=False)

            servers = self.nova_manager.getProjectServers(project.getId())

            for server in servers:
                if server.getState() != "building":
                    quota.allocate(server)

            self.projects[project.getId()] = project
        except Exception as ex:
            LOG.error("Exception has occured", exc_info=1)
            LOG.error(ex)
            raise ex

    def removeProject(self, project, destroy=False):
        project = self.projects[project.getId()]

        if project is None:
            return

        try:
            if destroy:
                quota = project.getQuota()

                ids = []
                ids.extend(quota.getServers("active", private=False))
                ids.extend(quota.getServers("pending", private=False))
                ids.extend(quota.getServers("error", private=False))

                for server_id in ids:
                    self.nova_manager.deleteServer(server_id)

            del self.projects[project.getId()]
        except Exception as ex:
            LOG.error(ex)
            raise ex

    def deleteExpiredServers(self):
        for prj_id, project in self.getProjects().items():
            TTL = project.getTTL()
            quota = project.getQuota()

            ids = []
            ids.extend(quota.getServers("active", private=False))
            ids.extend(quota.getServers("error", private=False))

            if TTL == 0:
                continue

            if not ids:
                continue

            try:
                servers = self.nova_manager.getExpiredServers(
                    prj_id=prj_id, server_ids=ids, TTL=TTL)

                for server in servers:
                    uuid = server.getId()
                    state = server.getState()

                    if server.getState() == "error":
                        LOG.info("the server instance %r will be destroyed "
                                 "because it is in %s state (TTL=%s, prj_id"
                                 "=%r)" % (uuid, state, TTL, prj_id))
                    else:
                        LOG.info("the server instance %r will be destroyed "
                                 "because it exceeded its maximum time to live"
                                 " (TTL=%s, state=%s, prj_id=%r)"
                                 % (uuid, TTL, state, prj_id))

                    self.nova_manager.deleteServer(server)
            except Exception as ex:
                LOG.error(ex)
                raise ex

    def updateSharedQuota(self):
        # calculate the the total limit per cores and ram
        total_memory = float(0)
        total_vcpus = float(0)
        static_memory = float(0)
        static_vcpus = float(0)
        shared_memory = float(0)
        shared_vcpus = float(0)

        try:
            cpu_ratio = self.nova_manager.getParameter("cpu_allocation_ratio")

            ram_ratio = self.nova_manager.getParameter("ram_allocation_ratio")

            hypervisors = self.nova_manager.getHypervisors()

            for hv in hypervisors:
                if hv.getState() == "down" or hv.getStatus() == "disabled":
                    continue

                if hv.getMemory() > 0:
                    total_memory += hv.getMemory()

                if hv.getVCPUs() > 0:
                    total_vcpus += hv.getVCPUs()

            total_memory *= float(ram_ratio)
            total_vcpus *= float(cpu_ratio)

            kprojects = self.keystone_manager.getProjects()

            for kproject in kprojects:
                project = self.getProject(kproject.getId())

                if project:
                    quota = project.getQuota()
                else:
                    quota = self.nova_manager.getQuota(kproject.getId())

                if quota.getSize("vcpus") > 0:
                    static_vcpus += quota.getSize("vcpus")

                if quota.getSize("memory") > 0:
                    static_memory += quota.getSize("memory")

            enabled = False

            if total_vcpus < static_vcpus:
                if self.getProjects():
                    LOG.warn("shared quota: the total statically "
                             "allocated vcpus (%s) is greater than the "
                             "total amount of vcpus allowed (%s)"
                             % (static_vcpus, total_vcpus))
            else:
                shared_vcpus = total_vcpus - static_vcpus

                if total_memory < static_memory:
                    if self.getProjects():
                        LOG.warn("shared quota: the total statically "
                                 "allocated memory (%s) is greater than "
                                 "the total amount of memory allowed (%s)"
                                 % (static_memory, total_memory))
                else:
                    enabled = True
                    shared_memory = total_memory - static_memory

            if enabled:
                LOG.info("shared quota enabled: vcpus=%s memory=%s"
                         % (shared_vcpus, shared_memory))

                SharedQuota.enable()
                SharedQuota.setSize("vcpus", shared_vcpus)
                SharedQuota.setSize("memory", shared_memory)
            else:
                LOG.info("shared quota disabled")

                SharedQuota.disable()
                SharedQuota.setSize("vcpus", 0)
                SharedQuota.setSize("memory", 0)

            for project in self.getProjects().values():
                quota = project.getQuota()
                quota.setSize("vcpus", shared_vcpus, private=False)
                quota.setSize("memory", shared_memory, private=False)
        except Exception as ex:
            LOG.error("Exception has occured", exc_info=1)
            LOG.error(ex)
            raise ex
