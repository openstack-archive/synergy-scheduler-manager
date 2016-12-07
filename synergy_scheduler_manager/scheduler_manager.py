import logging
import re

from common.flavor import Flavor
from common.quota import SharedQuota
from common.request import Request
from common.server import Server
from oslo_config import cfg
from synergy.common.manager import Manager
from threading import Thread


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
LOG = logging.getLogger(__name__)


class Notifications(object):

    def __init__(self, projects, nova_manager):
        super(Notifications, self).__init__()

        self.projects = projects
        self.nova_manager = nova_manager

    def _makeServer(self, server_info):
        if not server_info:
            return

        flavor = Flavor()
        flavor.setMemory(server_info["memory_mb"])
        flavor.setVCPUs(server_info["vcpus"])
        flavor.setStorage(server_info["root_gb"])

        if "instance_type" in server_info:
            flavor.setName(server_info["instance_type"])

        server = Server()
        server.setFlavor(flavor)
        server.setUserId(server_info["user_id"])
        server.setMetadata(server_info["metadata"])
        server.setDeletedAt(server_info["deleted_at"])
        server.setTerminatedAt(server_info["terminated_at"])

        if "uuid" in server_info:
            server.setId(server_info["uuid"])
        elif "instance_id" in server_info:
            server.setId(server_info["instance_id"])

        if "project_id" in server_info:
            server.setProjectId(server_info["project_id"])
        elif "tenant_id" in server_info:
            server.setProjectId(server_info["tenant_id"])

        if "vm_state" in server_info:
            server.setState(server_info["vm_state"])
        elif "state" in server_info:
            server.setState(server_info["state"])

        return server

    def info(self, ctxt, publisher_id, event_type, payload, metadata):
        LOG.debug("Notification INFO: event_type=%s payload=%s"
                  % (event_type, payload))

        if payload is None or "state" not in payload:
            return

        state = payload["state"]

        if ((event_type == "compute.instance.delete.end" and
            (state == "deleted" or state == "error" or state == "building")) or
            (event_type == "compute.instance.update" and state == "error") or
                (event_type == "scheduler.run_instance" and state == "error")):
            server_info = None

            if event_type == "scheduler.run_instance":
                server_info = payload["request_spec"]["instance_type"]
            else:
                server_info = payload

            if server_info["tenant_id"] not in self.projects:
                return

            server = self._makeServer(server_info)
            flavor = server.getFlavor()

            message = "N/A"
            if "message" in server_info:
                message = server_info["message"]

            LOG.debug("Notification INFO (type=%s state=%s): vcpus=%s "
                      "memory=%s prj_id=%s server_id=%s (message=%s)"
                      % (event_type, server.getState(), flavor.getVCPUs(),
                         flavor.getMemory(), server.getProjectId(),
                         server.getId(), message))

            quota = self.projects[server.getProjectId()].getQuota()

            try:
                quota.release(server)
            except Exception as ex:
                LOG.warn("Cannot release server id=%r: %s"
                         % (server.getId(), ex))
                LOG.error("Exception has occured", exc_info=1)

    def warn(self, ctxt, publisher_id, event_type, payload, metadata):
        state = payload["state"]
        instance_id = payload["instance_id"]
        LOG.debug("Notification WARN: event_type=%s state=%s instance_id=%s "
                  "payload=%s" % (event_type, state, instance_id, payload))

    def error(self, ctxt, publisher_id, event_type, payload, metadata):
        server = None
        message = "N\A"
        server_info = None

        if event_type == "terminate_instance":
            server_info = payload["args"]["instance"]
            message = payload["exception"]["value"]

        elif event_type == "compute.instance.create.error" or\
                event_type == "compute.instance.update.error":
            server_info = payload
            message = payload["message"]

        server = self._makeServer(server_info)

        if not server:
            LOG.info("Notification ERROR: event_type=%s payload=%s"
                     % (event_type, payload))
            return

        if server.getProjectId() not in self.projects:
            return

        flavor = server.getFlavor()

        LOG.debug("Notification ERROR (type=%s state=%s): vcpus=%s "
                  "memory=%s prj_id=%s server_id=%s (error=%s)"
                  % (event_type, server.getState(), flavor.getVCPUs(),
                     flavor.getMemory(), server.getProjectId(),
                     server.getId(), message))

        if not server.getTerminatedAt() and not server.getDeletedAt():
            try:
                self.nova_manager.deleteServer(server)
            except Exception as ex:
                LOG.error("Cannot delete server id=%r: %s"
                          % (server.getId(), ex))

        quota = self.projects[server.getProjectId()].getQuota()

        try:
            quota.release(server)
        except Exception as ex:
            LOG.warn("Cannot release server id=%r: %s"
                     % (server.getId(), ex))
            LOG.error("Exception has occured", exc_info=1)


class Worker(Thread):

    def __init__(self, name, queue, projects, nova_manager,
                 keystone_manager, backfill_depth=100):
        super(Worker, self).__init__()
        self.setDaemon(True)

        self.name = name
        self.backfill_depth = backfill_depth
        self.queue = queue
        self.projects = projects
        self.nova_manager = nova_manager
        self.keystone_manager = keystone_manager
        self.exit = False
        LOG.info("Worker %r created!" % self.name)

    def getName(self):
        return self.name

    def destroy(self):
        try:
            self.queue.close()

            self.exit = True
        except Exception as ex:
            LOG.error(ex)
            raise ex

    def run(self):
        LOG.info("Worker %r running!" % self.name)
        queue_items = []
        last_release_time = SharedQuota.getLastReleaseTime()

        while not self.exit and not self.queue.isClosed():
            if last_release_time < SharedQuota.getLastReleaseTime():
                last_release_time = SharedQuota.getLastReleaseTime()

                while queue_items:
                    self.queue.reinsertItem(queue_items.pop(0))

            if len(queue_items) >= self.backfill_depth:
                SharedQuota.wait()
                continue

            queue_item = self.queue.getItem(blocking=False)

            if queue_item is None:
                if self.queue.getSize():
                    SharedQuota.wait()
                    continue
                else:
                    queue_item = self.queue.getItem(blocking=True)

            if queue_item is None:
                continue

            try:
                request = Request.fromDict(queue_item.getData())

                prj_id = request.getProjectId()
                context = request.getContext()
                server = request.getServer()
                server_id = server.getId()
                quota = None

                try:
                    s = self.nova_manager.getServer(server_id, detail=True)
                    if s.getState() != "building":
                        # or server["OS-EXT-STS:task_state"] != "scheduling":
                        self.queue.deleteItem(queue_item)
                        continue
                except Exception as ex:
                    LOG.warn("Worker %s: the server %r is not anymore availa"
                             "ble ! [reason=%s]" % (self.name, server_id, ex))
                    self.queue.deleteItem(queue_item)

                    continue

                quota = self.projects[prj_id].getQuota()
                quota = self.projects[prj_id].getQuota()
                computes = []
                blocking = False

                if server.isEphemeral() and not SharedQuota.isEnabled():
                    blocking = True

                if quota.allocate(server, blocking=blocking):
                    try:
                        computes = self.nova_manager.selectComputes(request)
                    except Exception as ex:
                        LOG.warn("Worker %s: compute not found for server %r!"
                                 " [reason=%s]" % (self.name,
                                                   server.getId(), ex))

                    found = False

                    for compute in computes:
                        try:
                            km = self.keystone_manager
                            trust = km.getTrust(context["trust_id"])
                            token = trust.getToken(km.getToken().getId())

                            context["auth_token"] = token.getId()
                            context["user_id"] = token.getUser().getId()

                            self.nova_manager.buildServer(request, compute)

                            LOG.info("Worker %r: server (id=%r) "
                                     "builded!" % (self.name, server.getId()))

                            found = True
                            break
                        except Exception as ex:
                            LOG.error("Worker %r: error on building the "
                                      "server (id=%r) reason=%s"
                                      % (self.name, server.getId(), ex))

                    if found:
                        self.queue.deleteItem(queue_item)
                    else:
                        quota.release(server)
                        queue_items.append(queue_item)
                else:
                    queue_items.append(queue_item)

            except Exception as ex:
                LOG.error("Exception has occured", exc_info=1)
                LOG.error("Worker %r: %s" % (self.name, ex))

                self.queue.deleteItem(queue_item)

        LOG.info("Worker %r destroyed!" % self.name)


class SchedulerManager(Manager):

    def __init__(self):
        super(SchedulerManager, self).__init__("SchedulerManager")

        self.config_opts = [
            cfg.IntOpt("backfill_depth", default=100),
            cfg.FloatOpt("default_TTL", default=10.0),
            cfg.ListOpt("projects", default=[], help="the projects list"),
            cfg.ListOpt("shares", default=[], help="the shares list"),
            cfg.ListOpt("TTLs", default=[], help="the TTLs list")
        ]
        self.workers = []

    def setup(self):
        if self.getManager("NovaManager") is None:
            raise Exception("NovaManager not found!")

        if self.getManager("QueueManager") is None:
            raise Exception("QueueManager not found!")

        if self.getManager("QuotaManager") is None:
            raise Exception("QuotaManager not found!")

        if self.getManager("KeystoneManager") is None:
            raise Exception("KeystoneManager not found!")

        if self.getManager("FairShareManager") is None:
            raise Exception("FairShareManager not found!")

        self.nova_manager = self.getManager("NovaManager")
        self.queue_manager = self.getManager("QueueManager")
        self.quota_manager = self.getManager("QuotaManager")
        self.keystone_manager = self.getManager("KeystoneManager")
        self.fairshare_manager = self.getManager("FairShareManager")
        self.default_TTL = float(CONF.SchedulerManager.default_TTL)
        self.backfill_depth = CONF.SchedulerManager.backfill_depth
        self.projects = {}
        self.listener = None
        self.exit = False
        self.configured = False

    def parseAttribute(self, attribute):
        if attribute is None:
            return None

        parsed_attribute = re.split('=', attribute)

        if len(parsed_attribute) > 1:
            if not parsed_attribute[-1].isdigit():
                raise Exception("wrong value %r found in %r!"
                                % (parsed_attribute[-1], parsed_attribute))

            if len(parsed_attribute) == 2:
                prj_name = parsed_attribute[0]
                value = float(parsed_attribute[1])
            else:
                raise Exception("wrong attribute definition: %r"
                                % parsed_attribute)
        else:
            raise Exception("wrong attribute definition: %r"
                            % parsed_attribute)

        return (prj_name, value)

    def execute(self, command, *args, **kargs):
        if command == "show":
            usr_id = kargs.get("user_id", None)
            usr_name = kargs.get("user_name", None)
            all_users = kargs.get("all_users", False)
            all_projects = kargs.get("all_projects", False)
            prj_id = kargs.get("project_id", None)
            prj_name = kargs.get("project_name", None)
            project = None

            if all_projects:
                return self.projects.values()

            if (usr_id is not None or usr_name is not None or all_users) and \
                    prj_id is None and prj_name is None:
                raise Exception("project id or name not defined!")

            if prj_id:
                project = self.projects.get(prj_id, None)

                if not project:
                    raise Exception("project (id=%r) not found!" % prj_id)
            elif prj_name:
                for prj in self.projects.values():
                    if prj_name == prj.getName():
                        project = prj
                        break

                if not project:
                    raise Exception("project (name=%r) not found!" % prj_name)
            elif not all_users:
                return self.projects.values()

            if usr_id or usr_name:
                    return project.getUser(id=usr_id, name=usr_name)
            elif all_users:
                return project.getUsers()
            else:
                return project
        else:
            raise Exception("command=%r not supported!" % command)

    def task(self):
        if not self.configured:
            for project in self.keystone_manager.getProjects():
                if project.getName() in CONF.SchedulerManager.projects:
                    CONF.SchedulerManager.projects.remove(project.getName())
                    project.setTTL(self.default_TTL)

                    try:
                        users = self.keystone_manager.getUsers(
                            prj_id=project.getId())

                        for user in users:
                            project.addUser(user)
                    except Exception as ex:
                        LOG.error("Exception has occured", exc_info=1)
                        LOG.error(ex)

                    self.projects[project.getName()] = project
                else:
                    quota = self.nova_manager.getQuota(project.getId())

                    if quota.getSize("vcpus") <= -1 and \
                        quota.getSize("memory") <= -1 and \
                            quota.getSize("instances") <= -1:

                        qc = self.nova_manager.getQuota(project.getId(),
                                                        is_class=True)

                        self.nova_manager.updateQuota(qc)

            if len(CONF.SchedulerManager.projects) > 0:
                raise Exception("projects %s not found, please check the syn"
                                "ergy.conf" % CONF.SchedulerManager.projects)

            self.quota_manager.updateSharedQuota()

            for prj_ttl in CONF.SchedulerManager.TTLs:
                prj_name, TTL = self.parseAttribute(prj_ttl)
                self.projects[prj_name].setTTL(TTL)

            for prj_share in CONF.SchedulerManager.shares:
                prj_name, share_value = self.parseAttribute(prj_share)
                p_share = self.projects[prj_name].getShare()
                p_share.setValue(share_value)

            for prj_name, project in self.projects.items():
                del self.projects[prj_name]
                self.projects[project.getId()] = project

                self.quota_manager.addProject(project)

                self.fairshare_manager.addProject(project)

            self.quota_manager.updateSharedQuota()
            self.fairshare_manager.calculateFairShare()

            try:
                self.dynamic_queue = self.queue_manager.createQueue("DYNAMIC")
            except Exception as ex:
                LOG.error("Exception has occured", exc_info=1)
                LOG.error(ex)

            self.dynamic_queue = self.queue_manager.getQueue("DYNAMIC")

            dynamic_worker = Worker("DYNAMIC",
                                    self.dynamic_queue,
                                    self.projects,
                                    self.nova_manager,
                                    self.keystone_manager,
                                    self.backfill_depth)
            dynamic_worker.start()

            self.workers.append(dynamic_worker)

            self.notifications = Notifications(self.projects,
                                               self.nova_manager)

            target = self.nova_manager.getTarget(topic='notifications',
                                                 exchange="nova")

            self.listener = self.nova_manager.getNotificationListener(
                targets=[target],
                endpoints=[self.notifications])

            self.quota_manager.deleteExpiredServers()

            self.listener.start()
            self.configured = True
            return

        for project in self.projects.values():
            users = self.keystone_manager.getUsers(prj_id=project.getId())

            for user in users:
                try:
                    project.addUser(user)
                except Exception:
                    pass

    def destroy(self):
        for queue_worker in self.workers:
            queue_worker.destroy()

    def processRequest(self, request):
        server = request.getServer()

        try:
            if request.getProjectId() in self.projects:
                self.nova_manager.setQuotaTypeServer(server)

                project = self.projects[request.getProjectId()]
                quota = project.getQuota()

                if server.isPermanent():
                    if quota.allocate(server, blocking=False):
                        self.nova_manager.buildServer(request)

                        LOG.info("new request: id=%r user_id=%s prj_id=%s "
                                 "quota=private" % (request.getId(),
                                                    request.getUserId(),
                                                    request.getProjectId()))
                    else:
                        self.nova_manager.deleteServer(server)
                        LOG.info("request rejected (quota exceeded): "
                                 "id=%r user_id=%s prj_id=%s "
                                 "quota=private" % (request.getId(),
                                                    request.getUserId(),
                                                    request.getProjectId()))
                else:
                    timestamp = request.getCreatedAt()
                    priority = 0
                    retry = request.getRetry()

                    if retry:
                        num_attempts = retry["num_attempts"]

                        if num_attempts:
                            quota.release(server)

                            priority = 99999999
                            LOG.info("released resource uuid %s num attempts"
                                     "%s" % (request.getId(), num_attempts))

                    if priority == 0:
                        priority = self.fairshare_manager.calculatePriority(
                            user_id=request.getUserId(),
                            prj_id=request.getProjectId(),
                            timestamp=timestamp,
                            retry=0)

                    context = request.getContext()

                    km = self.keystone_manager
                    token_user = km.validateToken(context["auth_token"])
                    token_admin = km.getToken()

                    trusts = km.getTrusts(
                        user_id=token_user.getUser().getId(), token=token_user)

                    if trusts:
                        trust = trusts[0]
                    else:
                        trust = km.makeTrust(
                            token_admin.getUser().getId(), token_user)

                    context["trust_id"] = trust.getId()

                    self.dynamic_queue.insertItem(request.getUserId(),
                                                  request.getProjectId(),
                                                  priority=priority,
                                                  data=request.toDict())

                    LOG.info("new request: id=%r user_id=%s prj_id=%s priority"
                             "=%s quota=shared" % (request.getId(),
                                                   request.getUserId(),
                                                   request.getProjectId(),
                                                   priority))
            else:
                self.nova_manager.buildServer(request)

                self.nova_manager.setQuotaTypeServer(server)
                LOG.info("new request: id=%r user_id=%s prj_id=%s "
                         "quota=private" % (request.getId(),
                                            request.getUserId(),
                                            request.getProjectId()))
        except Exception as ex:
            LOG.error("Exception has occured", exc_info=1)
            LOG.error(ex)
