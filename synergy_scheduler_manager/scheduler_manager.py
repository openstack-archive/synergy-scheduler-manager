﻿import logging
import re

from common.flavor import Flavor
from common.quota import SharedQuota
from common.request import Request
from common.server import Server
from oslo_config import cfg
from synergy.common.manager import Manager
from synergy.exception import SynergyError
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

        if "host" in server_info:
            server.setHost(server_info["host"])

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

        event_types = ["compute.instance.create.end",
                       "compute.instance.delete.end",
                       "compute.instance.update",
                       "scheduler.run_instance"]

        if event_type not in event_types:
            return

        server_info = None

        if event_type == "scheduler.run_instance":
            server_info = payload["request_spec"]["instance_type"]
        else:
            server_info = payload

        server = self._makeServer(server_info)
        server_id = server.getId()
        host = server.getHost()

        if server.getProjectId() not in self.projects:
            return

        if event_type == "compute.instance.create.end" and \
                state == "active":
            LOG.info("the server %s is now active on host %s"
                     % (server_id, host))
        else:
            quota = self.projects[server.getProjectId()].getQuota()

            if event_type == "compute.instance.delete.end":
                LOG.info("the server %s has been deleted on host %s"
                         % (server_id, host))
                try:
                    quota.release(server)
                except Exception as ex:
                        LOG.warn("cannot release server %s "
                                 "(reason=%s)" % (server_id, ex))
            elif state == "error":
                LOG.info("error occurred on server %s (host %s)"
                         % (server_id, host))

                if not server.getTerminatedAt() and not server.getDeletedAt():
                    try:
                        self.nova_manager.deleteServer(server)
                    except Exception as ex:
                        LOG.error("cannot delete server %s: %s"
                                  % (server_id, ex))

                try:
                    quota.release(server)
                except Exception as ex:
                        LOG.warn("cannot release server %s "
                                 "(reason=%s)" % (server_id, ex))

    def warn(self, ctxt, publisher_id, event_type, payload, metadata):
        LOG.debug("Notification WARN: event_type=%s, payload=%s metadata=%s"
                  % (event_type, payload, metadata))

    def error(self, ctxt, publisher_id, event_type, payload, metadata):
        LOG.debug("Notification ERROR: event_type=%s, payload=%s metadata=%s"
                  % (event_type, payload, metadata))


class Worker(Thread):

    def __init__(self, name, queue, project_manager, nova_manager,
                 keystone_manager, backfill_depth=100):
        super(Worker, self).__init__()
        self.setDaemon(True)

        self.name = name
        self.backfill_depth = backfill_depth
        self.queue = queue
        self.project_manager = project_manager
        self.nova_manager = nova_manager
        self.keystone_manager = keystone_manager
        self.exit = False
        LOG.info("Worker %s created!" % self.name)

    def getName(self):
        return self.name

    def destroy(self):
        try:
            self.queue.close()

            self.exit = True
        except SynergyError as ex:
            LOG.error(ex)
            raise ex

    def run(self):
        LOG.info("Worker %s running!" % self.name)
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
                user_id = request.getUserId()
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
                except SynergyError as ex:
                    LOG.warn("the server %s is not anymore available!"
                             " (reason=%s)" % (server_id, ex))
                    self.queue.delete(queue_item)

                    continue

                project = self.project_manager.getProject(id=prj_id)

                if not project:
                    raise SynergyError("project %r not found!" % prj_id)

                quota = project.getQuota()
                blocking = False

                if server.isEphemeral() and not SharedQuota.isEnabled():
                    blocking = True

                if quota.allocate(server, blocking=blocking):
                    found = False

                    try:
                        km = self.keystone_manager
                        trust = km.getTrust(context["trust_id"])
                        token = trust.getToken(km.getToken().getId())

                        context["auth_token"] = token.getId()
                        context["user_id"] = token.getUser().getId()
                    except SynergyError as ex:
                        LOG.error("error on getting the token for server "
                                  "%s (reason=%s)" % (server.getId(), ex))
                        raise ex

                    try:
                        self.nova_manager.buildServer(request)

                        LOG.info("building server %s (user_id=%s prj_id=%s quo"
                                 "ta=shared)" % (server_id, user_id, prj_id))

                        found = True
                    except SynergyError as ex:
                        LOG.error("error on building the server %s (reason=%s)"
                                  % (server.getId(), ex))

                    if found:
                        self.queue.deleteItem(queue_item)
                    else:
                        quota.release(server)
                        queue_items.append(queue_item)
                else:
                    queue_items.append(queue_item)

            except SynergyError as ex:
                LOG.error("Exception has occured", exc_info=1)
                LOG.error("Worker %s: %s" % (self.name, ex))

                self.queue.deleteItem(queue_item)

        LOG.info("Worker %s destroyed!" % self.name)


class SchedulerManager(Manager):

    def __init__(self):
        super(SchedulerManager, self).__init__("SchedulerManager")

        self.config_opts = [
            cfg.StrOpt("notification_topic", default="notifications"),
            cfg.IntOpt("backfill_depth", default=100),
        ]
        self.workers = []

    def setup(self):
        if self.getManager("NovaManager") is None:
            raise SynergyError("NovaManager not found!")

        if self.getManager("QueueManager") is None:
            raise SynergyError("QueueManager not found!")

        if self.getManager("QuotaManager") is None:
            raise SynergyError("QuotaManager not found!")

        if self.getManager("KeystoneManager") is None:
            raise SynergyError("KeystoneManager not found!")

        if self.getManager("FairShareManager") is None:
            raise SynergyError("FairShareManager not found!")

        if self.getManager("ProjectManager") is None:
            raise SynergyError("ProjectManager not found!")

        self.nova_manager = self.getManager("NovaManager")
        self.queue_manager = self.getManager("QueueManager")
        self.quota_manager = self.getManager("QuotaManager")
        self.keystone_manager = self.getManager("KeystoneManager")
        self.fairshare_manager = self.getManager("FairShareManager")
        self.project_manager = self.getManager("ProjectManager")
        self.backfill_depth = CONF.SchedulerManager.backfill_depth
        self.notification_topic = CONF.SchedulerManager.notification_topic
        self.projects = {}
        self.listener = None
        self.exit = False
        self.configured = False

    def execute(self, command, *args, **kargs):
        raise SynergyError("command %r not supported!" % command)

    def task(self):
        if self.configured:
            return

        self.quota_manager.updateSharedQuota()

        try:
            self.dynamic_queue = self.queue_manager.createQueue("DYNAMIC")
        except SynergyError as ex:
            LOG.error("Exception has occured", exc_info=1)
            LOG.error(ex)

        self.dynamic_queue = self.queue_manager.getQueue("DYNAMIC")

        dynamic_worker = Worker("DYNAMIC",
                                self.dynamic_queue,
                                self.project_manager,
                                self.nova_manager,
                                self.keystone_manager,
                                self.backfill_depth)
        dynamic_worker.start()

        self.workers.append(dynamic_worker)

        self.notifications = Notifications(self.projects, self.nova_manager)

        target = self.nova_manager.getTarget(topic=self.notification_topic,
                                             exchange="nova")

        self.listener = self.nova_manager.getNotificationListener(
            targets=[target],
            endpoints=[self.notifications])

        self.quota_manager.deleteExpiredServers()

        self.listener.start()
        self.configured = True

    def destroy(self):
        for queue_worker in self.workers:
            queue_worker.destroy()

    def processRequest(self, request):
        server = request.getServer()

        project = self.project_manager.getProject(id=request.getProjectId())

        try:
            if project:
                quota = project.getQuota()
                retry = request.getRetry()
                num_attempts = 0
                reason = None

                if retry:
                    num_attempts = retry.get("num_attempts", 0)
                    reason = retry.get("exc_reason", "n/a")

                if 0 < num_attempts < 3:
                    self.nova_manager.buildServer(request)

                    LOG.info("retrying to build the server %s (user_id"
                             "=%s prj_id=%s, num_attempts=%s, reason=%s)"
                             % (request.getId(), request.getUserId(),
                                request.getProjectId(), num_attempts, reason))
                    return

                if server.isPermanent():
                    if quota.allocate(server, blocking=False):
                        LOG.info("new request: id=%s user_id=%s prj_id=%s "
                                 "quota=private" % (request.getId(),
                                                    request.getUserId(),
                                                    request.getProjectId()))

                        self.nova_manager.buildServer(request)
                        LOG.info("building server %s (user_id=%s prj_id=%s "
                                 "quota=private)" % (server.getId(),
                                                     request.getUserId(),
                                                     request.getProjectId()))
                    else:
                        self.nova_manager.deleteServer(server)
                        LOG.info("request rejected (quota exceeded): "
                                 "id=%s user_id=%s prj_id=%s "
                                 "quota=private" % (request.getId(),
                                                    request.getUserId(),
                                                    request.getProjectId()))
                else:
                    priority = self.fairshare_manager.calculatePriority(
                        user_id=request.getUserId(),
                        prj_id=request.getProjectId(),
                        timestamp=request.getCreatedAt(),
                        retry=num_attempts)

                    context = request.getContext()

                    km = self.keystone_manager
                    token_user = km.validateToken(context["auth_token"])
                    token_admin = km.getToken()
                    admin_id = token_admin.getUser().getId()
                    trust = None

                    trusts = km.getTrusts(
                        user_id=token_user.getUser().getId(), token=token_user)

                    for _trust in trusts:
                        if _trust.getTrusteeUserId() == admin_id:
                            trust = _trust
                            break

                    if not trust:
                        trust = km.makeTrust(
                            token_admin.getUser().getId(), token_user)

                    context["trust_id"] = trust.getId()

                    self.dynamic_queue.insertItem(request.getUserId(),
                                                  request.getProjectId(),
                                                  priority=priority,
                                                  data=request.toDict())

                    LOG.info("new request: id=%s user_id=%s prj_id=%s priority"
                             "=%s quota=shared" % (request.getId(),
                                                   request.getUserId(),
                                                   request.getProjectId(),
                                                   priority))
            else:
                self.nova_manager.buildServer(request)
        except SynergyError as ex:
            LOG.error("Exception has occured", exc_info=1)
            LOG.error(ex)
