import logging
import time

from common.quota import SharedQuota
from common.request import Request
from datetime import datetime
from datetime import timedelta
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
            try:
                if last_release_time < SharedQuota.getLastReleaseTime():
                    last_release_time = SharedQuota.getLastReleaseTime()

                    while queue_items:
                        self.queue.restore(queue_items.pop(0))

                    for project in self.project_manager.getProjects():
                        for user in project.getUsers():
                            self.queue.updatePriority(user)

                if len(queue_items) >= self.backfill_depth:
                    SharedQuota.wait()
                    continue

                queue_item = self.queue.dequeue(block=False)

                if queue_item is None:
                    if self.queue.getSize():
                        SharedQuota.wait()
                        continue
                    else:
                        queue_item = self.queue.dequeue(block=True)

                if queue_item is None:
                    continue

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
                        self.queue.delete(queue_item)
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

                        LOG.info("building server %s user_id=%s prj_id=%s quo"
                                 "ta=shared" % (server_id, user_id, prj_id))

                        found = True
                    except Exception as ex:
                        LOG.error("error on building the server %s (reason=%s)"
                                  % (server.getId(), ex))

                    if found:
                        self.queue.delete(queue_item)
                    else:
                        quota.release(server)
                        queue_items.append(queue_item)
                else:
                    queue_items.append(queue_item)

            except Exception as ex:
                LOG.error("Exception has occured", exc_info=1)
                LOG.error("Worker %s: %s" % (self.name, ex))

                self.queue.delete(queue_item)

        LOG.info("Worker %s destroyed!" % self.name)


class SchedulerManager(Manager):

    def __init__(self):
        super(SchedulerManager, self).__init__("SchedulerManager")

        self.config_opts = [
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
        self.exit = False
        self.configured = False
        self.queue = None

    def execute(self, command, *args, **kargs):
        raise SynergyError("command %r not supported!" % command)

    def task(self):
        if self.configured:
            return

        try:
            self.queue = self.queue_manager.createQueue("DYNAMIC", "PRIORITY")
        except SynergyError as ex:
            LOG.error("Exception has occured", exc_info=1)
            LOG.error(ex)

        self.queue = self.queue_manager.getQueue("DYNAMIC")

        for project in self.project_manager.getProjects():
            project.setQueue(self.queue)

        worker = Worker("DYNAMIC",
                        self.queue,
                        self.project_manager,
                        self.nova_manager,
                        self.keystone_manager,
                        self.backfill_depth)

        self.workers.append(worker)

        self.quota_manager.deleteExpiredServers()

        self.configured = True

    def destroy(self):
        for queue_worker in self.workers:
            queue_worker.destroy()

    def doOnEvent(self, event_type, *args, **kwargs):
        if event_type == "SERVER_EVENT":
            server = kwargs["server"]
            event = kwargs["event"]
            state = kwargs["state"]

            self._processServerEvent(server, event, state)
        elif event_type == "SERVER_CREATE":

            self._processServerCreate(kwargs["request"])

        elif event_type == "PROJECT_ADDED":
            if not self.configured:
                return

            project = kwargs.get("project", None)

            if self.queue and project:
                project.setQueue(self.queue)

        elif event_type == "USER_PRIORITY_UPDATED":
            if self.queue:
                self.queue.updatePriority(kwargs.get("user", None))

        elif event_type == "PROJECT_DONE":
            for worker in self.workers:
                worker.start()

    def _processServerEvent(self, server, event, state):
        project = self.project_manager.getProject(id=server.getProjectId())

        if not project:
            return

        if event == "compute.instance.create.end" and state == "active":
            LOG.info("the server %s is now active on host %s"
                     % (server.getId(), server.getHost()))

            now = datetime.now()
            expiration = now + timedelta(minutes=project.getTTL())
            expiration = time.mktime(expiration.timetuple())
            expiration = str(expiration)[:-2]

            self.nova_manager.setServerMetadata(server,
                                                "expiration_time",
                                                expiration)
        else:
            quota = project.getQuota()

            if event == "compute.instance.delete.end":
                LOG.info("the server %s has been deleted on host %s"
                         % (server.getId(), server.getHost()))
                try:
                    quota.release(server)
                except Exception as ex:
                    LOG.warn("cannot release server %s "
                             "(reason=%s)" % (server.getId(), ex))
            elif state == "error":
                if not server.getTerminatedAt() and not server.getDeletedAt():
                    try:
                        LOG.info("error occurred on server %s (host %s)"
                                 % (server.getId(), server.getHost()))

                        self.nova_manager.deleteServer(server)
                    except Exception:
                        pass

    def _processServerCreate(self, request):
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

                    LOG.info("retrying to build the server %s user_id"
                             "=%s prj_id=%s, num_attempts=%s, reason=%s"
                             % (request.getId(), request.getUserId(),
                                request.getProjectId(), num_attempts, reason))
                    return

                self.nova_manager.setQuotaTypeServer(server)

                if server.isPermanent():
                    if quota.allocate(server, blocking=False):
                        LOG.info("new request: id=%s user_id=%s prj_id=%s "
                                 "quota=private" % (request.getId(),
                                                    request.getUserId(),
                                                    request.getProjectId()))

                        self.nova_manager.buildServer(request)
                        LOG.info("building server %s user_id=%s prj_id=%s "
                                 "quota=private" % (server.getId(),
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
                    user = project.getUser(id=request.getUserId())
                    priority = user.getPriority().getValue()

                    self.queue.enqueue(user, request.toDict())

                    LOG.info("new request: id=%s user_id=%s prj_id=%s priority"
                             "=%s quota=shared" % (request.getId(),
                                                   request.getUserId(),
                                                   request.getProjectId(),
                                                   priority))
            else:
                self.nova_manager.buildServer(request)
        except Exception as ex:
            LOG.error("Exception has occured", exc_info=1)
            LOG.error(ex)
