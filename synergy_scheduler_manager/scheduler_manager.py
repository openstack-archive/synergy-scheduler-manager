import logging
import re
import threading

from datetime import datetime

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
LOG = logging.getLogger(__name__)


class Notifications(object):

    def __init__(self, dynamic_quota):
        super(Notifications, self).__init__()

        self.dynamic_quota = dynamic_quota

    def info(self, event_type, payload):
        LOG.debug("Notification INFO: event_type=%s payload=%s"
                  % (event_type, payload))

        if payload is None or "state" not in payload:
            return

        state = payload["state"]
        instance_id = payload["instance_id"]

        if ((event_type == "compute.instance.delete.end" and
            (state == "deleted" or state == "error" or state == "building")) or
            (event_type == "compute.instance.update" and
            (state == "deleted" or state == "error")) or
                (event_type == "scheduler.run_instance" and state == "error")):
            ram = 0
            cores = 0
            prj_id = None
            instance_info = None

            if event_type == "scheduler.run_instance":
                instance_info = payload["request_spec"]["instance_type"]
            else:
                instance_info = payload

            prj_id = instance_info["tenant_id"]
            instance_id = instance_info["instance_id"]
            ram = instance_info["memory_mb"]
            cores = instance_info["vcpus"]
            # disk = instance_info["root_gb"]
            # node = instance_info["node"]

            LOG.debug("Notification INFO (type=%s state=%s): cores=%s ram=%s "
                      "prj_id=%s instance_id=%s"
                      % (event_type, state, cores, ram, prj_id, instance_id))

            try:
                self.dynamic_quota.release(instance_id, prj_id, cores, ram)
            except Exception as ex:
                LOG.warn("Notification INFO: %s" % ex)

    def warn(self, event_type, payload):
        state = payload["state"]
        instance_id = payload["instance_id"]
        LOG.info("Notification WARN: event_type=%s state=%s instance_id=%s"
                 % (event_type, state, instance_id))

    def error(self, event_type, payload, metadata):
        LOG.info("Notification ERROR: event_type=%s payload=%s metadata=%s"
                 % (event_type, payload, metadata))


class Worker(threading.Thread):

    def __init__(self, name, queue, quota, nova_manager):
        super(Worker, self).__init__()
        self.setDaemon(True)

        self.name = name
        self.queue = queue
        self.quota = quota
        self.nova_manager = nova_manager
        self.exit = False
        LOG.info("Worker %r created!" % self.name)

    def getName(self):
        return self.name

    def destroy(self):
        try:
            # if self.queue:
            self.queue.close()

            self.exit = True
        except Exception as ex:
            LOG.error(ex)
            raise ex

    def run(self):
        LOG.info("Worker %r running!" % self.name)

        while not self.exit and not self.queue.isClosed():
            try:
                queue_item = self.queue.getItem()
            except Exception as ex:
                LOG.error("Worker %r: %s" % (self.name, ex))
                # self.exit = True
                # break
                continue

            if queue_item is None:
                continue

            try:
                request = queue_item.getData()

                instance = request["instance"]
                # user_id = instance["nova_object.data"]["user_id"]
                prj_id = instance["nova_object.data"]["project_id"]
                uuid = instance["nova_object.data"]["uuid"]
                vcpus = instance["nova_object.data"]["vcpus"]
                memory_mb = instance["nova_object.data"]["memory_mb"]
                context = request["context"]
                filter_properties = request["filter_properties"]
                admin_password = request["admin_password"]
                injected_files = request["injected_files"]
                requested_networks = request["requested_networks"]
                security_groups = request["security_groups"]
                block_device_mapping = request["block_device_mapping"]
                legacy_bdm = request["legacy_bdm"]
                image = request["image"]

                try:
                    # vm_instance = self.novaConductorAPI.instance_get_by_uuid
                    # (context, instance_uuid=instance_uuid)
                    server = self.nova_manager.execute("GET_SERVER",
                                                       id=uuid)
                except Exception as ex:
                    LOG.warn("Worker %s: server %r not found! reason=%s"
                             % (self.name, uuid, ex))
                    self.queue.deleteItem(queue_item)

                    self.quota.release(instance_id=uuid,
                                       prj_id=prj_id,
                                       cores=vcpus,
                                       ram=memory_mb)
                    continue

                if server["OS-EXT-STS:vm_state"] != "building" or \
                   server["OS-EXT-STS:task_state"] != "scheduling":
                    self.queue.deleteItem(queue_item)

                    self.quota.release(instance_id=uuid,
                                       prj_id=prj_id,
                                       cores=vcpus,
                                       ram=memory_mb)
                    continue

                # LOG.info(request_spec)

                # if (self.quota.reserve(instance_uuid, vcpus, memory_mb)):
                # done = False

                if self.quota.allocate(instance_id=uuid,
                                       prj_id=prj_id,
                                       cores=vcpus,
                                       ram=memory_mb,
                                       blocking=True):
                    try:
                        self.nova_manager.execute(
                            "BUILD_SERVER",
                            context=context,
                            instance=instance,
                            image=image,
                            filter_properties=filter_properties,
                            admin_password=admin_password,
                            injected_files=injected_files,
                            requested_networks=requested_networks,
                            security_groups=security_groups,
                            block_device_mapping=block_device_mapping,
                            legacy_bdm=legacy_bdm)

                        LOG.info("Worker %r: server (instance_id=%s) build OK"
                                 % (self.name, uuid))
                    except Exception as ex:
                        LOG.error("Worker %r: error on building the server "
                                  "(instance_id=%s) reason=%s"
                                  % (self.name, uuid, ex))

                        self.quota.release(instance_id=uuid,
                                           prj_id=prj_id,
                                           cores=vcpus,
                                           ram=memory_mb)

                self.queue.deleteItem(queue_item)
            except Exception as ex:
                LOG.error("Worker '%s': %s" % (self.name, ex))
                # self.queue.reinsertItem(queue_item)

                continue

            # LOG.info("Worker done is %s" % done)

        # LOG.info(">>>> Worker '%s' queue.isClosed %s exit=%s"
        # % (self.name, self.queue.isClosed(), self.exit))
        LOG.info("Worker '%s' destroyed!" % self.name)


class SchedulerManager(Manager):

    def __init__(self):
        Manager.__init__(self, name="SchedulerManager")

        self.config_opts = [
            cfg.FloatOpt('default_TTL', default=10.0),
            cfg.ListOpt("projects", default=[], help="the projects list"),
            cfg.ListOpt("shares", default=[], help="the shares list"),
            cfg.ListOpt("TTLs", default=[], help="the TTLs list"),
        ]

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
        self.fairshare_manager = self.getManager("FairShareManager")
        self.projects = {}
        self.workers = []
        self.listener = None
        self.exit = False

        try:
            self.dynamic_quota = self.quota_manager.execute(
                "GET_DYNAMIC_QUOTA")

            k_projects = self.keystone_manager.execute("GET_PROJECTS")

            for k_project in k_projects:
                prj_id = str(k_project["id"])
                prj_name = str(k_project["name"])

                if prj_name in CONF.SchedulerManager.projects:
                    CONF.SchedulerManager.projects.remove(prj_name)

                    self.projects[prj_name] = {"id": prj_id,
                                               "name": prj_name,
                                               "type": "dynamic",
                                               "share": float(0),
                                               "TTL": self.default_TTL}

            if len(CONF.SchedulerManager.projects) > 0:
                raise Exception("projects %s not found"
                                % CONF.SchedulerManager.projects)

            for prj_ttl in CONF.SchedulerManager.TTLs:
                prj_name, TTL = self.parseAttribute(prj_ttl)
                self.projects[prj_name]["TTL"] = TTL

            for prj_share in CONF.SchedulerManager.shares:
                prj_name, share = self.parseAttribute(prj_share)
                self.projects[prj_name]["share"] = share

            for project in self.projects.values():
                prj_id = project["id"]
                prj_name = project["name"]
                prj_share = project["share"]

                del self.projects[prj_name]
                self.projects[prj_id] = project

                quota = {"cores": -1, "ram": -1, "instances": -1}

                self.nova_manager.execute("UPDATE_QUOTA",
                                          id=prj_id,
                                          data=quota)

                self.quota_manager.execute("ADD_PROJECT",
                                           prj_id=prj_id,
                                           prj_name=prj_name)

                self.fairshare_manager.execute("ADD_PROJECT",
                                               prj_id=prj_id,
                                               prj_name=prj_name,
                                               share=prj_share)
            try:
                self.dynamic_queue = self.queue_manager.execute("CREATE_QUEUE",
                                                                name="DYNAMIC")
            except Exception as ex:
                LOG.error("Exception has occured", exc_info=1)
                LOG.error(ex)

            self.dynamic_queue = self.queue_manager.execute("GET_QUEUE",
                                                            name="DYNAMIC")

            dynamic_worker = Worker(name="DYNAMIC",
                                    queue=self.dynamic_queue,
                                    quota=self.dynamic_quota,
                                    nova_manager=self.nova_manager)
            dynamic_worker.start()

            self.workers.append(dynamic_worker)

            print(self.projects)
            print(self.dynamic_quota.toDict())

        except Exception as ex:
            LOG.error("Exception has occured", exc_info=1)
            LOG.error(ex)
            raise ex

    def parseAttribute(self, attribute):
        if attribute is None:
            return None

        prj_name = None
        value = float(0)

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
        if command == "PROCESS_REQUEST":
            return self.processRequest(*args, **kargs)
        else:
            raise Exception("command=%r not supported!" % command)

    def task(self):
        if self.listener is None:
            self.notifications = Notifications(self.dynamic_quota)

            target = self.nova_manager.execute("GET_TARGET",
                                               topic='notifications',
                                               exchange="nova")
            self.listener = self.nova_manager.execute(
                "GET_NOTIFICATION_LISTENER",
                targets=[target],
                endpoints=[self.notifications])

            LOG.info("listener created")

            self.listener.start()
        for prj_id, project in self.dynamic_quota.getProjects().items():
            instances = project["instances"]["active"]
            TTL = self.projects[prj_id]["TTL"]
            uuids = self.nova_manager.execute("GET_EXPIRED_SERVERS",
                                              prj_id=prj_id,
                                              instances=instances,
                                              TTL=TTL)

            for uuid in uuids:
                LOG.info("deleting the expired instance %r from project=%s"
                         % (uuid, prj_id))
                self.nova_manager.execute("DELETE_SERVER", id=uuid)

    def destroy(self):
        if self.workers:
            for queue_worker in self.workers:
                queue_worker.destroy()

    def processRequest(self, request):
        try:
            filter_properties = request["filter_properties"]
            instance = request["instance"]
            user_id = instance["nova_object.data"]["user_id"]
            prj_id = instance["nova_object.data"]["project_id"]
            uuid = instance["nova_object.data"]["uuid"]
            vcpus = instance["nova_object.data"]["vcpus"]
            memory_mb = instance["nova_object.data"]["memory_mb"]

            if prj_id in self.projects:
                # prj_name = self.projects[prj_id]["name"]
                # metadata = instance["nova_object.data"]["metadata"]
                timestamp = instance["nova_object.data"]["created_at"]
                timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
                priority = 0

                try:
                    if "retry" in filter_properties:
                        retry = filter_properties["retry"]
                        num_attempts = retry["num_attempts"]

                        if num_attempts > 0:
                            self.dynamic_quota.release(instance_id=uuid,
                                                       prj_id=prj_id,
                                                       cores=vcpus,
                                                       ram=memory_mb)
                            priority = 999999999999
                            LOG.info("released resource uuid %s "
                                     "num_attempts %s" % (uuid, num_attempts))
                except Exception as ex:
                    LOG.error("Exception has occured", exc_info=1)
                    LOG.error(ex)

                if priority == 0:
                    priority = self.fairshare_manager.execute(
                        "CALCULATE_PRIORITY",
                        user_id=user_id,
                        prj_id=prj_id,
                        timestamp=timestamp,
                        retry=0)

                self.dynamic_queue.insertItem(user_id,
                                              prj_id,
                                              priority=priority,
                                              data=request)

                LOG.info("new request: instance_id=%s user_id=%s prj_id=%s "
                         "priority=%s type=dynamic" % (uuid, user_id,
                                                       prj_id, priority))

            else:
                context = request["context"]
                admin_password = request["admin_password"]
                injected_files = request["injected_files"]
                requested_networks = request["requested_networks"]
                security_groups = request["security_groups"]
                block_device_mapping = request["block_device_mapping"]
                legacy_bdm = request["legacy_bdm"]
                image = request["image"]

                self.nova_manager.execute(
                    "BUILD_SERVER",
                    context=context,
                    instance=instance,
                    image=image,
                    filter_properties=filter_properties,
                    admin_password=admin_password,
                    injected_files=injected_files,
                    requested_networks=requested_networks,
                    security_groups=security_groups,
                    block_device_mapping=block_device_mapping,
                    legacy_bdm=legacy_bdm)

                LOG.info("new request: instance_id=%s user_id=%s "
                         "prj_id=%s type=static" % (uuid, user_id, prj_id))
        except Exception as ex:
            LOG.error("Exception has occured", exc_info=1)
            LOG.error(ex)
