# Copyright (c) 2014 INFN - "Istituto Nazionale di Fisica Nucleare" - Italy

# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

__author__ = "Lisa Zangrando"
__email__ = "lisa.zangrando[AT]pd.infn.it"

import MySQLdb
import threading
import traceback
import os
import sys
import json
import re
import time
import eventlet
import ConfigParser

from datetime import datetime, timedelta
try:
    from oslo_config import cfg
except ImportError:
    from oslo.config import cfg
from oslo import messaging
#from keystoneclient.v2_0 import client

from synergy.common import manager
from synergy.common import rpc
from synergy.common import serializer
from synergy.common import context
from synergy.common.client import keystone_v3
from synergy.openstack.common import log as logging
from synergy.openstack.common import jsonutils
from nova.baserpc import BaseAPI
from nova.conductor.rpcapi import ConductorAPI, ComputeTaskAPI
from nova.scheduler.rpcapi import SchedulerAPI
from nova.compute.rpcapi import ComputeAPI

CONF = cfg.CONF
CONFIG = ConfigParser.SafeConfigParser()

LOG = logging.getLogger(__name__)


class NovaBaseAPI(BaseAPI):

    def __init__(self, topic):
        LOG.info("creating RPC server for topic '%s'" % topic)
        self.target = messaging.Target(
            topic=topic, namespace="baseapi", version="1.0")

        LOG.info("creating RPC client for topic '%s'" % (topic + "_synergy"))
        self.client = rpc.getClient(
            target=messaging.Target(
                topic=topic + "_synergy",
                namespace="baseapi",
                version="1.0"))


class NovaComputeAPI(ComputeAPI):

    def __init__(self, topic):
        LOG.info("creating RPC server for topic '%s'" % topic)
        self.target = messaging.Target(topic=topic, version="3.0")

        LOG.info("creating RPC client for topic '%s'" % (topic))
        self.client = rpc.getClient(target=messaging.Target(topic=topic))


class NovaConductorAPI(ConductorAPI):

    def __init__(self, topic):
        report_interval = cfg.IntOpt(
            'report_interval',
            default=10,
            help='Seconds between nodes reporting state to datastore')
        cfg.CONF.register_opt(report_interval)

        LOG.info("creating RPC server for topic '%s'" % topic)
        self.target = messaging.Target(
            topic=topic, namespace=None, version="2.0")

        LOG.info("creating RPC client for topic '%s'" % (topic + "_synergy"))
        self.client = rpc.getClient(
            target=messaging.Target(
                topic=topic + "_synergy",
                version="2.0"),
            version_cap="2.0")

    def instance_get_by_uuid(self, context, instance_uuid,
                             columns_to_join=None):
        cctxt = self.client.prepare()
        return cctxt.call(context, 'instance_get_by_uuid',
                          instance_uuid=instance_uuid,
                          columns_to_join=columns_to_join)

    def service_get_all_by(self, context, topic=None, host=None, binary=None):
        cctxt = self.client.prepare()
        return cctxt.call(context, 'service_get_all_by',
                          topic=topic, host=host, binary=binary)

    def service_create(self, context, values):
        cctxt = self.client.prepare()
        return cctxt.call(context, 'service_create', values=values)

    def object_class_action(self, context, objname, objmethod, objver, args,
                            kwargs):
        cctxt = self.client.prepare()
        return cctxt.call(context, 'object_class_action', objname=objname,
                          objmethod=objmethod, objver=objver, args=args,
                          kwargs=kwargs)


class NovaConductorAPI_FIX(ConductorAPI):

    def __init__(self, topic):
        report_interval = cfg.IntOpt(
            'report_interval',
            default=10,
            help='Seconds between nodes reporting state to datastore')
        cfg.CONF.register_opt(report_interval)

        LOG.info("creating RPC server for topic '%s'" % topic)
        self.target = messaging.Target(
            topic=topic, namespace=None, version="1.9")

        LOG.info("creating RPC client for topic '%s'" % (topic + "_synergy"))
        self.client = rpc.getClient(
            target=messaging.Target(
                topic=topic + "_synergy",
                version="2.0"),
            version_cap="2.0")

    def object_class_action(self, context, objname, objmethod, objver, args,
                            kwargs):
        cctxt = self.client.prepare()
        return cctxt.call(context, 'object_class_action', objname=objname,
                          objmethod=objmethod, objver=objver, args=args,
                          kwargs=kwargs)


class NovaConductorComputeAPI(ComputeTaskAPI):

    def __init__(self, topic, processRequest, keystone_client):
        self.processRequest = processRequest
        self.keystone_client = keystone_client

        LOG.info("creating RPC server for topic '%s'" % topic)
        self.target = messaging.Target(
            topic=topic, namespace='compute_task', version="1.9")

        LOG.info("creating RPC client for topic '%s'" % (topic + "_synergy"))
        self.client = rpc.getClient(
            target=messaging.Target(
                topic=topic + "_synergy",
                namespace='compute_task',
                version="1.9"))

    """ nova-conductor rpc operations """

    def build_instances(self, context, instances, image, filter_properties,
                        admin_password, injected_files, requested_networks,
                        security_groups, block_device_mapping=None,
                        legacy_bdm=True):
        """
        LOG.info(">>>>>>>>>>>>>>>>> build_instance context %s" % context)
        token = self.keystone_client.validateToken(context["auth_token"])


        LOG.info("token id=%s" % token.getId())
        LOG.info("token is admin=%s" % token.isAdmin())
        LOG.info("token expiration=%s" % token.getExpiration())
        LOG.info("token is expired=%s" % token.isExpired())
        LOG.info("token issued at=%s" % token.issuedAt())
        LOG.info("token project=%s" % token.getProject())
        LOG.info("token user=%s" % token.getUser())
        LOG.info("token roles=%s" % token.getRoles())
        LOG.info("token catalog=%s" % token.getCatalog())
        LOG.info("token extras=%s" % token.getExtras())
        """

        for instance in instances:
            request = {'instance': instance,
                       'image': jsonutils.to_primitive(image),
                       'filter_properties': filter_properties,
                       'admin_password': admin_password,
                       'injected_files': injected_files,
                       'requested_networks': requested_networks,
                       'security_groups': security_groups,
                       'block_device_mapping': block_device_mapping,
                       'legacy_bdm': legacy_bdm,
                       'context': context}

            self.processRequest(request)

    def build_instance(self, context, instance, image, filter_properties,
                       admin_password, injected_files, requested_networks,
                       security_groups, block_device_mapping=None,
                       legacy_bdm=True):
        try:
            kw = {'instances': [instance],
                  'image': jsonutils.to_primitive(image),
                  'filter_properties': filter_properties,
                  'admin_password': admin_password,
                  'injected_files': injected_files,
                  'requested_networks': requested_networks,
                  'security_groups': security_groups}

            version = '1.9'
            if not self.client.can_send_version('1.9'):
                version = '1.8'
                kw['requested_networks'] = kw['requested_networks'].as_tuples()
            if not self.client.can_send_version('1.7'):
                version = '1.5'
                kw.update({
                    'block_device_mapping': block_device_mapping,
                    'legacy_bdm': legacy_bdm})

            cctxt = self.client.prepare(version_cap=version)
            cctxt.cast(context, 'build_instances', **kw)
        except Exception as ex:
            LOG.error(ex)
            raise ex

    def object_class_action(self, context, objname, objmethod, objver, args,
                            kwargs):
        #LOG.info("NNNNNNNNNNNNNNNNNNNNNN object_class_action")
        client = rpc.getClient(
            target=messaging.Target(
                topic=topic + "_synergy",
                version="2.0"),
            version_cap="2.0")
        cctxt = client.prepare()
        return cctxt.call(context, 'object_class_action', objname=objname,
                          objmethod=objmethod, objver=objver, args=args,
                          kwargs=kwargs)


class QueueWorker(threading.Thread):

    def __init__(self, name, queue, quota_manager, novaComputeAPI,
                 novaConductorAPI, novaConductorComputeAPI):
        super(QueueWorker, self).__init__()
        self.setDaemon(True)

        self.name = name
        self.queue = queue
        self.quota_manager = quota_manager
        self.novaComputeAPI = novaComputeAPI
        self.novaConductorAPI = novaConductorAPI
        self.novaConductorComputeAPI = novaConductorComputeAPI
        self.exit = False
        LOG.info("QueueWorker '%s' created!" % self.name)

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
        LOG.info("QueueWorker '%s' running!" % self.name)

        while not self.exit and not self.queue.isClosed():
            try:
                queue_item = self.queue.getItem()
            except Exception as ex:
                LOG.error("QueueWorker '%s': %s" % (self.name, ex))
                #self.exit = True
                # break
                continue

            if queue_item is None:
                continue

            try:
                request = queue_item.getData()
                """
                request = { 'instance': instance,
                        'image': jsonutils.to_primitive(image),
                        'filter_properties': filter_properties,
                        'admin_password': admin_password,
                        'injected_files': injected_files,
                        'requested_networks': requested_networks,
                        'security_groups': security_groups,
                        'block_device_mapping': block_device_mapping,
                        'legacy_bdm': legacy_bdm,
                        'context': context }
                """

                #LOG.info("XXXXXXXXXXXXXXXXXXXXXXXXX request=%s" % (request))
                #ctxt = context.RequestContext.fromDict(request["context"])
                context = request["context"]
                instance_type = request["filter_properties"]["instance_type"]
                vcpus = instance_type["vcpus"]
                memory = instance_type["memory_mb"]
                disk = instance_type["root_gb"]
                instance = request["instance"]["nova_object.data"]
                user_id = instance["user_id"]
                project_id = instance["project_id"]
                instance_uuid = instance["uuid"]
                timestamp = instance["created_at"]

                try:
                    vm_instance = self.novaConductorAPI.instance_get_by_uuid(
                        context, instance_uuid=instance_uuid)
                except Exception as ex:
                    LOG.warn(
                        "QueueWorker %s: instance %s not found!" %
                        (self.name, instance_uuid))
                    self.queue.deleteItem(queue_item)
                    continue

                if vm_instance["vm_state"] != "building" or vm_instance[
                        "task_state"] != "scheduling":
                    self.queue.deleteItem(queue_item)
                    continue

                # LOG.info(request_spec)

                # if (self.quota.reserve(instance_uuid, vcpus, memory)):
                done = False
                quota = self.quota_manager.getQuota(project_id=project_id)

                blocking = not quota.isStaticType()

                if quota.allocate(instance_uuid, vcpus, memory, blocking):
                    try:
                        self.novaConductorComputeAPI.build_instance(
                            context=context,
                            instance=request["instance"],
                            image=request["image"],
                            filter_properties=request["filter_properties"],
                            admin_password=request["admin_password"],
                            injected_files=request["injected_files"],
                            requested_networks=request["requested_networks"],
                            security_groups=request["security_groups"],
                            block_device_mapping=request[
                                "block_device_mapping"],
                            legacy_bdm=request["legacy_bdm"])

                        LOG.info("QueueWorker '%s' build_instance OK:"
                                 " instace_id=%s" % (self.name, instance_uuid))
                    except Exception as ex:
                        LOG.error("QueueWorker '%s' build_instance ERROR:"
                                  "instace_id=%s reason=%s" %
                                  (self.name, instance_uuid, ex))
                        quota.release(instance_uuid, vcpus, memory)

                self.queue.deleteItem(queue_item)

            except Exception as ex:
                LOG.error("QueueWorker '%s': %s" % (self.name, ex))
                # self.queue.reinsertItem(queue_item)

                continue

            #LOG.info("QueueWorker done is %s" % done)

        LOG.info("QueueWorker '%s' destroyed!" % self.name)


class FairShareManager(manager.Manager):

    def __init__(self):
        super(
            FairShareManager,
            self).__init__(
            name="FairShareManager",
            autostart=True,
            rate=60)

        eventlet.monkey_patch()

        self.config_opts = [
            cfg.IntOpt('num_of_periods', default=3),
            cfg.IntOpt('period_length', default=7),
            cfg.FloatOpt('default_project_share', default=10.0),
            cfg.FloatOpt('default_runtime_limit', default=10.0),
            #cfg.StrOpt('user_shares', default=None),
            cfg.FloatOpt('decay_weight', default=0.5, help="the decay weight"),
            cfg.IntOpt('age_weight', default=1000, help="the age weight"),
            cfg.IntOpt('vcpus_weight', default=10000, help="the vcpus weight"),
            cfg.IntOpt(
                'memory_weight',
                default=7000,
                help="the memory weight"),
            #cfg.ListOpt("projects", default = [], help = "the projects list"),
            cfg.ListOpt("shares", default=[], help="the shares list"),
            cfg.ListOpt(
                "dynamic_quota",
                default=[],
                help="the projects list with dynamic quota"),
            cfg.ListOpt(
                "extended_quota",
                default=[],
                help="the projects list with extended quota")
        ]

    def execute(self, cmd):

        def makeProjectInfo(project_id):
            project_info = None

            if project_id in self.usage_table:
                project = self.usage_table.get(project_id)
                project_info = {
                    "name": project["name"],
                    "share": project["share"],
                    "actual_vcpus_usage": project["actual_vcpus_usage"],
                    "actual_memory_usage": project["actual_memory_usage"],
                    "users": {}}

                for user_id, user in project.get("users", {}).items():
                    priority = self.calculatePriority(user_id, project_id)
                    relative_vcpus_usage = 0
                    relative_memory_usage = 0
                    relative_share = 0

                    if project["sibling_share"] > 0:
                        relative_share = user["share"] / project["sibling_share"]

                    if project["actual_vcpus_usage"] > 0:
                        relative_vcpus_usage = user["normalized_vcpus_usage"] / project["actual_vcpus_usage"]

                    if project["actual_memory_usage"] > 0:
                        relative_memory_usage = user["normalized_memory_usage"] / project["actual_memory_usage"]

                    project_info["users"][user_id] = {
                        "name": user["name"],
                        "share": user["share"],
                        "relative_share": relative_share,
                        "absolute_share": user["normalized_share"],
                        "priority": priority,
                        "normalized_vcpus_usage":
                            user["normalized_vcpus_usage"],
                        "normalized_memory_usage":
                            user["normalized_memory_usage"],
                        "relative_vcpus_usage": relative_vcpus_usage,
                        "relative_memory_usage": relative_memory_usage}

            return project_info

        if cmd.getName() == "get_priority":
            project_ids = cmd.getParameter("project_id")
            project_names = cmd.getParameter("project_name")
            user_list = cmd.getParameter("users")
            project_list = None

            if project_ids:
                if isinstance(project_ids, str):
                    project_list = [project_ids]
                elif isinstance(project_ids, list):
                    project_list = project_ids

                for project_id in project_list:
                    project_info = makeProjectInfo(project_id)

                    if project_info:
                        cmd.addResult(project_id, project_info)

                if len(project_list) == 1 and len(cmd.getResults()) == 0:
                    raise Exception(
                        "project id='%s' not found!" %
                        project_list[0])

            elif project_names:
                if isinstance(project_names, str):
                    project_list = [project_names]
                elif isinstance(project_names, list):
                    project_list = project_names

                for project_id, project in self.usage_table.items():
                    if project["name"] in project_list:
                        cmd.addResult(project_id, makeProjectInfo(project_id))

                if len(project_list) == 1 and len(cmd.getResults()) == 0:
                    raise Exception(
                        "project name='%s' not found!" %
                        project_list[0])

            else:
                for project_id in self.usage_table.keys():
                    cmd.addResult(project_id, makeProjectInfo(project_id))
        else:
            raise Exception("command '%s' not supported!" % cmd.getName())

    def task(self):
        try:
            for project in self.extended_projects:
                self.calculateFairShares([project])

            self.calculateFairShares(self.dynamic_projects)

            # self.printTable()
            # for project_id, project in self.usage_table.items():
            #    LOG.info(project)

        except Exception as ex:
            LOG.error(ex)
            raise ex

    def setup(self):
        LOG.info("%s setup invoked!" % (self.name))

        self.keystone_admin_user = CONF.Keystone.admin_user
        self.keystone_admin_password = CONF.Keystone.admin_password
        self.keystone_admin_project_name = CONF.Keystone.admin_project_name
        self.keystone_auth_url = CONF.Keystone.auth_url
        self.mysql_host = CONF.MYSQL.host
        self.mysql_user = CONF.MYSQL.user
        self.mysql_password = CONF.MYSQL.password
        self.num_of_periods = CONF.FairShareManager.num_of_periods
        self.period_length = CONF.FairShareManager.period_length
        self.default_project_share = CONF.FairShareManager.default_project_share
        self.decay_weight = CONF.FairShareManager.decay_weight
        self.vcpus_weight = CONF.FairShareManager.vcpus_weight
        self.age_weight = CONF.FairShareManager.age_weight
        self.memory_weight = CONF.FairShareManager.memory_weight
        #self.project_shares = {}
        self.projects = {}  # CONF.FairShareManager.projects
        self.dynamic_projects = []
        self.extended_projects = []
        self.shares = CONF.FairShareManager.shares
        self.dynamic_quota = CONF.FairShareManager.dynamic_quota
        self.extended_quota = CONF.FairShareManager.extended_quota
        self.default_runtime_limit = CONF.FairShareManager.default_runtime_limit

        self.lock = threading.Lock()
        self.usage_table = {}
        self.exit = False

        self.queue_manager = self.getDependence("QueueManager")
        if not self.queue_manager:
            raise Exception("QueueManager not found!")

        self.quota_manager = self.getDependence("QuotaManager")
        if not self.quota_manager:
            raise Exception("QuotaManager not found!")

        try:
            self.keystone_client = keystone_v3.KeystoneClient(
                auth_url=self.keystone_auth_url,
                username=self.keystone_admin_user,
                password=self.keystone_admin_password,
                project_name=self.keystone_admin_project_name)

            rpc.init(CONF)

            endpoints = [self]
            conductor_topic = "conductor"
            compute_topic = "compute"
            scheduler_topic = "scheduler"
            server = None
            self.client = None

            nova_cfg_file = cfg.find_config_files(
                project="nova", extension='.conf')
            CONFIG.read(nova_cfg_file)

            if len(nova_cfg_file) > 0 and os.path.isfile(nova_cfg_file[0]):

                try:
                    server = CONFIG.get("DEFAULT", "my_ip")
                except Exception as xxx_todo_changeme:
                    ConfigParser.NoOptionError = xxx_todo_changeme
                    raise Exception(
                        "No option 'my_ip' found in %s: using default '%s'" %
                        (nova_cfg_file, server))

                try:
                    conductor_topic = CONFIG.get("DEFAULT", "conductor_topic")
                except Exception as xxx_todo_changeme1:
                    ConfigParser.NoOptionError = xxx_todo_changeme1
                    LOG.info(
                        "No option 'conductor_topic' found in %s: using default '%s'" %
                        (nova_cfg_file, conductor_topic))

                try:
                    compute_topic = CONFIG.get("DEFAULT", "compute_topic")
                except Exception as xxx_todo_changeme2:
                    ConfigParser.NoOptionError = xxx_todo_changeme2
                    LOG.info(
                        "No option 'compute_topic' found in %s: using default '%s'" %
                        (nova_cfg_file, compute_topic))

                try:
                    scheduler_topic = CONFIG.get("DEFAULT", "scheduler_topic")
                except Exception as xxx_todo_changeme3:
                    ConfigParser.NoOptionError = xxx_todo_changeme3
                    LOG.info(
                        "No option 'scheduler_topic' found in %s: using default '%s'" %
                        (nova_cfg_file, scheduler_topic))
            else:
                raise Exception("nova configuration file not found!!!")

            #self.novaSchedulerAPI = NovaSchedulerAPI(conductor_topic, server)

            self.novaBaseAPI = NovaBaseAPI(conductor_topic)
            self.novaConductorAPI = NovaConductorAPI(conductor_topic)
            #self.novaConductorAPI_FIX = NovaConductorAPI_FIX(conductor_topic)
            self.novaConductorComputeAPI = NovaConductorComputeAPI(
                conductor_topic, self.processRequest, self.keystone_client)

            self.rpcserver = rpc.getServer(
                target=messaging.Target(
                    topic=conductor_topic,
                    server=server),
                endpoints=[
                    self.novaBaseAPI,
                    self.novaConductorAPI,
                    self.novaConductorComputeAPI])
            self.rpcserver.start()

            self.novaComputeAPI = NovaComputeAPI(compute_topic)

            self.rpcserver = rpc.getServer(
                target=messaging.Target(
                    topic=compute_topic,
                    server=server),
                endpoints=[
                    self.novaComputeAPI])
            # self.rpcserver.start()

            self.queue_workers = []

            k_projects = self.keystone_client.getProjects()

            LOG.info("processing the extended quota %s" % self.extended_quota)

            for project in self.extended_quota:
                project_name = None
                runtime_limit = self.default_runtime_limit

                parsed_limit = re.split('=', project)

                if len(parsed_limit) > 1:
                    if not parsed_limit[-1].isdigit():
                        raise Exception("wrong runtime limit '%s' found in '%s'!" % (
                            parsed_limit[-1], project))

                    if len(parsed_limit) == 2:
                        project_name = parsed_limit[0]
                        runtime_limit = int(parsed_limit[1])
                    else:
                        raise Exception(
                            "wrong runtime limit definition: '%s'" %
                            project)
                else:
                    project_name = project

                found = False
                for k_project in k_projects:
                    if k_project["name"] == project_name:
                        found = True

                        self.projects[
                            k_project["name"]] = {
                            "name": k_project["name"],
                            "id": k_project["id"],
                            "type": "extended",
                            "share": self.default_project_share,
                            "runtime_limit": runtime_limit,
                            "users": {}}
                        self.extended_projects.append(project_name)

                        quota = self.quota_manager.getQuota(
                            project_id=k_project["id"])
                        quota.setRuntimeLimit(runtime_limit)
                        quota.setType("extended")

                        try:
                            queue = self.queue_manager.createQueue(
                                name=project_name)
                        except Exception as ex:
                            queue = self.queue_manager.getQueue(
                                name=project_name)

                        queue.setPriorityUpdater(self.priorityUpdater)

                        queue_worker = QueueWorker(
                            queue.getName(),
                            queue,
                            self.quota_manager,
                            self.novaComputeAPI,
                            self.novaConductorAPI,
                            self.novaConductorComputeAPI)
                        queue_worker.start()

                        self.queue_workers.append(queue_worker)
                if not found:
                    LOG.warn(
                        "project '%s' not found in Keystone: ignored!" %
                        (project_name))

            LOG.info("processing the dynamic quota %s" % self.dynamic_quota)

            for project in self.dynamic_quota:
                project_name = None
                runtime_limit = self.default_runtime_limit

                parsed_limit = re.split('=', project)

                if len(parsed_limit) > 1:
                    if not parsed_limit[-1].isdigit():
                        raise Exception("wrong runtime limit '%s' found in '%s'!" % (
                            parsed_limit[-1], project))

                    if len(parsed_limit) == 2:
                        project_name = parsed_limit[0]
                        runtime_limit = int(parsed_limit[1])
                    else:
                        raise Exception(
                            "wrong runtime limit definition: '%s'" %
                            project)
                else:
                    project_name = project

                found = False
                for k_project in k_projects:
                    if k_project["name"] == project_name:
                        found = True

                        self.projects[
                            k_project["name"]] = {
                            "name": k_project["name"],
                            "id": k_project["id"],
                            "type": "dynamic",
                            "share": self.default_project_share,
                            "runtime_limit": runtime_limit,
                            "users": {}}
                        self.dynamic_projects.append(project_name)

                        quota = self.quota_manager.getQuota(
                            project_id=k_project["id"])
                        quota.setRuntimeLimit(runtime_limit)
                        quota.setType("dynamic")
                if not found:
                    LOG.warn(
                        "project '%s' not found in Keystone: ignored!" %
                        (project_name))

            for k_project in k_projects:
                if not k_project["name"] in self.dynamic_projects and not k_project[
                        "name"] in self.extended_projects:
                    try:
                        queue = self.queue_manager.createQueue(
                            name=k_project["name"])
                    except Exception as ex:
                        queue = self.queue_manager.getQueue(
                            name=k_project["name"])

                    if queue.getSize() > 0:
                        queue_worker = QueueWorker(
                            queue.getName(),
                            queue,
                            self.quota_manager,
                            self.novaComputeAPI,
                            self.novaConductorAPI,
                            self.novaConductorComputeAPI)
                        queue_worker.start()

                        self.queue_workers.append(queue_worker)

            for share in self.shares:
                user_name = None
                project_name = None
                share_value = None

                parsed_share = re.split('@|=', share)

                if len(parsed_share) > 1:
                    if not parsed_share[-1].isdigit():
                        raise Exception("wrong share value '%s' found in '%s'!" % (
                            parsed_share[-1], share))

                    if len(parsed_share) == 3:
                        user_name = parsed_share[0]
                        project_name = parsed_share[1]
                        share_value = parsed_share[2]
                        #LOG.info(">>>>>>> user_name='%s' project_name='%s' share_value=%s" % (user_name, project_name, share_value))
                    elif len(parsed_share) == 2:
                        project_name = parsed_share[0]
                        share_value = parsed_share[1]
                        #LOG.info(">>>>>>> project_name='%s' share_value=%s" % (project_name, share_value))
                    else:
                        raise Exception("wrong share definition: '%s'" % limit)

                if project_name not in self.projects:
                    LOG.warn(
                        "project '%s' not found in the projects list %s: ignored!" %
                        (project_name, self.projects))
                    continue

                project = self.projects[project_name]

                if user_name:
                    project["users"][user_name] = float(share_value)
                else:
                    project["share"] = float(share_value)

            try:
                dynamic_queue = self.queue_manager.createQueue(name="dynamic")
            except Exception as ex:
                dynamic_queue = self.queue_manager.getQueue(name="dynamic")

            dynamic_queue_worker = QueueWorker(
                dynamic_queue.getName(),
                dynamic_queue,
                self.quota_manager,
                self.novaComputeAPI,
                self.novaConductorAPI,
                self.novaConductorComputeAPI)
            dynamic_queue_worker.start()

            self.queue_workers.append(dynamic_queue_worker)

            """
            try:
                default_queue = self.queue_manager.createQueue(name="default")
            except Exception as ex:
                default_queue = self.queue_manager.getQueue(name="default")

            default_queue_worker = QueueWorker(default_queue.getName(), default_queue, self.quota_manager, self.novaComputeAPI, self.novaConductorAPI, self.novaConductorComputeAPI)
            default_queue_worker.start()

            self.queue_workers.append(default_queue_worker)
            """
            self.task()
        except Exception as ex:
            LOG.error(ex)
            raise ex

    def destroy(self):
        #LOG.info("destroy invoked!")

        if self.queue_workers:
            for queue_worker in self.queue_workers:
                queue_worker.destroy()
                # queue_worker.join()

        # LOG.info("destroyed!")

    def processRequest(self, request):
        LOG.info(">>>>>>>>>>>>>>>>>  processRequest begin")
        try:
            instance = request["instance"]
            user_id = instance["nova_object.data"]["user_id"]
            project_id = instance["nova_object.data"]["project_id"]
            uuid = instance["nova_object.data"]["uuid"]
            #metadata = instance["nova_object.data"]["metadata"]
            timestamp = instance["nova_object.data"]["created_at"]
            project_name = "default"
            timestamp = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ')
            priority = 0

            if project_id in self.usage_table:
                project_name = self.usage_table[project_id]["name"]

                try:
                    filter_properties = request["filter_properties"]
                    if "retry" in filter_properties:
                        num_attempts = filter_properties[
                            "retry"]["num_attempts"]

                        if num_attempts > 0:
                            instance_type = filter_properties["instance_type"]
                            vcpus = instance_type["vcpus"]
                            memory = instance_type["memory_mb"]

                            quota = self.quota_manager.getQuota(
                                project_id=project_id)
                            quota.release(uuid, vcpus, memory)

                            priority = 999999999999
                            LOG.info(
                                "released resource uuid %s num_attempts %s" %
                                (uuid, num_attempts))
                except Exception as ex:
                    LOG.error(ex)

                if priority == 0:
                    priority = self.calculatePriority(
                        user_id, project_id, timestamp)

                if project_name in self.dynamic_projects:
                    queue = self.queue_manager.getQueue("dynamic")
                    queue.insertItem(
                        user_id, project_id, priority=priority, data=request)

                    LOG.info(
                        "new request: instance_id=%s user_id=%s project_id=%s priority=%s queue=%s type=dynamic" %
                        (uuid, user_id, project_id, priority, queue.getName()))

                elif project_name in self.extended_projects:
                    queue = self.queue_manager.getQueue(project_name)
                    queue.insertItem(
                        user_id, project_id, priority=priority, data=request)

                    LOG.info(
                        "new request: instance_id=%s user_id=%s project_id=%s priority=%s queue=%s type=extended" %
                        (uuid, user_id, project_id, priority, queue.getName()))

            else:
                k_project = self.keystone_client.getProject(id=project_id)
                project_name = k_project["name"]
                try:
                    queue = self.queue_manager.createQueue(name=project_name)
                except Exception as ex:
                    queue = self.queue_manager.getQueue(name=project_name)

                found = False

                for queue_worker in self.queue_workers:
                    if queue_worker.getName() == project_name:
                        found = True

                if not found:
                    queue_worker = QueueWorker(
                        queue.getName(),
                        queue,
                        self.quota_manager,
                        self.novaComputeAPI,
                        self.novaConductorAPI,
                        self.novaConductorComputeAPI)
                    queue_worker.start()

                    self.queue_workers.append(queue_worker)

                queue.insertItem(
                    user_id,
                    project_id,
                    priority=priority,
                    data=request)

                LOG.info(
                    "new request: instance_id=%s user_id=%s project_id=%s queue=%s type=static" %
                    (uuid, user_id, project_id, priority, queue.getName()))

        except Exception as ex:
            LOG.error(ex)
        LOG.info(">>>>>>>>>>>>>>>>>  processRequest end")

    def priorityUpdater(self, queue_item):
        return self.calculatePriority(user_id=queue_item.getUserId(),
                                      project_id=queue_item.getProjectId(),
                                      timestamp=queue_item.getCreationTime(),
                                      retry_count=queue_item.getRetryCount())

    def calculatePriority(self, user_id, project_id,
                          timestamp=None, retry_count=0):
        if project_id not in self.usage_table or user_id not in self.usage_table[
                project_id]["users"]:
            raise Exception(
                "user=%s project=%s not found!" %
                (user_id, project_id))

        fair_share_vcpus = 0
        fair_share_memory = 0

        self.lock.acquire()
        try:
            user = self.usage_table[project_id]["users"].get(user_id)
            fair_share_vcpus = user["fair_share_vcpus"]
            fair_share_memory = user["fair_share_memory"]
        finally:
            self.lock.release()

        if not timestamp:
            timestamp = datetime.utcnow()
        """
        elif not isinstance(timestamp, datetime):
            # ISO 8601 extended time format with microseconds
            ISO8601_TIME_FORMAT_SUBSECOND = '%Y-%m-%dT%H:%M:%S.%f'
            timestamp = datetime.strptime(timestamp, ISO8601_TIME_FORMAT_SUBSECOND)
        """
        now = datetime.utcnow()

        diff = (now - timestamp)
        minutes = diff.seconds / 60  # convert days to minutes
        priority = float(self.age_weight) * minutes + float(self.vcpus_weight) * fair_share_vcpus + \
            float(self.memory_weight) * fair_share_memory - \
            float(self.age_weight) * retry_count

        #LOG.info("PRIORITY %s for user_id %s project_id %s" % (priority, user_id, project_id))

        return int(priority)

    def calculateFairShares(self, projects):
        now = datetime.utcnow()

        usage_table = {}
        users = self.keystone_client.getUsers()

        if not users:
            LOG.error("cannot receive the users list from keystone")
            return

        for user in users:
            user_id = user["id"]
            user_name = user["name"]
            user_projects = self.keystone_client.getUserProjects(id=user_id)

            for project in user_projects:
                project_id = project["id"]
                project_name = project["name"]

                if not project_name in projects:
                    continue

                if project_id not in usage_table:
                    usage_table[project_id] = {
                        "name": project_name, "share": float(
                            self.projects[project_name]["share"]), "users": {}}

                users = usage_table[project_id]["users"]

                if user_id not in users:
                    users[user_id] = {
                        "name": user_name,
                        "usage_records": [],
                        "normalized_vcpus_usage": float(0),
                        "normalized_memory_usage": float(0)}

        for x in xrange(self.num_of_periods):
            self.__getUsage(
                usage_table=usage_table,
                fromDate=str(
                    now -
                    timedelta(
                        days=(
                            x *
                            self.period_length))),
                period_length=self.period_length)

        total_memory_usage = []
        total_vcpus_usage = []
        total_historical_vcpus_usage = 0
        total_historical_memory_usage = 0
        total_actual_vcpus_usage = 0
        total_actual_memory_usage = 0
        total_share = 0

        for x in xrange(self.num_of_periods):
            total_memory_usage.append(0)
            total_vcpus_usage.append(0)

        for project_id, project in usage_table.items():
            # check the share for each user and update the usage_record
            users = project["users"]
            project_name = project["name"]
            project_share = project["share"]

            sibling_share = float(0)

            for user_id, user in users.items():
                user_name = user["name"]
                user_share = float(0)
                LOG.info(">>>>>>>>> %s" % user)

                if project_name in self.projects and user_name in self.projects[
                        project_name]["users"]:
                    user_share = self.projects[
                        project_name]["users"][user_name]

                    """
                    if user_share > project_share:
                        user_share = 1
                    """

                    sibling_share += user_share

                elif len(users) == 1:
                    sibling_share = project_share
                    user_share = project_share
                else:
                    if project_share > 0:
                        user_share = 1
                        sibling_share += user_share
                    else:
                        user_share = 0

                user.update({"share": user_share})

                # calculate the total_memory_usage and total_vcpus_usage
                index = 0

                for usage_record in user["usage_records"]:
                    total_vcpus_usage[index] += usage_record["vcpus_usage"]
                    total_memory_usage[index] += usage_record["memory_usage"]
                    index += 1

            project["sibling_share"] = sibling_share
            #project["share"] = project_share
            total_share += project_share

        for x in xrange(self.num_of_periods):
            LOG.info("total_vcpus_usage %s" % total_vcpus_usage)
            decay = self.decay_weight ** x
            total_historical_vcpus_usage += decay * total_vcpus_usage[x]
            total_historical_memory_usage += decay * total_memory_usage[x]

        for project_id, project in usage_table.items():
            sibling_share = project["sibling_share"]
            project_share = project["share"]
            actual_vcpus_usage = 0
            actual_memory_usage = 0

            users = project["users"]

            for user_id, user in users.items():
                # for each user the normalized share is calculated (0 <=
                # user_normalized_share <= 1)
                user_share = float(user["share"])

                if project_share > 0 and sibling_share > 0 and total_share > 0:
                    user_normalized_share = (
                        user_share / sibling_share) * (project_share / total_share)
                else:
                    user_normalized_share = user_share

                user["normalized_share"] = user_normalized_share

                # calculate the normalized_vcpus_usage,
                # normalized_memory_usage, historical_vcpus_usage and the
                # historical_memory_usage
                index = 0
                historical_vcpus_usage = 0
                historical_memory_usage = 0

                for usage_record in user["usage_records"]:
                    decay = self.decay_weight ** index

                    historical_vcpus_usage += decay * \
                        usage_record["vcpus_usage"]
                    historical_memory_usage += decay * \
                        usage_record["memory_usage"]
                    LOG.info(
                        ">>>>>>>>>>>>>> name=%s historical_vcpus_usage=%s" %
                        (user["name"], historical_vcpus_usage))

                    index += 1

                if total_historical_vcpus_usage > 0:
                    user["normalized_vcpus_usage"] = historical_vcpus_usage / \
                        total_historical_vcpus_usage
                else:
                    user["normalized_vcpus_usage"] = historical_vcpus_usage

                if total_historical_memory_usage > 0:
                    user["normalized_memory_usage"] = historical_memory_usage / \
                        total_historical_memory_usage
                else:
                    user["normalized_memory_usage"] = historical_memory_usage

                actual_vcpus_usage += user["normalized_vcpus_usage"]
                actual_memory_usage += user["normalized_memory_usage"]

            project["actual_vcpus_usage"] = actual_vcpus_usage
            project["actual_memory_usage"] = actual_memory_usage

            total_actual_vcpus_usage += actual_vcpus_usage
            total_actual_memory_usage += actual_memory_usage
            LOG.info(
                ">>>>>>>>>>>>>> total_actual_vcpus_usage=%s" %
                total_actual_vcpus_usage)

        for project in usage_table.values():
            actual_vcpus_usage = project["actual_vcpus_usage"]
            actual_memory_usage = project["actual_memory_usage"]
            project_share = project["share"]
            sibling_share = project["sibling_share"]
            users = project["users"]

            #effective_project_vcpus_usage = actual_vcpus_usage + ((total_actual_vcpus_usage - actual_vcpus_usage) * project_share / total_share)
            #effective_project_memory_usage = actual_memory_usage + ((total_actual_memory_usage - actual_memory_usage) * project_share / total_share)
            effective_project_vcpus_usage = actual_vcpus_usage
            effective_project_memory_usage = actual_memory_usage

            project["effective_vcpus_usage"] = effective_project_vcpus_usage
            project["effective_memory_usage"] = effective_project_memory_usage

            for user in users.values():
                share = user["share"]

                if share == 0:
                    user["fair_share_vcpus"] = 0
                    user["fair_share_memory"] = 0
                    user["effective_vcpus_usage"] = 0
                    user["effective_memory_usage"] = 0
                    continue
                else:
                    normalized_share = user["normalized_share"]
                    normalized_vcpus_usage = user["normalized_vcpus_usage"]
                    normalized_memory_usage = user["normalized_memory_usage"]

                    effective_vcpus_usage = normalized_vcpus_usage + \
                        ((effective_project_vcpus_usage -
                          normalized_vcpus_usage) * share / sibling_share)
                    effective_memory_usage = normalized_memory_usage + \
                        ((effective_project_memory_usage -
                          normalized_memory_usage) * share / sibling_share)

                    user["effective_vcpus_usage"] = effective_vcpus_usage
                    user["effective_memory_usage"] = effective_memory_usage
                    LOG.info(
                        ">>>>>>>>>>>>>> actual_vcpus_usage=%s" %
                        actual_vcpus_usage)
                    if actual_vcpus_usage > 0:
                        user[
                            "effective_vcpus_usage_relative"] = normalized_vcpus_usage / actual_vcpus_usage
                    else:
                        user["effective_vcpus_usage_relative"] = 0

                    if actual_memory_usage > 0:
                        user[
                            "effective_memory_usage_relative"] = normalized_memory_usage / actual_memory_usage
                    else:
                        user["effective_memory_usage_relative"] = 0

                    #user["effective_vcpus_usage_relative"] = effective_vcpus_usage / effective_project_vcpus_usage
                    #user["effective_memory_usage_relative"] = effective_memory_usage / effective_project_memory_usage

                    if normalized_share == 0:
                        user["fair_share_vcpus"] = 0
                        user["fair_share_memory"] = 0
                    else:
                        user[
                            "fair_share_vcpus"] = 2 ** (-effective_vcpus_usage / normalized_share)
                        user[
                            "fair_share_memory"] = 2 ** (-effective_memory_usage / normalized_share)
                    # LOG.info(user)
            LOG.info("project %s" % project)

        self.lock.acquire()
        try:
            # self.usage_table.clear()
            self.usage_table.update(usage_table)
        finally:
            self.lock.release()

    def __getUsage(self, usage_table, fromDate, period_length):
        #LOG.info("getUsage: fromDate=%s period_length=%s days" % (fromDate, period_length))
        #print("getUsage: fromDate=%s period_length=%s days" % (fromDate, period_length))

        if len(usage_table.keys()) == 0:
            return

        conn = MySQLdb.connect(
            self.mysql_host,
            self.mysql_user,
            self.mysql_password)
        cursor = conn.cursor()
        period = str(period_length)

        try:
            project_ids = "("
            for project_id in usage_table.keys():
                project_ids += "'%s', " % project_id

            if "," in project_ids:
                project_ids = project_ids[:-2]

            project_ids += ")"

            #QUERY = "select ni.user_id as user_id, ni.project_id as project, ((sum((UNIX_TIMESTAMP(IF(IFNULL(ni.terminated_at, '%s')>='%s','%s', ni.terminated_at)) - (IF((ni.launched_at>=DATE_SUB('%s', INTERVAL '%s' day)), UNIX_TIMESTAMP(ni.launched_at), UNIX_TIMESTAMP(DATE_SUB('%s', INTERVAL '%s' day)) ))))/60) *ni.memory_mb) as memory_usage, ((sum((UNIX_TIMESTAMP(IF(IFNULL(ni.terminated_at, '%s')>='%s','%s', ni.terminated_at)) - (IF((ni.launched_at>=DATE_SUB('%s', INTERVAL '%s' day)), UNIX_TIMESTAMP(ni.launched_at), UNIX_TIMESTAMP(DATE_SUB('%s', INTERVAL '%s' day))))))/60) * ni.vcpus) as vcpu_usage from nova.instances ni where ni.project_id IN %s and ni.launched_at IS NOT NULL and ni.launched_at <='%s' and (ni.terminated_at>=DATE_SUB('%s', INTERVAL '%s' day) OR ni.terminated_at is null) group by ni.user_id, ni.project_id" % (fromDate, fromDate, fromDate, fromDate, period, fromDate, period, fromDate, fromDate, fromDate, fromDate, period, fromDate, period, project_ids, fromDate, fromDate, period)

            QUERY = "select user_id, project_id, sum(TIMESTAMPDIFF(SECOND, IF(launched_at>=DATE_SUB('%(date)s', INTERVAL '%(period)s' day), launched_at, DATE_SUB('%(date)s', INTERVAL '%(period)s' day)), IF(IFNULL(terminated_at, '%(date)s')>='%(date)s','%(date)s', terminated_at))*memory_mb) as memory_usage, sum(TIMESTAMPDIFF(SECOND, IF(launched_at>=DATE_SUB('%(date)s', INTERVAL '%(period)s' day), launched_at, DATE_SUB('%(date)s', INTERVAL '%(period)s' day)), IF(IFNULL(terminated_at, '%(date)s')>='%(date)s','%(date)s', terminated_at))*vcpus) as vcpus_usage from nova.instances where project_id IN %(project_ids)s and launched_at IS NOT NULL and launched_at <='%(date)s' and (terminated_at>=DATE_SUB('%(date)s', INTERVAL '%(period)s' day) OR terminated_at is NULL) group by user_id, project_id" % {
                "project_ids": project_ids,
                "date": fromDate,
                "period": period}

            LOG.info("QUERY %s" % QUERY)
            cursor.execute(QUERY)

            #cursor.execute("select ni.user_id as user_id, ni.project_id as project, ((sum((UNIX_TIMESTAMP(IF(IFNULL(ni.terminated_at, '" + fromDate + "')>='" + fromDate + "','" + fromDate + "',  ni.terminated_at)) - (IF((ni.launched_at>=DATE_SUB('" + fromDate + "', INTERVAL '" + period + "' day)), UNIX_TIMESTAMP(ni.launched_at), UNIX_TIMESTAMP(DATE_SUB('" + fromDate + "', INTERVAL '" + period + "' day)) ))))/60) *ni.memory_mb) as memory_usage, ((sum((UNIX_TIMESTAMP(IF(IFNULL(ni.terminated_at, '" + fromDate + "')>='" + fromDate + "','" + fromDate + "', ni.terminated_at)) - (IF((ni.launched_at>=DATE_SUB('" + fromDate + "', INTERVAL '" + period + "' day)), UNIX_TIMESTAMP(ni.launched_at), UNIX_TIMESTAMP(DATE_SUB('" + fromDate + "', INTERVAL '" + period + "' day))))))/60) * ni.vcpus) as vcpu_usage from nova.instances ni where ni.launched_at IS NOT NULL and ni.launched_at <='" + fromDate + "' and (ni.terminated_at>=DATE_SUB('" + fromDate + "', INTERVAL '" + period + "' day) OR ni.terminated_at is null) group by ni.user_id, ni.project_id")

            project_id = 0
            user_id = 0

            for row in cursor.fetchall():
                #LOG.info("row=%s" % row)
                user_id = row[0]
                project_id = row[1]

                if project_id not in usage_table:
                    LOG.warn("project not found: %s" % project_id)
                    continue

                project = usage_table[project_id]

                if user_id not in project["users"]:
                    LOG.warn("user not found: %s" % user_id)
                    continue

                usage_record = {
                    "memory_usage": float(
                        row[2]), "vcpus_usage": float(
                        row[3])}

                users = project["users"]
                users[user_id]["usage_records"].append(usage_record)
        except Exception as ex:
            LOG.error(ex)
        finally:
            cursor.close()
            conn.close()

    def printTable(self, stdout=False):
        msg = "\n---------------------------------------------------------------------------------------------------------------------------------------------------------------\n"
        msg += '{0:10s}| {1:13s}| {2:11s}| {3:14s}| {4:19s}| {5:20s}| {6:19s}| {7:19s}| {8:9s}| {9:6s}|\n'.format(
            "USER",
            "PROJECT",
            "USER SHARE",
            "PROJECT SHARE",
            "FAIR-SHARE (Vcpus)",
            "FAIR-SHARE (Memory)",
            "actual vcpus usage",
            "effec. vcpus usage",
            "priority",
            "VMs")
        msg += "---------------------------------------------------------------------------------------------------------------------------------------------------------------\n"

        conn = MySQLdb.connect(
            self.mysql_host,
            self.mysql_user,
            self.mysql_password)

        for project_id, project in self.usage_table.items():
            vmInstances = 0

            for user_id, user in project["users"].items():
                cursor = conn.cursor()
                try:
                    #cursor.execute("select count(*) from nova.instances where vm_state='active' and user_id='"+user_id+"'")
                    cursor.execute(
                        "select count(*) from nova.instances where terminated_at is null and launched_at is not null and deleted_at is null and user_id='" +
                        user_id +
                        "' and project_id='" +
                        project_id +
                        "'")
                    row = cursor.fetchone()
                    vmInstances = row[0]
                except Exception as ex:
                    LOG.error(ex)
                finally:
                    cursor.close()
                    # conn.close()

                msg += "{0:10s}| {1:13s}| {2:11s}| {3:14s}| {4:19s}| {5:20s}| {6:19s}| {7:19s}| {8:9}| {9:6}|\n".format(
                    user["name"], project["name"], str(
                        user["share"]) + "%", str(
                        project["share"]) + "%", str(
                        user["fair_share_vcpus"]), str(
                        user["fair_share_memory"]), "{0:.1f}%".format(
                        user["normalized_vcpus_usage"] * 100), "{0:.1f}%".format(
                            user["effective_vcpus_usage"] * 100), str(
                                int(
                                    user["fair_share_vcpus"] * self.vcpus_weight + user["fair_share_memory"] * self.memory_weight)), str(vmInstances))

        msg += "---------------------------------------------------------------------------------------------------------------------------------------------------------------\n"

        if stdout:
            print(msg)
        else:
            LOG.info(msg)

        conn.close()
