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

import os
import threading
import MySQLdb
import mysql.connector
import eventlet
import ConfigParser

from DBUtils.PooledDB import PooledDB
from datetime import datetime, timedelta

from oslo import messaging
from oslo.config import cfg
from synergy.common import config
from synergy.common import manager
from synergy.common import rpc
from synergy.common.client import keystone_v3
from synergy.common.client import nova
from synergy.openstack.common import log as logging

CONF = cfg.CONF
CONFIG = ConfigParser.SafeConfigParser()

LOG = logging.getLogger(__name__)


class Quota(object):
    cores = {"total": 0, "static_assigned": 0, "dynamic_in_use": 0}
    ram = {"total": 0, "static_assigned": 0, "dynamic_in_use": 0}
    dynamic_condition = threading.Condition()

    def __init__(self, name, project_id, quota_type, nova_client, pool):
        LOG.info(
            "new Quota project_id=%s quota_type=%s" %
            (project_id, quota_type))
        self.name = name
        self.quota_type = None
        self.project_id = project_id
        self.nova_client = nova_client
        self.pool = pool
        self.cores = {"in_use": 0, "hard_limit": 0}
        self.ram = {"in_use": 0, "hard_limit": 0}
        self.instances = {
            "in_use": 0,
            "hard_limit": 0,
            "ids": [],
            "pending": [],
            "processed": 0,
            "canc_active": 0,
            "canc_error": 0,
            "canc_building": 0}
        self.condition = None
        self.exit = False
        self.runtime_limit = -1

        self.__createTable()
        # self.__restoreQuotaDB()
        self.__getUsageDB()
        self.setType(quota_type)

    def __createTable(self):
        try:
            TABLE = "CREATE TABLE IF NOT EXISTS `quotas` (`project_id` CHAR(40) NOT NULL PRIMARY KEY, `type` INT DEFAULT 0, `name` CHAR(40) NOT NULL, `cores` INT DEFAULT 0 NOT NULL, `ram` INT DEFAULT 0 NOT NULL, `instances` INT DEFAULT 0 NOT NULL, `creation_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, `last_update` TIMESTAMP NULL) ENGINE=InnoDB"

            dbConnection = self.pool.connection()
            cursor = dbConnection.cursor()
            cursor.execute(TABLE)
        except Exception as ex:
            LOG.error(ex)
            raise ex
        finally:
            try:
                cursor.close()
                dbConnection.close()
            except MySQLdb.Error as ex:
                pass

    def __restoreQuotaDB(self):
        try:
            QUERY = "select cores, ram, instances from quotas where project_id = %(id)s"
            dbConnection = self.pool.connection()
            cursor = dbConnection.cursor()
            cursor.execute(QUERY, {"id": self.project_id})

            row = cursor.fetchone()
            cores = 0
            ram = 0
            instances = 0

            if row is not None:
                cores = row[0]
                ram = row[1]
                instances = row[2]
            #LOG.info(">>>>>>>>>>>>>>>>>>>>>>>> restoreQuotaDB project_id=%s, cores=%s, ram=%s instances=%s" % (self.project_id, cores, ram, instances))

            self.cores["hard_limit"] = cores
            self.ram["hard_limit"] = ram
            self.instances["hard_limit"] = instances
        except Exception as ex:
            LOG.error(ex)
            raise ex
        finally:
            try:
                cursor.close()
                dbConnection.close()
            except MySQLdb.Error as ex:
                pass

    def __storeQuotaDB(self):
        try:
            QUERY = "INSERT INTO quotas (project_id, type, cores, ram, instances) VALUES (%(project_id)s, %(type)s, %(cores)s, %(ram)s, %(instances)s) ON DUPLICATE KEY UPDATE cores=%(cores)s, ram=%(ram)s, instances=%(instances)s"

            dbConnection = self.pool.connection()
            cursor = dbConnection.cursor()
            cursor.execute(QUERY,
                           {"project_id": self.project_id,
                            "name": self.name,
                            "type": 0,
                            "cores": self.cores["hard_limit"],
                               "ram": self.ram["hard_limit"],
                               "instances": self.instances["hard_limit"]})

            dbConnection.commit()
        except Exception as ex:
            LOG.error(ex)
            if dbConnection:
                dbConnection.rollback()
        finally:
            try:
                cursor.close()
                dbConnection.close()
            except MySQLdb.Error as ex:
                pass

    def __getUsageDB(self):
        try:
            # retrieve the amount of resources in terms of cores and ram the specified project is consuming
            #QUERY = "select sum(vcpus), sum(memory_mb) from nova.instances where terminated_at is NULL and deleted_at is NULL and vm_state in ('error', 'active') and project_id = %(id)s"
            #QUERY = "select sum(vcpus), sum(memory_mb) from nova.instances where project_id = %(project_id)s and deleted_at is NULL and (vm_state in ('error') or (vm_state in ('active') and terminated_at is NULL))"
            QUERY = "select uuid, vcpus, memory_mb from nova.instances where project_id = %(project_id)s and deleted_at is NULL and (vm_state in ('error') or (vm_state in ('active') and terminated_at is NULL))"

            dbConnection = self.pool.connection()
            cursor = dbConnection.cursor()
            cursor.execute(QUERY, {"project_id": self.project_id})

            cores = 0
            ram = 0

            for row in cursor.fetchall():
                self.instances["ids"].append(row[0])
                cores += row[1]
                ram += row[2]

            self.cores["in_use"] = int(cores)
            self.ram["in_use"] = int(ram)

            #LOG.info(">>>>>>>>>>>>>>>>>>>>>>>> getUsageDB instances=%s ram=%s cores=%s" % (self.instances["ids"], self.ram["in_use"], self.cores["in_use"]))
        except Exception as ex:
            LOG.error(ex)
            raise ex
        finally:
            try:
                cursor.close()
                dbConnection.close()
            except MySQLdb.Error as ex:
                pass

    def getName(self):
        return self.name

    def getType(self):
        return self.quota_type

    def update(self):
        #LOG.info(">>>>>>>>>>>>>>>>>>>>>>>> update name=%s type=%s runtime_limit=%s" % (self.name, self.quota_type, self.runtime_limit))
        if self.quota_type == "dynamic":
            if len(self.instances["ids"]) == 0:
                return

            if self.runtime_limit > 0:
                try:
                    dbConnection = self.pool.connection()

                    cursor = dbConnection.cursor()
                    # retrieve all expired instances for the specified project and expiration time
                    #instance_id_list = u'{}'.format(self.instances["ids"])
                    instance_id_list = "'%s'" % "','".join(
                        self.instances["ids"])

                    query = "select uuid, vcpus, memory_mb, vm_state from nova.instances where project_id = '%(project_id)s' and deleted_at is NULL and (vm_state in ('error') or (uuid in (%(instances)s) and (vm_state in ('active') and terminated_at is NULL and timestampdiff(minute, launched_at, utc_timestamp()) >= %(expiration)s) or (vm_state in ('building') and task_state in ('scheduling') and created_at != updated_at and timestampdiff(minute, updated_at, utc_timestamp()) >= 20)))"

                    #LOG.info(query % {"instances": instance_id_list, "expiration": self.runtime_limit })
                    query = query % {
                        "project_id": self.project_id,
                        "instances": instance_id_list,
                        "expiration": self.runtime_limit}
                    # LOG.info(query)
                    #query = "select uuid, vcpus, memory_mb from nova.instances where uuid in (%(instances)s) and project_id = %(project_id)s and deleted_at is NULL and (vm_state in ('error') or (vm_state in ('active') and terminated_at is NULL and timestampdiff(minute, launched_at, utc_timestamp()) >= %(expiration)s) or (vm_state in ('building') and task_state in ('scheduling') and created_at != updated_at and timestampdiff(minute, updated_at, utc_timestamp()) >= 2))"

                    #cursor.execute(query, { "project_id": self.project_id, "expiration": self.runtime_limit, "instances": instance_id_list })
                    #cursor.execute(query, { "instances": instance_id_list, "expiration": self.runtime_limit })
                    cursor.execute(query)

                    #LOG.info("query=%s project_id=%s expiration=%s instance_id_list=%s" % (query, self.project_id, self.runtime_limit, instance_id_list))
                    for row in cursor.fetchall():
                        instance_id = row[0]
                        cores = row[1]
                        ram = row[2]

                        if row[3] == "active":
                            self.instances["canc_active"] += 1
                        elif row[3] == "canc_building":
                            self.instances["building"] += 1
                        elif row[3] == "error":
                            self.instances["canc_error"] += 1

                        try:
                            LOG.info(
                                "deleting the expired instance '%s' from project=%s" %
                                (instance_id, self.project_id))
                            self.nova_client.deleteServer(instance_id)
                            self.release(instance_id, cores, ram)
                        except Exception as ex:
                            LOG.error(ex)
                except Exception as ex:
                    LOG.error(ex)
                    raise ex
                finally:
                    try:
                        cursor.close()
                        dbConnection.close()
                    except MySQLdb.Error as ex:
                        pass
        else:
            cores = self.cores["hard_limit"]
            ram = self.ram["hard_limit"]
            instances = self.instances["hard_limit"]

            if cores == 0 and ram == 0 and instances == 0:
                self.__restoreQuotaDB()

                cores = self.cores["hard_limit"]
                ram = self.ram["hard_limit"]
                instances = self.instances["hard_limit"]

            project_quota = self.nova_client.getQuota(self.project_id, False)

            if project_quota["cores"] == -1 and project_quota["ram"] == -1:
                if cores == 0 and ram == 0:
                    project_quota_defaults = self.nova_client.getQuota(
                        self.project_id, True)

                    cores = project_quota_defaults["cores"]
                    ram = project_quota_defaults["ram"]
                    instances = project_quota_defaults["instances"]
            else:
                cores = project_quota["cores"]
                ram = project_quota["ram"]
                instances = project_quota["instances"]

            if self.cores["hard_limit"] != cores or self.ram[
                    "hard_limit"] != ram or self.instances["hard_limit"] != instances:
                with self.condition:
                    self.cores["hard_limit"] = cores
                    self.ram["hard_limit"] = ram
                    self.instances["hard_limit"] = instances

                    self.__storeQuotaDB()

                    self.condition.notifyAll()

    def setType(self, quota_type):
        if quota_type != "static" and quota_type != "extended" and quota_type != "dynamic":
            raise Exception(
                "wrong quota type %s, use 'static', 'extended' or 'dynamic'" %
                (self.quota_type))

        if self.quota_type == quota_type:
            return

        elif self.quota_type is None:
            if quota_type == "dynamic":
                self.update()
                self.quota_type = quota_type
                self.condition = Quota.dynamic_condition

                with self.condition:
                    Quota.ram["dynamic_in_use"] += self.ram["in_use"]
                    Quota.cores["dynamic_in_use"] += self.cores["in_use"]
                    self.condition.notifyAll()
            else:
                self.quota_type = quota_type
                self.condition = threading.Condition()
                self.update()
                Quota.ram["static_assigned"] += self.ram["hard_limit"]
                Quota.cores["static_assigned"] += self.cores["hard_limit"]

        elif self.quota_type == "dynamic":
            self.quota_type = quota_type
            self.condition = threading.Condition()
            self.update()

            with self.condition:
                Quota.ram["static_assigned"] += self.ram["hard_limit"]
                Quota.ram["dynamic_in_use"] -= self.ram["in_use"]
                Quota.cores["static_assigned"] += self.cores["hard_limit"]
                Quota.cores["dynamic_in_use"] -= self.cores["in_use"]
                self.condition.notifyAll()

        elif self.quota_type != "dynamic":
            if quota_type == "dynamic":
                self.update()
                self.quota_type = quota_type
                self.condition = Quota.dynamic_condition

                with self.condition:
                    Quota.ram["static_assigned"] -= self.ram["hard_limit"]
                    Quota.ram["dynamic_in_use"] += self.ram["in_use"]
                    Quota.cores["static_assigned"] -= self.cores["hard_limit"]
                    Quota.cores["dynamic_in_use"] += self.cores["in_use"]

                    self.condition.notifyAll()
            else:
                self.quota_type = quota_type
                self.update()

        try:
            if self.quota_type == "static" or self.quota_type == "extended":
                quota_set = {
                    "cores": self.cores["hard_limit"],
                    "ram": self.ram["hard_limit"],
                    "instances": self.instances["hard_limit"]}
                project_quota = self.nova_client.updateQuota(
                    self.project_id, quota_set)
            else:
                quota_set = {"cores": -1, "ram": -1, "instances": -1}
                project_quota = self.nova_client.updateQuota(
                    self.project_id, quota_set)
        except Exception as ex:
            LOG.error(ex)

        LOG.info(self.toDict())

    def isDynamicType(self):
        return self.quota_type == "dynamic"

    def isExtendedType(self):
        return self.quota_type == "extended"

    def isStaticType(self):
        return self.quota_type == "static"

    def getCores(self):
        return self.cores

    def getInstances(self):
        return self.instances

    def setRuntimeLimit(self, limit):
        self.runtime_limit = limit

    def setCores(self, in_use=None, hard_limit=None):
        with self.condition:
            if in_use is not None:
                self.cores["in_use"] = int(in_use)

            if hard_limit is not None:
                self.cores["hard_limit"] = int(hard_limit)

            self.condition.notifyAll()

    def getRam(self):
        return self.ram

    def setRam(self, in_use=None, hard_limit=None):
        with self.condition:
            if in_use is not None:
                self.ram["in_use"] = int(in_use)

            if hard_limit is not None:
                self.ram["hard_limit"] = int(hard_limit)

            self.condition.notifyAll()

    def allocate(self, instance_id, cores, ram, blocking=True):
        LOG.info(
            "allocate instance_id=%s cores=% ram=%s" %
            (instance_id, cores, ram))
        found = False

        with self.condition:
            if instance_id in self.instances["ids"]:
                found = True
            elif instance_id in self.instances["pending"]:
                found = True
            else:
                self.instances["pending"].append(instance_id)

            while (not found and instance_id in self.instances[
                   "pending"] and not self.exit):
                if self.isDynamicType():
                    LOG.info(
                        "allocate name=%s type=%s instance_id=%s cores=%s ram=%s dynamic [vcpu in_use %s of %s; ram in use %s of %s]" %
                        (self.getName(),
                         self.getType(),
                            instance_id,
                            cores,
                            ram,
                            Quota.cores["dynamic_in_use"],
                            Quota.cores["total"] -
                            Quota.cores["static_assigned"],
                            Quota.ram["dynamic_in_use"],
                            Quota.ram["total"] -
                            Quota.ram["static_assigned"]))

                    if (Quota.cores["total"] - Quota.cores["static_assigned"] - Quota.cores["dynamic_in_use"] >= cores) and (
                            Quota.ram["total"] - Quota.ram["static_assigned"] - Quota.ram["dynamic_in_use"] >= ram):
                        self.cores["in_use"] += cores
                        self.ram["in_use"] += ram

                        Quota.cores["dynamic_in_use"] += cores
                        Quota.ram["dynamic_in_use"] += ram

                        found = True
                        self.instances["ids"].append(instance_id)
                        self.instances["pending"].remove(instance_id)

                        LOG.info(
                            "allocated name=%s type=%s instance_id=%s cores=%s ram=%s dynamic [vcpu in_use %s of %s; ram in use %s of %s]" %
                            (self.getName(),
                             self.getType(),
                                instance_id,
                                cores,
                                ram,
                                Quota.cores["dynamic_in_use"],
                                Quota.cores["total"] -
                                Quota.cores["static_assigned"],
                                Quota.ram["dynamic_in_use"],
                                Quota.ram["total"] -
                                Quota.ram["static_assigned"]))
                        self.instances["processed"] += 1
                        # self.condition.notifyAll()
                    elif blocking:
                        LOG.info("allocate wait!!!")
                        self.condition.wait()

                elif self.cores["hard_limit"] - self.cores["in_use"] >= cores and self.ram["hard_limit"] - self.ram["in_use"] >= ram:
                    LOG.info(
                        "allocate name=%s type=%s instance_id=%s cores=%s of %s ram=%s of %s" %
                        (self.getName(),
                         self.getType(),
                            instance_id,
                            cores,
                            ram,
                            self.getCores(),
                            self.getRam()))

                    self.cores["in_use"] += cores
                    self.ram["in_use"] += ram

                    found = True
                    self.instances["ids"].append(instance_id)
                    self.instances["pending"].remove(instance_id)

                    LOG.info(
                        "allocated name=%s type=%s instance_id=%s cores=%s of %s ram=%s of %s" %
                        (self.getName(),
                         self.getType(),
                            instance_id,
                            cores,
                            ram,
                            self.getCores(),
                            self.getRam()))
                    # self.condition.notifyAll()

                elif blocking:
                    LOG.info("allocate wait!!!")
                    self.condition.wait()
                """
                LOG.info("allocate name=%s type=%s cores=%s ram=%s" % (self.getName(), self.getType(), self.getCores(), self.getRam()))
                if self.cores["hard_limit"] - self.cores["in_use"] - self.cores["reserved"] >= cores and self.ram["hard_limit"] - self.ram["in_use"] - self.ram["reserved"] >= ram:
                    self.cores["in_use"] += cores
                    self.ram["in_use"] += ram

                    found = True
                elif blocking:
                    LOG.info("allocate wait!!!")
                    self.condition.wait()
                """

            self.condition.notifyAll()

        return found

    def release(self, instance_id, cores, ram):
        LOG.info(
            "release: instance_id=%s cores=%s ram=%s before release: cores=%s ram=%s d_cores=%s d_ram=%s" %
            (instance_id,
             cores,
             ram,
             self.cores["in_use"],
             self.ram["in_use"],
             Quota.cores["dynamic_in_use"],
             Quota.ram["dynamic_in_use"]))

        with self.condition:
            if instance_id in self.instances["pending"]:
                self.instances["pending"].remove(instance_id)
            elif instance_id in self.instances["ids"]:
                if self.cores["in_use"] - cores < 0:
                    self.cores["in_use"] = 0
                else:
                    self.cores["in_use"] -= cores

                if self.ram["in_use"] - ram < 0:
                    self.ram["in_use"] - 0
                else:
                    self.ram["in_use"] -= ram

                if self.isDynamicType():
                    if Quota.cores["dynamic_in_use"] - cores < 0:
                        Quota.cores["dynamic_in_use"] = 0
                    else:
                        Quota.cores["dynamic_in_use"] -= cores

                    if Quota.ram["dynamic_in_use"] - ram < 0:
                        Quota.ram["dynamic_in_use"] - 0
                    else:
                        Quota.ram["dynamic_in_use"] -= ram

                self.instances["ids"].remove(instance_id)

                LOG.info(
                    "release: instance_id=%s cores=%s ram=%s after release: cores=%s ram=%s d_cores=%s d_ram=%s" %
                    (instance_id,
                     cores,
                     ram,
                     self.cores["in_use"],
                        self.ram["in_use"],
                        Quota.cores["dynamic_in_use"],
                        Quota.ram["dynamic_in_use"]))
            else:
                LOG.warn("release: instance '%s' not found!" % (instance_id))
            self.condition.notifyAll()

    def toDict(self):
        quota = {}
        quota["name"] = self.name
        quota["type"] = self.quota_type
        quota["instances"] = self.instances["ids"]

        #quota["cores"] = self.cores
        #quota["ram"] = self.ram

        if self.isDynamicType():
            quota["ram"] = {
                "in_use_p": self.ram["in_use"],
                "in_use": Quota.ram["dynamic_in_use"],
                "hard_limit": Quota.ram["total"] -
                Quota.ram["static_assigned"]}
            quota["cores"] = {
                "in_use_p": self.cores["in_use"],
                "in_use": Quota.cores["dynamic_in_use"],
                "hard_limit": Quota.cores["total"] -
                Quota.cores["static_assigned"]}
        else:
            quota["ram"] = {
                "in_use": self.ram["in_use"],
                "hard_limit": self.ram["hard_limit"]}
            quota["cores"] = {
                "in_use": self.cores["in_use"],
                "hard_limit": self.cores["hard_limit"]}

        return quota


class QuotaManager(manager.Manager):

    def __init__(self):
        super(
            QuotaManager,
            self).__init__(
            name="QuotaManager",
            autostart=True,
            rate=60)
        # eventlet.monkey_patch()

        self.config_opts = [
            cfg.IntOpt(
                "quota_instances",
                default=10,
                help="Number of instances allowed per project"),
            cfg.IntOpt(
                "quota_cores",
                default=20,
                help="Number of instance cores allowed per project"),
            cfg.IntOpt(
                "quota_ram",
                default=50 * 1024,
                help="Megabytes of instance RAM allowed per project"),
            cfg.ListOpt(
                "dynamic_quotas",
                default=[],
                help="the projects list with dynamic quota")
        ]

    def setup(self):
        LOG.info("%s setup invoked!" % (self.getName()))
        self.condition = threading.Condition()
        self.exit = False
        self.dynamic_quotas = CONF.QuotaManager.dynamic_quotas
        self.quotas = {}

        try:
            mysql_user = CONF.MYSQL.user
            mysql_passwd = CONF.MYSQL.password
            mysql_db = CONF.MYSQL.db
            mysql_host = CONF.MYSQL.host
            poolSize = CONF.MYSQL.pool_size

            if poolSize < 1:
                poolSize = 10

            self.pool = None
            self.pool = PooledDB(
                mysql.connector,
                poolSize,
                database=mysql_db,
                user=mysql_user,
                passwd=mysql_passwd,
                host=mysql_host)
        except Exception as ex:
            LOG.error(ex)
            raise ex

        try:
            nova_cfg_file = cfg.find_config_files(
                project="nova", extension='.conf')
            CONFIG.read(nova_cfg_file)

            self.cpu_allocation_ratio = 16
            self.ram_allocation_ratio = 16
            self.disk_allocation_ratio = 1

            if len(nova_cfg_file) > 0 and os.path.isfile(nova_cfg_file[0]):
                try:
                    self.cpu_allocation_ratio = CONFIG.getfloat(
                        "DEFAULT", "cpu_allocation_ratio")
                except Exception as xxx_todo_changeme:
                    ConfigParser.NoOptionError = xxx_todo_changeme
                    LOG.info(
                        "No option 'cpu_allocation_ratio' found in %s: using default '%s'" %
                        (nova_cfg_file, self.cpu_allocation_ratio))

                try:
                    self.ram_allocation_ratio = CONFIG.getfloat(
                        "DEFAULT", "ram_allocation_ratio")
                except Exception as xxx_todo_changeme1:
                    ConfigParser.NoOptionError = xxx_todo_changeme1
                    LOG.info(
                        "No option 'ram_allocation_ratio' found in %s: using default '%s'" %
                        (nova_cfg_file, self.ram_allocation_ratio))

                try:
                    self.disk_allocation_ratio = CONFIG.getfloat(
                        "DEFAULT", "disk_allocation_ratio")
                except Exception as xxx_todo_changeme2:
                    ConfigParser.NoOptionError = xxx_todo_changeme2
                    LOG.info(
                        "No option 'disk_allocation_ratio' found in %s: using default '%s'" %
                        (nova_cfg_file, self.disk_allocation_ratio))
            else:
                LOG.warn("nova configuration file not found!!!")

            keystone_admin_user = CONF.Keystone.admin_user
            keystone_admin_password = CONF.Keystone.admin_password
            keystone_admin_project_name = CONF.Keystone.admin_project_name
            keystone_auth_url = CONF.Keystone.auth_url

            self.keystone_client = keystone_v3.KeystoneClient(
                auth_url=keystone_auth_url,
                username=keystone_admin_user,
                password=keystone_admin_password,
                project_name=keystone_admin_project_name)
            self.nova_client = nova.NovaClient(self.keystone_client)

            #self.dynamic_quota = Quota(quota_type="dynamic", name="dynamic quota", project_id="pippo", nova_client=self.nova_client, pool=self.pool)
            self.getQuotas()
            """
            for project_id, project in self.projects.items():
                quota = project["quota"]
                LOG.info("quota name='%s' type='%s' project_id='%s' project_name='%s' cores=%s ram=%s instances=%s" % (quota.getName(), quota.getType(), project_id, project["name"], quota.getCores(), quota.getRam(), quota.getInstances()))
            """

            rpc.init(CONF)

            endpoints = [self]

            LOG.info("creating notification listener for topic 'notifications'")
            self.target = [
                messaging.Target(
                    topic='notifications',
                    version="3.0")]

            eventlet.monkey_patch()
            self.listener = rpc.getNotificationListener(self.target, endpoints)
            self.listener.start()
        except Exception as ex:
            raise ex

    def destroy(self):
        LOG.info("destroy invoked!")
        with self.condition:
            self.exit = True
            self.quotas.clear()
            self.condition.notifyAll()

    def execute(self, cmd):
        if cmd.getName() == "get_quota":
            project_ids = cmd.getParameter("project_id")
            project_names = cmd.getParameter("project_name")
            quota_type = cmd.getParameter("type")
            item_list = None

            if project_ids:
                if isinstance(project_ids, str):
                    item_list = [project_ids]
                elif isinstance(project_ids, list):
                    item_list = project_ids

                for project_id in item_list:
                    if project_id in self.quotas:
                        quota = self.quotas.get(project_id)
                        cmd.addResult(project_id, quota.toDict())
                        #cmd.addResult(project_id, { "name": project["name"], "quota": project["quota"].toDict() })

                if len(item_list) == 1 and len(cmd.getResults()) == 0:
                    raise Exception(
                        "quota for project id='%s' not found!" %
                        item_list[0])

            elif project_names:
                if isinstance(project_names, str):
                    item_list = [project_names]
                elif isinstance(project_names, list):
                    item_list = project_names

                for project_id, quota in self.quotas.items():
                    if quota.getName() in item_list:
                        cmd.addResult(project_id, quota.toDict())
                        #cmd.addResult(project_id, { "name": project["name"], "quota": project["quota"].toDict() })

                if len(item_list) == 1 and len(cmd.getResults()) == 0:
                    raise Exception(
                        "quota for project name='%s' not found!" %
                        item_list[0])

            elif quota_type:
                for project_id, quota in self.quotas.items():
                    if quota.getType() == quota_type:
                        cmd.addResult(project_id, quota.toDict())
                        #cmd.addResult(project_id, { "name": project["name"], "quota": project["quota"].toDict() })

                if len(cmd.getResults()) == 0:
                    raise Exception("quota type='%s' not found!" % quota_type)
            else:
                for project_id, quota in self.quotas.items():
                    cmd.addResult(project_id, quota.toDict())
        else:
            raise Exception("command '%s' not supported!" % cmd.getName())

        # cmd.log()

    def task(self):
        with self.condition:
            try:
                self.getQuotas()

                for project_id, quota in self.quotas.items():
                    LOG.info(
                        "quota name='%s' type='%s' project_id='%s' cores=%s ram=%s instances=%s" %
                        (quota.getName(),
                         quota.getType(),
                            project_id,
                            quota.getCores(),
                            quota.getRam(),
                            quota.getInstances()))

                self.condition.notifyAll()
            except Exception as ex:
                LOG.error(ex)

    # mysql> select resource, sum(hard_limit) from nova.quotas where deleted_at is NULL and project_id NOT IN ('f12a98640faf4616951843b331b66534', '4377704578d44117a4f60764b948b98c') group by resource;
    # mysql> select sum(vcpus), sum(memory_mb), sum(local_gb) from
    # compute_nodes;

    def info(self, ctxt, publisher_id, event_type, payload, metadata):
        #LOG.info("Notification INFO: event_type=%s payload=%s" % (event_type, payload))

        if payload is None or "state" not in payload:
            return

        state = payload["state"]
        instance_id = payload["instance_id"]

        #LOG.info(">>>>>>>> Notification INFO: event_type=%s desc=%s state=%s instance_id=%s" % (event_type, payload["state_description"], payload["state"], instance_id))

        if (event_type == "compute.instance.delete.end" and (state == "deleted" or state == "error" or state == "building")) or (event_type ==
                                                                                                                                 "compute.instance.update" and (state == "deleted" or state == "error")) or (event_type == "scheduler.run_instance" and state == "error"):
            ram = 0
            cores = 0
            project_id = 0
            instance_info = None
            state_description = None

            if event_type == "scheduler.run_instance":
                instance_info = payload["request_spec"]["instance_type"]
            else:
                instance_info = payload
                state_description = payload["state_description"]

            project_id = instance_info["tenant_id"]
            instance_id = instance_info["instance_id"]
            ram = instance_info["memory_mb"]
            cores = instance_info["vcpus"]
            disk = instance_info["root_gb"]
            node = instance_info["node"]

            LOG.info(
                "Notification INFO (type=%s state=%s): cores=%s ram=%s project_id=%s instance_id=%s" %
                (event_type, state_description, cores, ram, project_id, instance_id))

            if state == "error":
                try:
                    self.nova_client.deleteServer(instance_id)
                except Exception as ex:
                    LOG.warn("Notification INFO: %s" % ex)

            try:
                quota = self.getQuota(project_id)
                quota.release(instance_id, cores, ram)
            except Exception as ex:
                LOG.warn("Notification INFO: %s" % ex)

    def warn(self, ctxt, publisher_id, event_type, payload, metadata):
        #LOG.info("Notification WARN: event_type=%s payload=%s metadata=%s" % (event_type, payload, metadata))
        #state = payload["state"]
        instance_id = payload["instance_id"]
        LOG.info(
            "Notification WARN: event_type=%s state=%s instance_id=%s" %
            (event_type, state, instance_id))

    def error(self, ctxt, publisher_id, event_type, payload, metadata):
        LOG.info(
            "Notification ERROR: event_type=%s payload=%s metadata=%s" %
            (event_type, payload, metadata))

        if payload is None or "state" not in payload:
            return

        state = payload["state"]
        instance_id = payload["instance_id"]

        if event_type == "scheduler.run_instance" and state == "error":
            ram = payload["request_spec"]["instance_type"]["memory_mb"]
            cores = payload["request_spec"]["instance_type"]["vcpus"]
            project_id = payload["request_spec"][
                "instance_properties"]["project_id"]
            instance_id = payload["instance_id"]

            LOG.info(
                "Notification ERROR (%s): cores=%s ram=%s project_id=%s instance_id=%s" %
                (event_type, cores, ram, project_id, instance_id))

            try:
                self.nova_client.deleteServer(instance_id)
            except Exception as ex:
                LOG.warn("Notification ERROR: %s" % ex)

            try:
                self.nova_client.deleteServer(instance_id)

                quota = self.getQuota(project_id)
                quota.release(instance_id, cores, ram)
            except Exception as ex:
                LOG.warn("Notification ERROR: %s" % ex)

    def getQuota(self, project_id):
        if project_id not in self.quotas:
            raise Exception("quota for project '%s' not found!" % project_id)

        return self.quotas[project_id]

    def getQuotas(self):
        #dynamic_quota = Quota(quota_type="dynamic", name="dynamic quota")
        conn = None
        cursor = None

        # calculate the the total hard_limit per cores and ram
        total_ram = 0
        total_cores = 0
        total_cores_limit = 0
        total_ram_limit = 0
        total_dynamic_cores_limit = 0
        total_dynamic_ram_limit = 0
        dynamic_project_ids = []

        try:
            hypervisors = self.nova_client.getHypervisors()

            for hypervisor in hypervisors:
                if hypervisor["status"] == "enabled" and hypervisor[
                        "state"] == "up":
                    hypervisor_details = self.nova_client.getHypervisor(hypervisor[
                                                                        "id"])

                    total_ram += hypervisor_details["memory_mb"]
                    total_cores += hypervisor_details["vcpus"]

            LOG.info("total_ram=%s total_cores=%s" % (total_ram, total_cores))

            Quota.ram["total"] = total_ram * int(self.ram_allocation_ratio)
            Quota.cores["total"] = total_cores * int(self.cpu_allocation_ratio)
            #Quota.dynamic_ram["hard_limit"] = total_ram
            #Quota.dynamic_cores["hard_limit"] = total_cores

            kprojects = self.keystone_client.getProjects()

            for project in kprojects:
                project_id = project["id"]
                project_name = str(project["name"])

                if project_id in self.quotas:
                    quota = self.quotas[project_id]
                    quota.update()
                else:
                    quota_type = "static"

                    #LOG.warn(">>>>>>>>>>>>>>>>>>> new quota project_name %s " % project_name)
                    # if project_name in self.dynamic_quotas:
                    #    quota_type = "dynamic"

                    quota = Quota(
                        project_name,
                        project_id,
                        quota_type,
                        self.nova_client,
                        self.pool)
                    self.quotas[project_id] = quota

                if quota.isDynamicType():
                    dynamic_project_ids.append(project_id)
                else:
                    total_cores_limit += quota.getCores()["hard_limit"]
                    total_ram_limit += quota.getRam()["hard_limit"]

            if total_cores < total_cores_limit:
                LOG.warn(
                    "dynamic quota disabled: the total hard limit for cores ('%s') is greater than the total amount of cores allowed ('%s')" %
                    (total_cores_limit, total_cores))
                total_dynamic_cores_limit = 0
            else:
                total_dynamic_cores_limit = total_cores - total_cores_limit

            if total_ram < total_ram_limit:
                LOG.warn(
                    "dynamic quota disabled: the total hard limit for ram ('%s') is greater than the total amount of ram allowed ('%s')" %
                    (total_ram_limit, total_ram))
                total_dynamic_ram_limit = 0
            else:
                total_dynamic_ram_limit = total_ram - total_ram_limit

            Quota.ram["static_assigned"] = total_ram_limit
            Quota.cores["static_assigned"] = total_cores_limit

            LOG.info(">>>>>>> quota_ram %s" % Quota.ram)
            LOG.info(">>>>>>> quota_cores %s" % Quota.cores)
            LOG.info(">>>>>>> total_cores %s" % total_cores)
            LOG.info(">>>>>>> total_ram %s" % total_ram)
            LOG.info(">>>>>>> total_cores_limit %s" % total_cores_limit)
            LOG.info(">>>>>>> total_ram_limit %s" % total_ram_limit)
            LOG.info(
                ">>>>>>> total_dynamic_cores_limit %s" %
                total_dynamic_cores_limit)
            LOG.info(
                ">>>>>>> total_dynamic_ram_limit %s" %
                total_dynamic_ram_limit)
            LOG.info(">>>>>>> dynamic_project_ids %s" % dynamic_project_ids)
            """
            for quota in self.quotas.values():
                LOG.info(">>>>>>> quota %s" % quota.toDict())
            """

            #Quota.dynamic_cores["hard_limit"] = total_dynamic_cores_limit
            #Quota.dynamic_ram["hard_limit"] = total_dynamic_ram_limit
            """
            if len(dynamic_project_ids) > 0:
                try:
                    dbConnection = self.pool.connection()

                    cursor = dbConnection.cursor()
                    # retrieve all expired instances for the specified project and expiration time
                    query = "select uuid from nova.instances where terminated_at is NULL and deleted_at is NULL and project_id = %(project_id)s and (vm_state in ('error') or (vm_state in ('active') and now()-launched_at>= %(expiration)s))"

                    for project_id in dynamic_project_ids:
                        LOG.info(">>>>>>> dynamic_quota_cores %s %s %s" % (project_id, self.quotas[project_id].getCores(), self.quotas[project_id].getRam()))
                        cursor.execute(query, { "project_id": project_id, "expiration": 3 })

                        for row in cursor.fetchall():
                            instance_id = row[0]

                            try:
                                self.nova_client.deleteServer(instance_id)
                            except Exception as ex:
                                LOG.error(ex)
                except Exception as ex:
                    LOG.error(ex)
                    raise ex
                finally:
                    try:
                        cursor.close()
                        dbConnection.close()
                    except MySQLdb.Error as ex:
                        pass
            """
        except Exception as ex:
            LOG.error(ex)
            raise ex

    """
    def printTable(self, stdout=False):
        msg = "\n-----------------------------------------------------------------------------------------------------------\n"
        msg += '{0:26s}| {1:23s}| {2:28s}| {3:23s}|\n'.format("COMPUTE", "VCPUS (T-U-A)", "MEMORY MB (T-U-A)", "DISK GB (T-U-A)")
        msg += "-----------------------------------------------------------------------------------------------------------\n"

        with self.condition:
            for compute, resource in self.computeResource.items():
                msg += "{0:26s}| {1:23s}| {2:28s}| {3:23s}|\n".format(compute, str(resource["vcpus"]["total"]) + " - " + str(resource["vcpus"]["used"]) + " - " + str(resource["vcpus"]["available"]), str(resource["memory"]["total"]) + " - " + str(resource["memory"]["used"]) + " - " + str(resource["memory"]["available"]), str(resource["disk"]["total"]) + " - " + str(resource["disk"]["used"]) + " - " + str(resource["disk"]["available"]))

        msg += "-----------------------------------------------------------------------------------------------------------\n"

        if stdout:
            print(msg)
        else:
            LOG.info(msg)
    """
