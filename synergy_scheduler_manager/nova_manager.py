import eventlet
import hashlib
import hmac
import json
import logging
import requests

from common.block_device import BlockDeviceMapping
from common.compute import Compute
from common.flavor import Flavor
from common.hypervisor import Hypervisor
from common.messaging import AMQP
from common.quota import Quota
from common.request import Request
from common.server import Server
from oslo_config import cfg
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from synergy.common.manager import Manager
from synergy.exception import SynergyError

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


LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class ServerEventHandler(object):

    def __init__(self, nova_manager):
        super(ServerEventHandler, self).__init__()

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
        server.setType()

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

        self.nova_manager.notify(event_type="SERVER_EVENT", server=server,
                                 event=event_type, state=state)

    def warn(self, ctxt, publisher_id, event_type, payload, metadata):
        LOG.debug("Notification WARN: event_type=%s, payload=%s metadata=%s"
                  % (event_type, payload, metadata))

    def error(self, ctxt, publisher_id, event_type, payload, metadata):
        LOG.debug("Notification ERROR: event_type=%s, payload=%s metadata=%s"
                  % (event_type, payload, metadata))


class NovaConductorComputeAPI(object):

    def __init__(self, synergy_topic, conductor_topic, nova_manager, msg):
        self.nova_manager = nova_manager

        self.target = msg.getTarget(topic=synergy_topic,
                                    namespace="compute_task",
                                    version="1.16")

        self.client = msg.getRPCClient(
            target=msg.getTarget(topic=conductor_topic,
                                 namespace="compute_task",
                                 version="1.10"))

    def build_instances(self, context, instances, image, filter_properties,
                        admin_password, injected_files, requested_networks,
                        security_groups, block_device_mapping=None,
                        legacy_bdm=True):
        for instance in instances:
            data = {'instances': [instance],
                    'image': image,
                    'filter_properties': filter_properties,
                    'admin_password': admin_password,
                    'injected_files': injected_files,
                    'requested_networks': requested_networks,
                    'security_groups': security_groups,
                    'block_device_mapping': block_device_mapping,
                    'legacy_bdm': legacy_bdm}

            req = {"context": context, "data": data,
                   "action": "build_instances"}
            try:
                request = Request.fromDict(req)

                self.nova_manager.notify(event_type="SERVER_CREATE",
                                         request=request)
            except Exception as ex:
                LOG.info(ex)

    def schedule_and_build_instances(self, context, build_requests,
                                     request_specs, image,
                                     admin_password, injected_files,
                                     requested_networks, block_device_mapping):
        index = 0

        for build_request in build_requests:
            request_spec = request_specs[index]

            index += 1

            data = {'build_requests': [build_request],
                    'request_specs': [request_spec],
                    'image': image,
                    'admin_password': admin_password,
                    'injected_files': injected_files,
                    'requested_networks': requested_networks,
                    'block_device_mapping': block_device_mapping}

            req = {"context": context, "data": data,
                   "action": "schedule_and_build_instances"}

            request = Request.fromDict(req)

            self.nova_manager.notify(event_type="SERVER_CREATE",
                                     request=request)

    def build_instance(self, context, action, data):
        try:
            cctxt = self.client.prepare()
            cctxt.cast(context, action, **data)
        except Exception as ex:
            LOG.info(ex)

    def migrate_server(self, context, **kwargs):
        cctxt = self.client.prepare()
        return cctxt.call(context, 'migrate_server', **kwargs)

    def unshelve_instance(self, context, **kwargs):
        cctxt = self.client.prepare()
        cctxt.cast(context, 'unshelve_instance', **kwargs)

    def rebuild_instance(self, ctxt, **kwargs):
        cctxt = self.client.prepare()
        cctxt.cast(ctxt, 'rebuild_instance', **kwargs)

    def resize_instance(self, ctxt, **kwargs):
        cctxt = self.client.prepare()
        cctxt.cast(ctxt, 'resize_instance', **kwargs)


class NovaManager(Manager):

    def __init__(self):
        super(NovaManager, self).__init__("NovaManager")

        self.config_opts = [
            cfg.StrOpt("amqp_url",
                       help="the amqp transport url",
                       default=None,
                       required=False),
            cfg.StrOpt("amqp_exchange",
                       help="the amqp exchange",
                       default="nova",
                       required=False),
            cfg.StrOpt("amqp_backend",
                       help="the amqp backend tpye (e.g. rabbit, qpid)",
                       default=None,
                       required=False),
            cfg.ListOpt("amqp_hosts",
                        help="AMQP HA cluster host:port pairs",
                        default=None,
                        required=False),
            cfg.StrOpt("amqp_host",
                       help="the amqp host name",
                       default="localhost",
                       required=False),
            cfg.IntOpt("amqp_port",
                       help="the amqp listening port",
                       default=5672,
                       required=False),
            cfg.StrOpt("amqp_user",
                       help="the amqp user",
                       default=None,
                       required=False),
            cfg.StrOpt("amqp_password",
                       help="the amqp password",
                       default=None,
                       required=False),
            cfg.StrOpt("amqp_virtual_host",
                       help="the amqp virtual host",
                       default="/",
                       required=False),
            cfg.StrOpt("synergy_topic",
                       help="the Synergy topic",
                       default="synergy",
                       required=False),
            cfg.StrOpt("notification_topic",
                       help="the notifiction topic",
                       default="nova_notification",
                       required=False),
            cfg.StrOpt("conductor_topic",
                       help="the conductor topic",
                       default="conductor",
                       required=False),
            cfg.StrOpt("compute_topic",
                       help="the compute topic",
                       default="compute",
                       required=False),
            cfg.StrOpt("scheduler_topic",
                       help="the scheduler topic",
                       default="scheduler",
                       required=False),
            cfg.StrOpt("metadata_proxy_shared_secret",
                       help="the metadata proxy shared secret",
                       default="METADATA_SECRET",
                       required=True),
            cfg.FloatOpt("cpu_allocation_ratio",
                         help="the cpu allocation ratio",
                         default=float(16),
                         required=False),
            cfg.FloatOpt("ram_allocation_ratio",
                         help="the ram allocation ratio",
                         default=float(1.5),
                         required=False),
            cfg.StrOpt("db_connection",
                       help="the NOVA database connection",
                       default=None,
                       required=True),
            cfg.StrOpt("host",
                       help="the host name",
                       default="localhost",
                       required=False),
            cfg.IntOpt("timeout",
                       help="set the http connection timeout",
                       default=60,
                       required=False),
            cfg.StrOpt("ssl_ca_file",
                       help="set the PEM encoded Certificate Authority to "
                            "use when verifying HTTPs connections",
                       default=None,
                       required=False),
            cfg.StrOpt("ssl_cert_file",
                       help="set the SSL client certificate (PEM encoded)",
                       default=None,
                       required=False)
        ]

    def setup(self):
        eventlet.monkey_patch(os=False)

        self.ssl_ca_file = CONF.NovaManager.ssl_ca_file
        self.ssl_cert_file = CONF.NovaManager.ssl_cert_file
        self.timeout = CONF.NovaManager.timeout

        if self.getManager("KeystoneManager") is None:
            raise SynergyError("KeystoneManager not found!")

        if self.getManager("SchedulerManager") is None:
            raise SynergyError("SchedulerManager not found!")

        self.keystone_manager = self.getManager("KeystoneManager")

        amqp_url = self.getParameter("amqp_url")

        amqp_backend = self.getParameter("amqp_backend")

        amqp_hosts = self.getParameter("amqp_hosts")

        amqp_host = self.getParameter("amqp_host")

        amqp_port = self.getParameter("amqp_port")

        amqp_user = self.getParameter("amqp_user")

        amqp_password = self.getParameter("amqp_password")

        amqp_virtual_host = self.getParameter("amqp_virtual_host")

        amqp_exchange = self.getParameter("amqp_exchange")

        db_connection = self.getParameter("db_connection", fallback=True)

        host = self.getParameter("host")

        synergy_topic = self.getParameter("synergy_topic")

        notification_topic = self.getParameter("notification_topic")

        conductor_topic = self.getParameter("conductor_topic")

        self.getParameter("metadata_proxy_shared_secret", fallback=True)

        if not amqp_hosts:
            amqp_hosts = ["%s:%s" % (amqp_host, amqp_port)]
        try:
            LOG.debug("setting up the NOVA database connection: %s"
                      % db_connection)

            self.db_engine = create_engine(db_connection, pool_recycle=30)

            self.messaging = AMQP(url=amqp_url, backend=amqp_backend,
                                  username=amqp_user,
                                  password=amqp_password,
                                  hosts=amqp_hosts,
                                  virt_host=amqp_virtual_host,
                                  exchange=amqp_exchange)

            self.novaConductorComputeAPI = NovaConductorComputeAPI(
                synergy_topic,
                conductor_topic,
                self,
                self.messaging)

            self.conductor_rpc = self.messaging.getRPCServer(
                target=self.messaging.getTarget(topic=synergy_topic,
                                                server=host),
                endpoints=[self.novaConductorComputeAPI])

            self.conductor_rpc.start()

            self.serverEventHandler = ServerEventHandler(self)

            target = self.messaging.getTarget(topic=notification_topic,
                                              exchange=amqp_exchange)

            self.listener = self.messaging.getNotificationListener(
                targets=[target], endpoints=[self.serverEventHandler])

            self.listener.start()
        except Exception as ex:
            LOG.error("Exception has occured", exc_info=1)
            LOG.error("NovaManager initialization failed! %s" % (ex))
            raise ex

    def execute(self, command, *args, **kargs):
        raise SynergyError("command %r not supported!" % command)

    def task(self):
        pass

    def destroy(self):
        pass

    def getParameter(self, name, fallback=False):
        result = CONF.NovaManager.get(name, None)

        if result is not None:
            return result

        if fallback is True:
            raise SynergyError("No attribute %r found in [NovaManager] "
                               "section of synergy.conf" % name)
        else:
            return None

    def getUserData(self, server):
        if not server:
            return None

        secret = CONF.NovaManager.metadata_proxy_shared_secret

        if not secret:
            raise SynergyError("'metadata_proxy_shared_secret' "
                               "attribute not defined in synergy.conf")

        digest = hmac.new(secret, server.getId(), hashlib.sha256).hexdigest()

        self.keystone_manager.authenticate()
        token = self.keystone_manager.getToken()
        service = token.getService("nova")

        if not service:
            raise SynergyError("nova service not found!")

        endpoint = service.getEndpoint("public")

        if not endpoint:
            raise SynergyError("nova endpoint not found!")

        url = endpoint.getURL()
        url = url[:url.rfind(":") + 1] + "8775/openstack/2015-10-15/user_data"

        headers = {"Content-Type": "application/text",
                   "Accept": "application/text",
                   "User-Agent": "synergy",
                   "x-instance-id": server.getId(),
                   "x-tenant-id": server.getProjectId(),
                   "x-instance-id-signature": digest}

        request = requests.get(url, headers=headers,
                               timeout=self.timeout,
                               verify=self.ssl_ca_file,
                               cert=self.ssl_cert_file)

        if request.status_code != requests.codes.ok:
            if request.status_code == 404:
                return None
            elif request.status_code == 403:
                if "Invalid proxy request signature" in request._content:
                    raise SynergyError("cannot retrieve the 'userdata' value: "
                                       "check the 'metadata_proxy_shared_"
                                       "secret' attribute value")
                else:
                    request.raise_for_status()
            else:
                request.raise_for_status()

        if request.text:
            return request.text
        else:
            return None

    def getFlavors(self):
        url = "flavors/detail"

        try:
            response_data = self.getResource(url, method="GET")
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise SynergyError("error on retrieving the flavors list: %s"
                               % response)

        flavors = []

        if response_data:
            for flavor_data in response_data["flavors"]:
                flavor = Flavor()
                flavor.setId(flavor_data["id"])
                flavor.setName(flavor_data["name"])
                flavor.setVCPUs(flavor_data["vcpus"])
                flavor.setMemory(flavor_data["ram"])
                flavor.setStorage(flavor_data["disk"])

                flavors.append(flavor)

        return flavors

    def getFlavor(self, id):
        try:
            response_data = self.getResource("flavors/" + id, "GET")
        except requests.exceptions.HTTPError as ex:
            raise SynergyError("error on retrieving the flavor info (id=%r)"
                               ": %s" % (id, ex.response.json()))

        flavor = None

        if response_data:
            flavor_data = response_data["flavor"]

            flavor = Flavor()
            flavor.setId(flavor_data["id"])
            flavor.setName(flavor_data["name"])
            flavor.setVCPUs(flavor_data["vcpus"])
            flavor.setMemory(flavor_data["ram"])
            flavor.setStorage(flavor_data["disk"])

        return flavor

    def getServers(self, detail=False, status=None):
        params = {}
        if status:
            params["status"] = status

        url = "servers/detail"

        try:
            response_data = self.getResource(url, "GET", params)
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise SynergyError("error on retrieving the servers list"
                               ": %s" % (id, response))

        servers = []

        if response_data:
            for server_data in response_data["servers"]:
                server = Server()
                server.setId(server_data["id"])
                server.setName(server_data["name"])
                server.setKeyName(server_data["key_name"])
                server.setMetadata(server_data["metadata"])
                server.setUserData(server_data.get("OS-EXT-SRV-ATTR:user_data",
                                                   None))
                server.setType()
                server.setState(server_data["OS-EXT-STS:vm_state"])
                server.setUserId(server_data["user_id"])
                server.setProjectId(server_data["tenant_id"])
                server.setCreatedAt(server_data["created"])
                server.setUpdatedAt(server_data.get("updated", None))
                server.setLaunchedAt(
                    server_data.get("OS-SRV-USG:launched_at", None))
                server.setTerminatedAt(
                    server_data.get("OS-SRV-USG:terminated_at", None))

                if detail:
                    server.setFlavor(self.getFlavor(
                        server_data["flavor"]["id"]))

                servers.append(server)

        return servers

    def getServer(self, id, detail=False):
        try:
            response_data = self.getResource("servers/" + id, "GET")
        except requests.exceptions.HTTPError as ex:
            raise SynergyError("error on retrieving the server info (id=%r)"
                               ": %s" % (id, ex.response.json()))

        server = None

        if response_data:
            server_data = response_data["server"]

            server = Server()
            server.setId(server_data["id"])
            server.setName(server_data["name"])
            server.setKeyName(server_data["key_name"])
            server.setMetadata(server_data["metadata"])
            server.setUserData(server_data.get("OS-EXT-SRV-ATTR:user_data",
                                               None))
            server.setType()
            server.setState(server_data["OS-EXT-STS:vm_state"])
            server.setUserId(server_data["user_id"])
            server.setProjectId(server_data["tenant_id"])
            server.setCreatedAt(server_data["created"])
            server.setUpdatedAt(server_data.get("updated", None))
            server.setLaunchedAt(
                server_data.get("OS-SRV-USG:launched_at", None))
            server.setTerminatedAt(
                server_data.get("OS-SRV-USG:terminated_at", None))

            if detail:
                server.setFlavor(self.getFlavor(server_data["flavor"]["id"]))

        return server

    def buildServer(self, request):
        self.novaConductorComputeAPI.build_instance(
            request.getContext(),
            request.getAction(),
            request.getData())

    def deleteServer(self, server):
        if not server:
            return

        id = server.getId()
        url = "servers/%s" % id

        try:
            response_data = self.getResource(url, "DELETE")
        except requests.exceptions.HTTPError as ex:
            raise SynergyError("error on deleting the server (id=%r)"
                               ": %s" % (id, ex.response.json()))

        if response_data:
            response_data = response_data["server"]

        return response_data

    def startServer(self, server):
        if not server:
            return

        id = server.getId()
        data = {"os-start": None}
        url = "servers/%s/action" % id

        try:
            response_data = self.getResource(url, "POST", data)
        except requests.exceptions.HTTPError as ex:
            raise SynergyError("error on starting the server %s"
                               ": %s" % (id, ex.response.json()))

        if response_data:
            response_data = response_data["server"]

        return response_data

    def stopServer(self, server):
        if not server:
            return

        id = server.getId()
        data = {"os-stop": None}
        url = "servers/%s/action" % id

        try:
            response_data = self.getResource(url, "POST", data)
        except requests.exceptions.HTTPError as ex:
            raise SynergyError("error on stopping the server info (id=%r)"
                               ": %s" % (id, ex.response.json()))

        if response_data:
            response_data = response_data["server"]

        return response_data

    def setServerMetadata(self, server, key, value):
        if not server:
            return

        id = server.getId()
        data = {"metadata": {key: value}}
        url = "servers/%s/metadata" % id

        try:
            response_data = self.getResource(url, "POST", data)
        except requests.exceptions.HTTPError as ex:
            raise SynergyError("error on setting the metadata (id=%r)"
                               ": %s" % (id, ex.response.json()))

        if response_data:
            response_data = response_data["metadata"]

        return response_data

    def setQuotaTypeServer(self, server):
        if not server:
            return

        QUERY = "insert into nova.instance_metadata (created_at, `key`, " \
            "`value`, instance_uuid) values (%s, 'quota', %s, %s)"

        connection = self.db_engine.connect()
        trans = connection.begin()

        quota_type = "private"

        if server.isEphemeral():
            quota_type = "shared"

        try:
            connection.execute(QUERY,
                               [server.getCreatedAt(), quota_type,
                                server.getId()])

            trans.commit()
        except SQLAlchemyError as ex:
            trans.rollback()
            raise SynergyError(ex.message)
        finally:
            connection.close()

    def getHosts(self):
        data = {}
        url = "os-hosts"
        # /%s" % id

        try:
            response_data = self.getResource(url, "GET", data)
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise SynergyError("error on retrieving the hypervisors list: %s"
                               % response["badRequest"]["message"])

        if response_data:
            response_data = response_data["hosts"]

        return response_data

    def getHost(self, name):
        data = {}
        url = "os-hosts/%s" % name

        try:
            response_data = self.getResource(url, "GET", data)
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise SynergyError("error on retrieving the hypervisor info (id=%r"
                               "): %s" % (id,
                                          response["badRequest"]["message"]))

        if response_data:
            response_data = response_data["host"]

        return response_data

    def getHypervisors(self):
        data = {"os-stop": None}
        url = "os-hypervisors/detail"

        try:
            response_data = self.getResource(url, "GET", data)
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise SynergyError("error on retrieving the hypervisors list: %s"
                               % response["badRequest"]["message"])

        hypervisors = []

        if response_data:
            hypervisors_data = response_data["hypervisors"]

        for hypervisor_data in hypervisors_data:
            hypervisor = Hypervisor()
            hypervisor.setId(hypervisor_data["id"])
            hypervisor.setIP(hypervisor_data["host_ip"])
            hypervisor.setName(hypervisor_data["hypervisor_hostname"])
            hypervisor.setType(hypervisor_data["hypervisor_type"])
            hypervisor.setState(hypervisor_data["state"])
            hypervisor.setStatus(hypervisor_data["status"])
            hypervisor.setWorkload(hypervisor_data["current_workload"])
            hypervisor.setVMs(hypervisor_data["running_vms"])
            hypervisor.setVCPUs(hypervisor_data["vcpus"])
            hypervisor.setVCPUs(hypervisor_data["vcpus_used"], used=True)
            hypervisor.setMemory(hypervisor_data["memory_mb"])
            hypervisor.setMemory(hypervisor_data["memory_mb_used"], used=True)
            hypervisor.setStorage(hypervisor_data["local_gb"])
            hypervisor.setStorage(hypervisor_data["local_gb_used"], used=True)

            hypervisors.append(hypervisor)

        return hypervisors

    def getHypervisor(self, id):
        data = {"os-stop": None}
        url = "os-hypervisors/%s" % id

        try:
            response_data = self.getResource(url, "GET", data)
        except requests.exceptions.HTTPError as ex:
            raise SynergyError("error on retrieving the hypervisor info (id=%r"
                               "): %s" % (id, ex.response.json()))

        hypervisor = None

        if response_data:
            hypervisor_data = response_data["hypervisor"]

            hypervisor = Hypervisor()
            hypervisor.setId(hypervisor_data["id"])
            hypervisor.setIP(hypervisor_data["host_ip"])
            hypervisor.setName(hypervisor_data["hypervisor_hostname"])
            hypervisor.setType(hypervisor_data["hypervisor_type"])
            hypervisor.setState(hypervisor_data["state"])
            hypervisor.setStatus(hypervisor_data["status"])
            hypervisor.setWorkload(hypervisor_data["current_workload"])
            hypervisor.setVMs(hypervisor_data["running_vms"])
            hypervisor.setVCPUs(hypervisor_data["vcpus"])
            hypervisor.setVCPUs(hypervisor_data["vcpus_used"], used=True)
            hypervisor.setMemory(hypervisor_data["memory_mb"])
            hypervisor.setMemory(hypervisor_data["memory_mb_used"], used=True)
            hypervisor.setStorage(hypervisor_data["local_gb"])
            hypervisor.setStorage(hypervisor_data["local_gb_used"], used=True)

        return hypervisor

    def getQuota(self, id=None, is_class=False, defaults=False):
        if defaults:
            try:
                url = "os-quota-sets/defaults"
                response_data = self.getResource(url, "GET")
            except requests.exceptions.HTTPError as ex:
                raise SynergyError("error on retrieving the quota defaults"
                                   ": %s" % ex.response.json())
        elif id is not None:
            if is_class:
                url = "os-quota-class-sets/%s" % id
            else:
                url = "os-quota-sets/%s" % id

            try:
                response_data = self.getResource(url, "GET")

                if is_class:
                    quota_data = response_data["quota_class_set"]
                else:
                    quota_data = response_data["quota_set"]
            except requests.exceptions.HTTPError as ex:
                raise SynergyError("error on retrieving the quota info (id=%r)"
                                   ": %s" % (id, ex.response.json()))
        else:
            raise SynergyError("wrong arguments")

        quota = None

        if quota_data:
            quota = Quota()
            quota.setId(id)
            quota.setSize("vcpus", quota_data["cores"])
            quota.setSize("memory", quota_data["ram"])
            quota.setSize("instances", quota_data["instances"])

        return quota

    def updateQuota(self, quota, is_class=False):
        if is_class:
            url = "os-quota-class-sets/%s" % quota.getId()

            qs = {"quota_class_set": {"cores": quota.getSize("vcpus"),
                                      "ram": quota.getSize("memory"),
                                      "instances": quota.getSize("instances")}}
        else:
            url = "os-quota-sets/%s" % quota.getId()

            qs = {"quota_set": {"force": True,
                                "cores": quota.getSize("vcpus"),
                                "ram": quota.getSize("memory"),
                                "instances": quota.getSize("instances")}}

        try:
            self.getResource(url, "PUT", qs)
        except requests.exceptions.HTTPError as ex:
            raise SynergyError("error on updating the quota info (id=%r)"
                               ": %s" % (id, ex.response.json()))

    def getResource(self, resource, method, data=None):
        self.keystone_manager.authenticate()
        token = self.keystone_manager.getToken()
        service = token.getService("nova")

        if not service:
            raise SynergyError("nova service not found!")

        endpoint = service.getEndpoint("public")

        if not endpoint:
            raise SynergyError("nova endpoint not found!")

        url = endpoint.getURL() + "/" + resource

        headers = {"Content-Type": "application/json",
                   "Accept": "application/json",
                   "User-Agent": "python-novaclient",
                   "X-Auth-Project-Id": token.getProject().getName(),
                   "X-Auth-Token": token.getId()}

        if method == "GET":
            request = requests.get(url, headers=headers,
                                   params=data, timeout=self.timeout,
                                   verify=self.ssl_ca_file,
                                   cert=self.ssl_cert_file)
        elif method == "POST":
            request = requests.post(url,
                                    headers=headers,
                                    data=json.dumps(data),
                                    timeout=self.timeout,
                                    verify=self.ssl_ca_file,
                                    cert=self.ssl_cert_file)
        elif method == "PUT":
            request = requests.put(url,
                                   headers=headers,
                                   data=json.dumps(data),
                                   timeout=self.timeout,
                                   verify=self.ssl_ca_file,
                                   cert=self.ssl_cert_file)
        elif method == "HEAD":
            request = requests.head(url,
                                    headers=headers,
                                    data=json.dumps(data),
                                    timeout=self.timeout,
                                    verify=self.ssl_ca_file,
                                    cert=self.ssl_cert_file)
        elif method == "DELETE":
            request = requests.delete(url,
                                      headers=headers,
                                      data=json.dumps(data),
                                      timeout=self.timeout,
                                      verify=self.ssl_ca_file,
                                      cert=self.ssl_cert_file)
        else:
            raise SynergyError("wrong HTTP method: %s" % method)

        if request.status_code != requests.codes.ok:
            request.raise_for_status()

        if request.text:
            return request.json()
        else:
            return None

    def getTarget(self, topic, exchange=None, namespace=None,
                  version=None, server=None):
        return self.messaging.getTarget(topic=topic,
                                        namespace=namespace,
                                        exchange=exchange,
                                        version=version,
                                        server=server)

    def getRPCClient(self, target, version_cap=None, serializer=None):
        return self.messaging.getRPCClient(target,
                                           version_cap=version_cap,
                                           serializer=serializer)

    def getRPCServer(self, target, endpoints, serializer=None):
        return self.messaging.getRPCServer(target,
                                           endpoints,
                                           serializer=serializer)

    def getNotificationListener(self, targets, endpoints):
        return self.messaging.getNotificationListener(targets, endpoints)

    def getProjectUsage(self, prj_id, from_date, to_date):
        usage = {}
        connection = self.db_engine.connect()

        try:
            QUERY = """select a.user_id, sum(TIMESTAMPDIFF(\
SECOND, IF(a.launched_at<='%(from_date)s', '%(from_date)s', IFNULL(\
a.launched_at, '%(from_date)s')), IF(a.terminated_at>='%(to_date)s', \
'%(to_date)s', IFNULL(a.terminated_at, '%(to_date)s')))*a.memory_mb) as \
memory_usage, sum(TIMESTAMPDIFF(SECOND, IF(a.launched_at<='%(from_date)s', \
'%(from_date)s', IFNULL(a.launched_at, '%(from_date)s')), IF(a.terminated_at\
>='%(to_date)s', '%(to_date)s', IFNULL(a.terminated_at, '%(to_date)s')))\
*a.vcpus) as vcpus_usage from nova.instances as a LEFT OUTER JOIN \
nova.instance_metadata as b ON a.uuid=b.instance_uuid where a.project_id\
='%(prj_id)s' and b.value='shared' and a.launched_at is not NULL and \
a.launched_at<='%(to_date)s' and (a.terminated_at>='%(from_date)s' or \
 a.terminated_at is NULL) group by user_id
""" % {"prj_id": prj_id, "from_date": from_date, "to_date": to_date}

            LOG.debug("persistent servers query: %s" % QUERY)

            result = connection.execute(QUERY)

            # for row in result.fetchall():
            for row in result:
                usage[row[0]] = {"memory": float(row[1]),
                                 "vcpus": float(row[2])}

        except SQLAlchemyError as ex:
            raise SynergyError(ex.message)
        finally:
            connection.close()

        return usage

    def getProjectServers(self, prj_id):
        connection = self.db_engine.connect()
        servers = []

        try:
            # retrieve the amount of resources in terms of cores and memory
            QUERY = """select a.uuid, a.vcpus, a.memory_mb, a.root_gb, \
a.vm_state, a.user_data from nova.instances as a WHERE a.project_id=\
'%(project_id)s'and a.vm_state in ('active', 'building', 'error') and \
a.deleted_at is NULL and a.terminated_at is NULL""" % {"project_id": prj_id}

            LOG.debug("getProjectServers query: %s" % QUERY)

            result = connection.execute(QUERY)

            for row in result.fetchall():
                flavor = Flavor()
                flavor.setVCPUs(row[1])
                flavor.setMemory(row[2])
                flavor.setStorage(row[3])

                server = Server()
                server.setId(row[0])
                server.setState(row[4])
                server.setUserData(row[5])
                server.setFlavor(flavor)

                QUERY = """select `key`, value from nova.instance_metadata \
where instance_uuid='%(id)s' and deleted_at is NULL""" % {"id": server.getId()}

                LOG.debug("getProjectServers query: %s" % QUERY)

                result = connection.execute(QUERY)
                metadata = {}

                for row in result.fetchall():
                    metadata[row[0]] = row[1]

                server.setMetadata(metadata)
                server.setType()

                servers.append(server)
        except SQLAlchemyError as ex:
            raise SynergyError(ex.message)
        finally:
            connection.close()

        return servers

    def getExpiredServers(self, prj_id, server_ids, TTL):
        servers = []
        connection = self.db_engine.connect()

        try:
            # retrieve all expired instances for the specified
            # project and expiration time
            ids = ""

            if server_ids:
                ids = "uuid in ('%s') and " % "', '".join(server_ids)

            QUERY = """select uuid, vcpus, memory_mb, root_gb, \
vm_state, user_data from nova.instances where project_id = \
'%(project_id)s' and deleted_at is NULL and (vm_state='error' or \
(%(server_ids)s vm_state='active' and terminated_at is NULL \
and timestampdiff(minute, launched_at, utc_timestamp()) >= %(expiration)s))\
""" % {"project_id": prj_id, "server_ids": ids, "expiration": TTL}

            LOG.debug("getExpiredServers query: %s" % QUERY)

            result = connection.execute(QUERY)

            for row in result.fetchall():
                flavor = Flavor()
                flavor.setVCPUs(row[1])
                flavor.setMemory(row[2])
                flavor.setStorage(row[3])

                server = Server()
                server.setId(row[0])
                server.setState(row[4])
                server.setUserData(row[5])
                server.setFlavor(flavor)

                QUERY = """select `key`, value from nova.instance_metadata \
where instance_uuid='%(id)s' and deleted_at is NULL""" % {"id": server.getId()}

                LOG.debug("getExpiredServers query: %s" % QUERY)

                result = connection.execute(QUERY)
                metadata = {}

                for row in result.fetchall():
                    metadata[row[0]] = row[1]

                server.setMetadata(metadata)
                server.setType()

                servers.append(server)
        except SQLAlchemyError as ex:
            raise SynergyError(ex.message)
        finally:
            connection.close()

        return servers

    def selectComputes(self, request):
        target = self.messaging.getTarget(topic='scheduler',
                                          exchange="nova",
                                          version="4.0")

        client = self.messaging.getRPCClient(target)
        cctxt = client.prepare(version='4.0')

        request_spec = {
            'image': request.getImage(),
            'instance_properties': request.getInstance(),
            'instance_type': request.getFilterProperties()['instance_type'],
            'num_instances': 1}

        hosts = cctxt.call(request.getContext(),
                           'select_destinations',
                           request_spec=request_spec,
                           filter_properties=request.getFilterProperties())

        computes = []

        for host in hosts:
            compute = Compute()
            compute.setHost(host['host'])
            compute.setNodeName(host['nodename'])
            compute.setLimits(host['limits'])

            computes.append(compute)

        return computes

    def getBlockDeviceMappingList(self, server_id):
        connection = self.db_engine.connect()
        blockDeviceMapList = []

        try:
            QUERY = """select id, created_at, updated_at, deleted_at, \
device_name, delete_on_termination, snapshot_id, volume_id, volume_size, \
no_device, connection_info, deleted, source_type, destination_type, \
guest_format, device_type, disk_bus, boot_index, image_id from \
nova.block_device_mapping where instance_uuid='%(server_id)s'
""" % {"server_id": server_id}

            LOG.debug("getBlockDeviceMapping query: %s" % QUERY)

            result = connection.execute(QUERY)

            for row in result.fetchall():
                blockDeviceMap = BlockDeviceMapping(row[0])
                blockDeviceMap.setCreatedAt(row[1])
                blockDeviceMap.setUpdatedAt(row[2])
                blockDeviceMap.setDeletedAt(row[3])
                blockDeviceMap.setDeviceName(row[4])
                blockDeviceMap.setDeleteOnTermination(row[5])
                blockDeviceMap.setSnapshotId(row[6])
                blockDeviceMap.setVolumeId(row[7])
                blockDeviceMap.setVolumeSize(row[8])
                blockDeviceMap.setNoDevice(row[9])
                blockDeviceMap.setConnectionInfo(row[10])
                blockDeviceMap.setDeleted(row[11])
                blockDeviceMap.setSourceType(row[12])
                blockDeviceMap.setDestinationType(row[13])
                blockDeviceMap.setGuestFormat(row[14])
                blockDeviceMap.setDeviceType(row[15])
                blockDeviceMap.setDiskBus(row[16])
                blockDeviceMap.setBootIndex(row[17])
                blockDeviceMap.setImageId(row[18])
                blockDeviceMap.setInstanceId(server_id)

                blockDeviceMapList.append(blockDeviceMap)
        except SQLAlchemyError as ex:
            raise SynergyError(ex.message)
        finally:
            connection.close()

        return blockDeviceMapList
