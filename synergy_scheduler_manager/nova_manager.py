import common.utils as utils
import ConfigParser
import eventlet
import hashlib
import hmac
import json
import logging
import os.path
import requests

from common.block_device import BlockDeviceMapping
from common.compute import Compute
from common.flavor import Flavor
from common.hypervisor import Hypervisor
from common.quota import Quota
from common.request import Request
from common.server import Server
from nova.baserpc import BaseAPI
from nova.compute.rpcapi import ComputeAPI
from nova.conductor.rpcapi import ComputeTaskAPI
from nova.conductor.rpcapi import ConductorAPI
from nova.objects import base as objects_base

try:
    from oslo_config import cfg
except ImportError:
    from oslo.config import cfg

try:
    import oslo_messaging as oslo_msg
except ImportError:
    import oslo.messaging as oslo_msg

try:
    from oslo_serialization import jsonutils
except ImportError:
    from oslo.serialization import jsonutils

try:
    from oslo_versionedobjects import base as ovo_base
except ImportError:
    from oslo.versionedobjects import base as ovo_base

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
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


LOG = logging.getLogger(__name__)
CONF = cfg.CONF
CONFIG = ConfigParser.SafeConfigParser()


class MessagingAPI(object):

    def __init__(self, transport_url):
        LOG.debug("setting up the AMQP transport url: %s" % transport_url)
        oslo_msg.set_transport_defaults(control_exchange="nova")
        self.TRANSPORT = oslo_msg.get_transport(CONF, url=transport_url)

    def getTarget(self, topic, exchange=None, namespace=None,
                  version=None, server=None):
        return oslo_msg.Target(topic=topic,
                               exchange=exchange,
                               namespace=namespace,
                               version=version,
                               server=server)

    def getRPCClient(self, target, version_cap=None, serializer=None):
        assert self.TRANSPORT is not None

        LOG.info("creating RPC client with target %s" % target)
        return oslo_msg.RPCClient(self.TRANSPORT,
                                  target,
                                  version_cap=version_cap,
                                  serializer=serializer)

    def getRPCServer(self, target, endpoints, serializer=None):
        assert self.TRANSPORT is not None

        LOG.info("creating RPC server with target %s" % target)
        return oslo_msg.get_rpc_server(self.TRANSPORT,
                                       target,
                                       endpoints,
                                       executor="eventlet",
                                       serializer=serializer)

    def getNotificationListener(self, targets, endpoints):
        assert self.TRANSPORT is not None

        LOG.info("creating notification listener with target %s endpoints %s"
                 % (targets, endpoints))
        return oslo_msg.get_notification_listener(self.TRANSPORT,
                                                  targets,
                                                  endpoints,
                                                  allow_requeue=True,
                                                  executor="eventlet")


class NovaBaseRPCAPI(BaseAPI):

    def __init__(self, topic, msg):
        self.target = msg.getTarget(topic=None,
                                    namespace="baseapi",
                                    version="1.1")

        target_synergy = msg.getTarget(topic=topic + "_synergy",
                                       namespace="baseapi",
                                       version="1.0")

        self.client = msg.getRPCClient(target=target_synergy, version_cap=None)

    def ping(self, context, arg, timeout=None):
        try:
            cctxt = self.client.prepare(timeout=timeout)

            return cctxt.call(context, 'ping', arg=arg)
        except Exception as ex:
            LOG.error("NovaBaseRPCAPI ping! %s" % (ex))
            raise ex

    def get_backdoor_port(self, context, host):
        cctxt = self.client.prepare(server=host, version='1.1')
        return cctxt.call(context, 'get_backdoor_port')


class NovaComputeAPI(ComputeAPI):

    def __init__(self, topic, msg):
        self.target = msg.getTarget(topic=topic, version="4.0")
        self.client = msg.getRPCClient(target=msg.getTarget(topic=topic))

    def build_and_run_instance(self, context, instance, host, image,
                               request_spec, filter_properties,
                               admin_password=None, injected_files=None,
                               requested_networks=None, security_groups=None,
                               block_device_mapping=None, node=None,
                               limits=None):
        if not filter_properties.get('force_hosts', None):
            filter_properties['limits'] = limits

        bdms = []
        for block_device in block_device_mapping:
            bdms.append(block_device.serialize())

        version = '4.0'
        cctxt = self.client.prepare(server=host, version=version)
        cctxt.cast(context,
                   'build_and_run_instance',
                   instance=instance,
                   image=image,
                   request_spec=request_spec,
                   filter_properties=filter_properties,
                   admin_password=admin_password,
                   injected_files=injected_files,
                   requested_networks=requested_networks,
                   security_groups=security_groups,
                   block_device_mapping=bdms,
                   node=node,
                   limits=limits)


class NovaConductorAPI(ConductorAPI):

    def __init__(self, topic, msg):
        report_interval = cfg.IntOpt("report_interval",
                                     default=10,
                                     help="Seconds between nodes reporting "
                                          "state to datastore")
        CONF.register_opt(report_interval)

        self.target = msg.getTarget(topic=topic, version="3.0")

        self.client = msg.getRPCClient(
            target=msg.getTarget(topic=topic + "_synergy", version="3.0"),
            version_cap=None)

    def provider_fw_rule_get_all(self, context):
        cctxt = self.client.prepare()
        return cctxt.call(context, 'provider_fw_rule_get_all')

    def object_class_action(self, context, objname, objmethod, objver,
                            args, kwargs):
        versions = ovo_base.obj_tree_get_versions(objname)
        return self.object_class_action_versions(context,
                                                 objname,
                                                 objmethod,
                                                 versions,
                                                 args, kwargs)

    def object_class_action_versions(self, context, objname, objmethod,
                                     object_versions, args, kwargs):
        cctxt = self.client.prepare()
        return cctxt.call(context, 'object_class_action_versions',
                          objname=objname, objmethod=objmethod,
                          object_versions=object_versions,
                          args=args, kwargs=kwargs)

    def object_action(self, context, objinst, objmethod, args, kwargs):
        cctxt = self.client.prepare()
        return cctxt.call(context, 'object_action', objinst=objinst,
                          objmethod=objmethod, args=args, kwargs=kwargs)

    def object_backport_versions(self, context, objinst, object_versions):
        cctxt = self.client.prepare()
        return cctxt.call(context, 'object_backport_versions', objinst=objinst,
                          object_versions=object_versions)


class NovaConductorComputeAPI(ComputeTaskAPI):

    def __init__(self, topic, scheduler_manager, keystone_manager, msg):
        self.topic = topic
        self.scheduler_manager = scheduler_manager
        self.keystone_manager = keystone_manager
        self.messagingAPI = msg
        serializer = objects_base.NovaObjectSerializer()

        self.target = self.messagingAPI.getTarget(topic=topic,
                                                  namespace="compute_task",
                                                  version="1.11")

        self.client = self.messagingAPI.getRPCClient(
            target=oslo_msg.Target(topic=topic + "_synergy",
                                   namespace="compute_task",
                                   version="1.10"),
            serializer=serializer)

    def build_instances(self, context, instances, image, filter_properties,
                        admin_password, injected_files, requested_networks,
                        security_groups, block_device_mapping=None,
                        legacy_bdm=True):
        # token = self.keystone_manager.validateToken(context["auth_token"])

        for instance in instances:
            try:

                request = Request.build(context, instance, image,
                                        filter_properties, admin_password,
                                        injected_files, requested_networks,
                                        security_groups, block_device_mapping,
                                        legacy_bdm)

                self.scheduler_manager.processRequest(request)
            except Exception as ex:
                LOG.error("Exception has occured", exc_info=1)
                LOG.error(ex)

    def build_instance(self, context, instance, image, filter_properties,
                       admin_password, injected_files, requested_networks,
                       security_groups, block_device_mapping=None,
                       legacy_bdm=True):
        try:
            version = '1.10'
            if not self.client.can_send_version(version):
                version = '1.9'
                if 'instance_type' in filter_properties:
                    flavor = filter_properties['instance_type']
                    flavor_p = objects_base.obj_to_primitive(flavor)
                    filter_properties = dict(filter_properties,
                                             instance_type=flavor_p)
            kw = {'instances': [instance],
                  'image': image,
                  'filter_properties': filter_properties,
                  'admin_password': admin_password,
                  'injected_files': injected_files,
                  'requested_networks': requested_networks,
                  'security_groups': security_groups}

            if not self.client.can_send_version(version):
                version = '1.8'
                kw['requested_networks'] = kw['requested_networks'].as_tuples()
            if not self.client.can_send_version('1.7'):
                version = '1.5'
                bdm_p = objects_base.obj_to_primitive(block_device_mapping)
                kw.update({'block_device_mapping': bdm_p,
                           'legacy_bdm': legacy_bdm})

            cctxt = self.client.prepare(version_cap=version)
            cctxt.cast(context, 'build_instances', **kw)
        except Exception as ex:
            LOG.error("Exception has occured", exc_info=1)
            LOG.error(ex)
            raise ex

    def migrate_server(self, context, instance, scheduler_hint, live, rebuild,
                       flavor, block_migration, disk_over_commit,
                       reservations=None, clean_shutdown=True,
                       request_spec=None):
        kw = {'instance': instance, 'scheduler_hint': scheduler_hint,
              'live': live, 'rebuild': rebuild, 'flavor': flavor,
              'block_migration': block_migration,
              'disk_over_commit': disk_over_commit,
              'reservations': reservations,
              'clean_shutdown': clean_shutdown,
              'request_spec': request_spec,
              }

        version = '1.13'
        if not self.client.can_send_version(version):
            del kw['request_spec']
            version = '1.11'
        if not self.client.can_send_version(version):
            del kw['clean_shutdown']
            version = '1.10'
        if not self.client.can_send_version(version):
            kw['flavor'] = objects_base.obj_to_primitive(flavor)
            version = '1.6'
        if not self.client.can_send_version(version):
            kw['instance'] = jsonutils.to_primitive(
                objects_base.obj_to_primitive(instance))
            version = '1.4'

        cctxt = self.client.prepare(version=version)
        return cctxt.call(context, 'migrate_server', **kw)

    def unshelve_instance(self, context, instance):
        cctxt = self.client.prepare(version='1.3')
        cctxt.cast(context, 'unshelve_instance', instance=instance)

    def rebuild_instance(self, ctxt, instance, new_pass, injected_files,
                         image_ref, orig_image_ref, orig_sys_metadata, bdms,
                         recreate=False, on_shared_storage=False, host=None,
                         preserve_ephemeral=False, kwargs=None):
        cctxt = self.client.prepare(version='1.8')
        cctxt.cast(ctxt, 'rebuild_instance',
                   instance=instance, new_pass=new_pass,
                   injected_files=injected_files, image_ref=image_ref,
                   orig_image_ref=orig_image_ref,
                   orig_sys_metadata=orig_sys_metadata, bdms=bdms,
                   recreate=recreate, on_shared_storage=on_shared_storage,
                   preserve_ephemeral=preserve_ephemeral,
                   host=host)


class NovaManager(Manager):

    def __init__(self):
        super(NovaManager, self).__init__("NovaManager")

        self.config_opts = [
            cfg.StrOpt("nova_conf",
                       help="the nova.conf path",
                       default=None,
                       required=False),
            cfg.StrOpt("amqp_backend",
                       help="the amqp backend tpye (e.g. rabbit, qpid)",
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
            cfg.StrOpt("amqp_virt_host",
                       help="the amqp virtual host",
                       default="/",
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
            cfg.FloatOpt("cpu_allocation_ratio",
                         help="the cpu allocation ratio",
                         default=float(16)),
            cfg.FloatOpt("ram_allocation_ratio",
                         help="the ram allocation ratio",
                         default=float(1.5)),
            cfg.StrOpt("db_connection",
                       help="the NOVA database connection",
                       default=None,
                       required=False),
            cfg.StrOpt("host",
                       help="the host name",
                       default="localhost",
                       required=False),
            cfg.IntOpt("timeout",
                       help="set the http connection timeout",
                       default=60,
                       required=False)
        ]

    def setup(self):
        eventlet.monkey_patch(os=False)

        self.timeout = CONF.NovaManager.timeout

        if self.getManager("KeystoneManager") is None:
            raise Exception("KeystoneManager not found!")

        if self.getManager("SchedulerManager") is None:
            raise Exception("SchedulerManager not found!")

        self.keystone_manager = self.getManager("KeystoneManager")
        self.scheduler_manager = self.getManager("SchedulerManager")

        amqp_backend = self.getParameter("amqp_backend", "NovaManager")

        amqp_host = self.getParameter("amqp_host", "NovaManager")

        amqp_port = self.getParameter("amqp_port", "NovaManager")

        amqp_user = self.getParameter("amqp_user", "NovaManager")

        amqp_password = self.getParameter("amqp_password", "NovaManager")

        amqp_virt_host = self.getParameter("amqp_virt_host", "NovaManager")

        db_connection = self.getParameter("db_connection", "NovaManager")

        host = self.getParameter("host", "NovaManager")

        conductor_topic = self.getParameter("conductor_topic", "NovaManager")

        compute_topic = self.getParameter("compute_topic", "NovaManager")

        scheduler_topic = self.getParameter("scheduler_topic", "NovaManager")

        host = amqp_host

        if CONF.NovaManager.nova_conf is not None:
            if os.path.isfile(CONF.NovaManager.nova_conf):
                CONFIG.read(CONF.NovaManager.nova_conf)
            else:
                raise Exception("nova configuration file not found at %s!"
                                % CONF.NovaManager.nova_conf)

            host = self.getParameter("my_ip", "DEFAULT", default=host)

            conductor_topic = self.getParameter("conductor_topic", "DEFAULT",
                                                default="conductor")

            compute_topic = self.getParameter("compute_topic", "DEFAULT",
                                              default="compute")

            scheduler_topic = self.getParameter("scheduler_topic",
                                                "DEFAULT",
                                                default="scheduler")

            db_connection = self.getParameter("connection", "database")

            amqp_backend = self.getParameter("rpc_backend", "DEFAULT")

            if amqp_backend == "rabbit":
                amqp_host = self.getParameter("rabbit_host",
                                              "oslo_messaging_rabbit",
                                              default="localhost")

                amqp_port = self.getParameter("rabbit_port",
                                              "oslo_messaging_rabbit",
                                              default="5672")

                amqp_virt_host = self.getParameter("rabbit_virtual_host",
                                                   "oslo_messaging_rabbit",
                                                   default="/")

                amqp_user = self.getParameter("rabbit_userid",
                                              "oslo_messaging_rabbit",
                                              default="guest")

                amqp_password = self.getParameter("rabbit_password",
                                                  "oslo_messaging_rabbit")
            elif amqp_backend == "qpid":
                amqp_host = self.getParameter("qpid_hostname",
                                              "oslo_messaging_qpid",
                                              default="localhost")

                amqp_port = self.getParameter("qpid_port",
                                              "oslo_messaging_qpid",
                                              default="5672")

                amqp_user = self.getParameter("qpid_username",
                                              "oslo_messaging_qpid")

                amqp_password = self.getParameter("qpid_password",
                                                  "oslo_messaging_qpid")
            else:
                raise Exception("unsupported amqp backend found: %s!"
                                % amqp_backend)

        if not amqp_backend:
            raise Exception("amqp_backend not defined!")

        if not amqp_user:
            raise Exception("amqp_user not defined!")

        if not amqp_password:
            raise Exception("amqp_password not defined!")

        if not amqp_host:
            raise Exception("amqp_host not defined!")

        if not amqp_port:
            raise Exception("amqp_port not defined!")

        if not amqp_virt_host:
            raise Exception("amqp_virt_host not defined!")

        if not db_connection:
            raise Exception("db_connection not defined!")

        if not conductor_topic:
            raise Exception("conductor_topic not defined!")

        if not compute_topic:
            raise Exception("compute_topic not defined!")

        if not scheduler_topic:
            raise Exception("scheduler_topic not defined!")

        try:
            LOG.debug("setting up the NOVA database connection: %s"
                      % db_connection)

            self.db_engine = create_engine(db_connection)

            transport_url = "{b}://{user}:{password}@{host}:{port}/{virt_host}"
            transport_url = transport_url.format(
                b=amqp_backend,
                user=amqp_user,
                password=amqp_password,
                host=amqp_host,
                port=amqp_port,
                virt_host=amqp_virt_host)

            self.messagingAPI = MessagingAPI(transport_url)

            self.novaBaseRPCAPI = NovaBaseRPCAPI(conductor_topic,
                                                 self.messagingAPI)

            self.novaConductorAPI = NovaConductorAPI(conductor_topic,
                                                     self.messagingAPI)

            self.novaConductorComputeAPI = NovaConductorComputeAPI(
                conductor_topic,
                self.scheduler_manager,
                self.keystone_manager,
                self.messagingAPI)

            self.conductor_rpc = self.messagingAPI.getRPCServer(
                target=self.messagingAPI.getTarget(topic=conductor_topic,
                                                   server=host),
                endpoints=[self.novaBaseRPCAPI,
                           self.novaConductorAPI,
                           self.novaConductorComputeAPI])

            self.conductor_rpc.start()

            self.novaComputeAPI = NovaComputeAPI(compute_topic,
                                                 self.messagingAPI)

            self.compute_rpc = self.messagingAPI.getRPCServer(
                target=self.messagingAPI.getTarget(topic=compute_topic,
                                                   server=host),
                endpoints=[self.novaComputeAPI])
        except Exception as ex:
            LOG.error("Exception has occured", exc_info=1)
            LOG.error("NovaManager initialization failed! %s" % (ex))
            raise ex

    def execute(self, command, *args, **kargs):
        raise Exception("command=%r not supported!" % command)

    def task(self):
        pass

    def destroy(self):
        pass

    def getParameter(self, name, section="DEFAULT",
                     default=None, fallback=False):
        if section != "NovaManager":
            try:
                return CONFIG.get(section, name)
            except Exception:
                if fallback is True:
                    raise Exception("No attribute %r found in [%s] section of "
                                    "nova.conf" % (name, section))
                else:
                    LOG.info("No attribute %r found in [%s] section of "
                             "nova.conf, using default: %r"
                             % (name, section, default))

                    return default
        else:
            result = CONF.NovaManager.get(name, None)

            if result is not None:
                return result

            if fallback is True:
                raise Exception("No attribute %r found in [NovaManager] "
                                "section of synergy.conf" % name)
            else:
                LOG.info("No attribute %r found in in [NovaManager] of "
                         "synergy.conf, using default: %r" % (name, default))
                return default

    def getUserData(self, server):
        if not server:
            return None

        secret = self.getParameter("metadata_proxy_shared_secret", "neutron")

        if not secret:
            return Exception("'metadata_proxy_shared_secret' "
                             "attribute not defined in nova.conf")

        digest = hmac.new(secret, server.getId(), hashlib.sha256).hexdigest()

        self.keystone_manager.authenticate()
        token = self.keystone_manager.getToken()
        service = token.getService("nova")

        if not service:
            raise Exception("nova service not found!")

        endpoint = service.getEndpoint("public")

        if not endpoint:
            raise Exception("nova endpoint not found!")

        url = endpoint.getURL()
        url = url[:url.rfind(":") + 1] + "8775/openstack/2015-10-15/user_data"

        headers = {"Content-Type": "application/text",
                   "Accept": "application/text",
                   "User-Agent": "synergy",
                   "x-instance-id": server.getId(),
                   "x-tenant-id": server.getProjectId(),
                   "x-instance-id-signature": digest}

        request = requests.get(url, headers=headers, timeout=self.timeout)

        if request.status_code != requests.codes.ok:
            if request.status_code == 404:
                return None
            elif request.status_code == 403:
                if "Invalid proxy request signature" in request._content:
                    raise Exception("cannot retrieve the 'userdata' value: "
                                    "check the 'metadata_proxy_shared_secret'"
                                    " attribute value")
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
            raise Exception("error on retrieving the flavors list: %s"
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
            raise Exception("error on retrieving the flavor info (id=%r)"
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
            raise Exception("error on retrieving the servers list"
                            ": %s" % (id, response))

        servers = []

        if response_data:
            for server_data in response_data["servers"]:
                server = Server()
                server.setId(server_data["id"])
                server.setName(server_data["name"])
                server.setKeyName(server_data["key_name"])
                server.setMetadata(server_data["metadata"])
                server.setState(server_data["OS-EXT-STS:vm_state"])
                server.setUserId(server_data["user_id"])
                server.setProjectId(server_data["tenant_id"])
                server.setCreatedAt(server_data["created"])
                server.setUpdatedAt(server_data.get("updated", None))
                server.setLaunchedAt(
                    server_data.get("OS-SRV-USG:launched_at", None))
                server.setTerminatedAt(
                    server_data.get("OS-SRV-USG:terminated_at", None))

                if "user_data" in server_data:
                    user_data = server_data["user_data"]
                    server.setUserData(utils.decodeBase64(user_data))

                if detail:
                    server.setFlavor(self.getFlavor(
                        server_data["flavor"]["id"]))

                servers.append(server)

        return servers

    def getServer(self, id, detail=False):
        try:
            response_data = self.getResource("servers/" + id, "GET")
        except requests.exceptions.HTTPError as ex:
            raise Exception("error on retrieving the server info (id=%r)"
                            ": %s" % (id, ex.response.json()))

        server = None

        if response_data:
            server_data = response_data["server"]

            server = Server()
            server.setId(server_data["id"])
            server.setName(server_data["name"])
            server.setKeyName(server_data["key_name"])
            server.setMetadata(server_data["metadata"])
            server.setState(server_data["OS-EXT-STS:vm_state"])
            server.setUserId(server_data["user_id"])
            server.setProjectId(server_data["tenant_id"])
            server.setCreatedAt(server_data["created"])
            server.setUpdatedAt(server_data.get("updated", None))
            server.setLaunchedAt(
                server_data.get("OS-SRV-USG:launched_at", None))
            server.setTerminatedAt(
                server_data.get("OS-SRV-USG:terminated_at", None))

            if "user_data" in server_data:
                user_data = server_data["user_data"]
                server.setUserData(utils.decodeBase64(user_data))

            if detail:
                server.setFlavor(self.getFlavor(server_data["flavor"]["id"]))

        return server

    def buildServer(self, request, compute=None):
        if compute:
            reqId = request.getId()

            self.novaComputeAPI.build_and_run_instance(
                request.getContext(),
                request.getInstance(),
                compute.getHost(),
                request.getImage(),
                request.getInstance(),
                request.getFilterProperties(),
                admin_password=request.getAdminPassword(),
                injected_files=request.getInjectedFiles(),
                requested_networks=request.getRequestedNetworks(),
                security_groups=request.getSecurityGroups(),
                block_device_mapping=self.getBlockDeviceMappingList(reqId),
                node=compute.getNodeName(),
                limits=compute.getLimits())
        else:
            self.novaConductorComputeAPI.build_instance(
                context=request.getContext(),
                instance=request.getInstance(),
                image=request.getImage(),
                filter_properties=request.getFilterProperties(),
                admin_password=request.getAdminPassword(),
                injected_files=request.getInjectedFiles(),
                requested_networks=request.getRequestedNetworks(),
                security_groups=request.getSecurityGroups(),
                block_device_mapping=request.getBlockDeviceMapping(),
                legacy_bdm=request.getLegacyBDM())

    def deleteServer(self, server):
        if not server:
            return

        id = server.getId()
        url = "servers/%s" % id

        try:
            response_data = self.getResource(url, "DELETE")
        except requests.exceptions.HTTPError as ex:
            raise Exception("error on deleting the server (id=%r)"
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
            raise Exception("error on starting the server info (id=%r)"
                            ": %s" % (id, ex.response.json()))

        if response_data:
            response_data = response_data["server"]

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
            raise Exception(ex.message)
        finally:
            connection.close()

    def stopServer(self, server):
        if not server:
            return

        id = server.getId()
        data = {"os-stop": None}
        url = "servers/%s/action" % id

        try:
            response_data = self.getResource(url, "POST", data)
        except requests.exceptions.HTTPError as ex:
            raise Exception("error on stopping the server info (id=%r)"
                            ": %s" % (id, ex.response.json()))

        if response_data:
            response_data = response_data["server"]

        return response_data

    def getHosts(self):
        data = {}
        url = "os-hosts"
        # /%s" % id

        try:
            response_data = self.getResource(url, "GET", data)
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise Exception("error on retrieving the hypervisors list: %s"
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
            raise Exception("error on retrieving the hypervisor info (id=%r)"
                            ": %s" % (id, response["badRequest"]["message"]))

        if response_data:
            response_data = response_data["host"]

        return response_data

    def getHypervisors(self):
        data = {"os-stop": None}
        url = "os-hypervisors/detail"

        try:
            response_data = self.getResource(url, "GET", data)
        except requests.exceptions.HTTPError as ex:
            LOG.info(ex)
            response = ex.response.json()
            raise Exception("error on retrieving the hypervisors list: %s"
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
            raise Exception("error on retrieving the hypervisor info (id=%r)"
                            ": %s" % (id, ex.response.json()))

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
                raise Exception("error on retrieving the quota defaults"
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
                raise Exception("error on retrieving the quota info (id=%r)"
                                ": %s" % (id, ex.response.json()))
        else:
            raise Exception("wrong arguments")

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

            qs = {"quota_class_set": {"force": True,
                                      "cores": quota.getSize("vcpus"),
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
            raise Exception("error on updating the quota info (id=%r)"
                            ": %s" % (id, ex.response.json()))

    def getResource(self, resource, method, data=None):
        self.keystone_manager.authenticate()
        token = self.keystone_manager.getToken()
        service = token.getService("nova")

        if not service:
            raise Exception("nova service not found!")

        endpoint = service.getEndpoint("public")

        if not endpoint:
            raise Exception("nova endpoint not found!")

        url = endpoint.getURL() + "/" + resource

        headers = {"Content-Type": "application/json",
                   "Accept": "application/json",
                   "User-Agent": "python-novaclient",
                   "X-Auth-Project-Id": token.getProject().getName(),
                   "X-Auth-Token": token.getId()}

        if method == "GET":
            request = requests.get(url, headers=headers,
                                   params=data, timeout=self.timeout)
        elif method == "POST":
            request = requests.post(url,
                                    headers=headers,
                                    data=json.dumps(data),
                                    timeout=self.timeout)
        elif method == "PUT":
            request = requests.put(url,
                                   headers=headers,
                                   data=json.dumps(data),
                                   timeout=self.timeout)
        elif method == "HEAD":
            request = requests.head(url,
                                    headers=headers,
                                    data=json.dumps(data),
                                    timeout=self.timeout)
        elif method == "DELETE":
            request = requests.delete(url,
                                      headers=headers,
                                      data=json.dumps(data),
                                      timeout=self.timeout)
        else:
            raise Exception("wrong HTTP method: %s" % method)

        if request.status_code != requests.codes.ok:
            request.raise_for_status()

        if request.text:
            return request.json()
        else:
            return None

    def getTarget(self, topic, exchange=None, namespace=None,
                  version=None, server=None):
        return self.messagingAPI.getTarget(topic=topic,
                                           namespace=namespace,
                                           exchange=exchange,
                                           version=version,
                                           server=server)

    def getRPCClient(self, target, version_cap=None, serializer=None):
        return self.messagingAPI.getRPCClient(target,
                                              version_cap=version_cap,
                                              serializer=serializer)

    def getRPCServer(self, target, endpoints, serializer=None):
        return self.messagingAPI.getRPCServer(target,
                                              endpoints,
                                              serializer=serializer)

    def getNotificationListener(self, targets, endpoints):
        return self.messagingAPI.getNotificationListener(targets, endpoints)

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
            raise Exception(ex.message)
        finally:
            connection.close()

        return usage

    def getProjectServers(self, prj_id):
        connection = self.db_engine.connect()
        servers = []

        try:
            # retrieve the amount of resources in terms of cores and memory
            QUERY = """select a.uuid, a.vcpus, a.memory_mb, a.root_gb, \
a.vm_state from nova.instances as a WHERE a.project_id='%(project_id)s' \
and a.vm_state in ('active', 'building', 'error') and a.deleted_at is NULL \
and a.terminated_at is NULL""" % {"project_id": prj_id}

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
                server.setFlavor(flavor)

                QUERY = """select `key`, value from nova.instance_metadata \
where instance_uuid='%(id)s' and deleted_at is NULL""" % {"id": server.getId()}

                LOG.debug("getProjectServers query: %s" % QUERY)

                result = connection.execute(QUERY)
                metadata = {}

                for row in result.fetchall():
                    metadata[row[0]] = row[1]

                server.setMetadata(metadata)

                servers.append(server)
        except SQLAlchemyError as ex:
            raise Exception(ex.message)
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
vm_state from nova.instances where project_id = \
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
                server.setFlavor(flavor)

                QUERY = """select `key`, value from nova.instance_metadata \
where instance_uuid='%(id)s' and deleted_at is NULL""" % {"id": server.getId()}

                LOG.debug("getExpiredServers query: %s" % QUERY)

                result = connection.execute(QUERY)
                metadata = {}

                for row in result.fetchall():
                    metadata[row[0]] = row[1]

                server.setMetadata(metadata)

                servers.append(server)

        except SQLAlchemyError as ex:
            raise Exception(ex.message)
        finally:
            connection.close()

        return servers

    def selectComputes(self, request):
        target = self.messagingAPI.getTarget(topic='scheduler',
                                             exchange="nova",
                                             version="4.0")

        client = self.messagingAPI.getRPCClient(target)
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
            raise Exception(ex.message)
        finally:
            connection.close()

        return blockDeviceMapList
