import ConfigParser
import eventlet
import json
import logging
import os.path
import requests

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

        LOG.info(target_synergy)
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

    # TODO(hanlind): This method can be removed once oslo.versionedobjects
    # has been converted to use version_manifests in remotable_classmethod
    # operations, which will use the new class action handler.
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

    """ nova-conductor rpc operations """
    def build_instances(self, context, instances, image, filter_properties,
                        admin_password, injected_files, requested_networks,
                        security_groups, block_device_mapping=None,
                        legacy_bdm=True):
        # token = self.keystone_manager.validateToken(context["auth_token"])

        for instance in instances:
            request = {'instance': instance,
                       'image': image,
                       'filter_properties': filter_properties,
                       'admin_password': admin_password,
                       'injected_files': injected_files,
                       'requested_networks': requested_networks,
                       'security_groups': security_groups,
                       'block_device_mapping': block_device_mapping,
                       'legacy_bdm': legacy_bdm,
                       'context': context}

            self.scheduler_manager.execute("PROCESS_REQUEST", request)

    def build_instance(self, context, instance, image, filter_properties,
                       admin_password, injected_files, requested_networks,
                       security_groups, block_device_mapping=None,
                       legacy_bdm=True):
        try:
            # LOG.info(">>> filter_properties %s" % filter_properties)
            # LOG.info(">>> context %s" % context)

            version = '1.10'
            if not self.client.can_send_version(version):
                version = '1.9'
                LOG.info(filter_properties)
                if 'instance_type' in filter_properties:
                    flavor = filter_properties['instance_type']
                    # #################################
                    # ####### objects_base WRONG!!! ###
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

    def rebuild_instance(self, context, instance, orig_image_ref, image_ref,
                         injected_files, new_pass, orig_sys_metadata,
                         bdms, recreate, on_shared_storage,
                         preserve_ephemeral=False, host=None):
        cctxt = self.client.prepare(version='1.8')
        cctxt.cast(context,
                   'rebuild_instance',
                   instance=instance, new_pass=new_pass,
                   injected_files=injected_files, image_ref=image_ref,
                   orig_image_ref=orig_image_ref,
                   orig_sys_metadata=orig_sys_metadata, bdms=bdms,
                   recreate=recreate, on_shared_storage=on_shared_storage,
                   preserve_ephemeral=preserve_ephemeral,
                   host=host)


class NovaManager(Manager):

    def __init__(self):
        super(NovaManager, self).__init__(name="NovaManager")

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
                       default=None,
                       required=False),
            cfg.IntOpt("amqp_port",
                       help="the amqp listening port",
                       default=None,
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
                       default=None,
                       required=False),
            cfg.StrOpt("conductor_topic",
                       help="the conductor topic",
                       default=None,
                       required=False),
            cfg.StrOpt("compute_topic",
                       help="the compute topic",
                       default=None,
                       required=False),
            cfg.StrOpt("scheduler_topic",
                       help="the scheduler topic",
                       default=None,
                       required=False),
            cfg.StrOpt("db_connection",
                       help="the NOVA database connection",
                       default=None,
                       required=False),
            cfg.StrOpt("host",
                       help="the host name",
                       default=None,
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

        host = "localhost"
        conductor_topic = "conductor"
        compute_topic = "compute"
        scheduler_topic = "scheduler"
        db_connection = None
        amqp_backend = None
        amqp_host = None
        amqp_port = None
        amqp_user = None
        amqp_password = None
        amqp_virt_host = None

        if CONF.NovaManager.nova_conf is not None:
            if os.path.isfile(CONF.NovaManager.nova_conf):
                CONFIG.read(CONF.NovaManager.nova_conf)
            else:
                raise Exception("nova configuration file not found at %s!"
                                % CONF.NovaManager.nova_conf)

            host = self.getParameter("my_ip",
                                     "DEFAULT",
                                     default=host)

            conductor_topic = self.getParameter("conductor_topic",
                                                "DEFAULT",
                                                default=conductor_topic)

            compute_topic = self.getParameter("compute_topic",
                                              "DEFAULT",
                                              default=compute_topic)

            scheduler_topic = self.getParameter("scheduler_topic",
                                                "DEFAULT",
                                                default=scheduler_topic)

            db_connection = self.getParameter("connection",
                                              "database",
                                              fallback=True)

            amqp_backend = self.getParameter("rpc_backend",
                                             "DEFAULT",
                                             fallback=True)

            if amqp_backend == "rabbit":
                amqp_host = self.getParameter("rabbit_host",
                                              "oslo_messaging_rabbit",
                                              "localhost",
                                              fallback=False)

                amqp_port = self.getParameter("rabbit_port",
                                              "oslo_messaging_rabbit",
                                              "5672",
                                              fallback=False)

                amqp_virt_host = self.getParameter("rabbit_virtual_host",
                                                   "oslo_messaging_rabbit",
                                                   "/",
                                                   fallback=False)

                amqp_user = self.getParameter("rabbit_userid",
                                              "oslo_messaging_rabbit",
                                              "guest",
                                              fallback=False)

                amqp_password = self.getParameter("rabbit_password",
                                                  "oslo_messaging_rabbit",
                                                  fallback=True)
            elif amqp_backend == "qpid":
                amqp_host = self.getParameter("qpid_hostname",
                                              "oslo_messaging_qpid",
                                              "localhost",
                                              fallback=False)

                amqp_port = self.getParameter("qpid_port",
                                              "oslo_messaging_qpid",
                                              "5672",
                                              fallback=False)

                amqp_user = self.getParameter("qpid_username",
                                              "oslo_messaging_qpid",
                                              fallback=True)

                amqp_password = self.getParameter("qpid_password",
                                                  "oslo_messaging_qpid",
                                                  fallback=True)
            else:
                raise Exception("unsupported amqp backend found: %s!"
                                % amqp_backend)
        else:
            amqp_backend = CONF.NovaManager.amqp_backend
            amqp_host = CONF.NovaManager.amqp_host
            amqp_port = CONF.NovaManager.amqp_port
            amqp_user = CONF.NovaManager.amqp_user
            amqp_password = CONF.NovaManager.amqp_password
            amqp_virt_host = CONF.NovaManager.amqp_virt_host
            db_connection = CONF.NovaManager.db_connection
            host = amqp_host

            amqp_backend = self.getParameter("amqp_backend",
                                             "NovaManager",
                                             fallback=True)

            amqp_host = self.getParameter("amqp_host",
                                          "NovaManager",
                                          fallback=True)

            amqp_host = self.getParameter("amqp_host",
                                          "NovaManager",
                                          default=5672)

            amqp_user = self.getParameter("amqp_user",
                                          "NovaManager",
                                          fallback=True)

            amqp_password = self.getParameter("amqp_password",
                                              "NovaManager",
                                              fallback=True)

            amqp_virt_host = self.getParameter("amqp_virt_host",
                                               "NovaManager",
                                               default="/")

            db_connection = self.getParameter("db_connection",
                                              "NovaManager",
                                              fallback=True)

            host = self.getParameter("host",
                                     "NovaManager",
                                     default="localhost")

            conductor_topic = self.getParameter("conductor_topic",
                                                "NovaManager",
                                                default=conductor_topic)

            compute_topic = self.getParameter("compute_topic",
                                              "NovaManager",
                                              default=compute_topic)

            scheduler_topic = self.getParameter("scheduler_topic",
                                                "NovaManager",
                                                default=scheduler_topic)

        try:
            LOG.debug("setting up the NOVA database connection: %s"
                      % db_connection)

            self.db_engine = create_engine(db_connection)

            transport_url = "%s://%s:%s@%s:%s%s" % (amqp_backend,
                                                    amqp_user,
                                                    amqp_password,
                                                    amqp_host,
                                                    amqp_port,
                                                    amqp_virt_host)

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

            # self.rpcserver.start()
        except Exception as ex:
            LOG.error("Exception has occured", exc_info=1)
            LOG.error("NovaManager initialization failed! %s" % (ex))
            raise ex

    def execute(self, command, *args, **kargs):
        if command == "GET_PARAMETER":
            return self.getParameter(*args, **kargs)
        elif command == "GET_FLAVORS":
            return self.getFlavors()
        elif command == "GET_FLAVOR":
            return self.getFlavor(*args, **kargs)
        elif command == "GET_SERVERS":
            return self.getServers(*args, **kargs)
        elif command == "GET_SERVER":
            return self.getServer(*args, **kargs)
        elif command == "DELETE_SERVER":
            return self.deleteServer(*args, **kargs)
        elif command == "START_SERVER":
            return self.startServer(*args, **kargs)
        elif command == "STOP_SERVER":
            return self.stopServer(*args, **kargs)
        elif command == "BUILD_SERVER":
            return self.buildServer(*args, **kargs)
        elif command == "GET_HYPERVISORS":
            return self.getHypervisors()
        elif command == "GET_HYPERVISOR":
            return self.getHypervisor(*args, **kargs)
        elif command == "GET_QUOTA":
            return self.getQuota(*args, **kargs)
        elif command == "UPDATE_QUOTA":
            return self.updateQuota(*args, **kargs)
        elif command == "GET_TARGET":
            return self.getTarget(*args, **kargs)
        elif command == "GET_RCP_CLIENT":
            return self.getRPCClient(*args, **kargs)
        elif command == "GET_RCP_SERVER":
            return self.getRPCServer(*args, **kargs)
        elif command == "GET_NOTIFICATION_LISTENER":
            return self.getNotificationListener(*args, **kargs)
        elif command == "GET_RESOURCE_USAGE":
            return self.getResourceUsage(*args, **kargs)
        elif command == "GET_PROJECT_USAGE":
            return self.getProjectUsage(*args, **kargs)
        elif command == "GET_EXPIRED_SERVERS":
            return self.getExpiredServers(*args, **kargs)
        else:
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
                LOG.info("No attribute %r found in in [NovaManager] of synergy"
                         ".conf, using default: %r" % (name, default))
                return default

    def getFlavors(self):
        url = "flavors/detail"

        try:
            response_data = self.getResource(url, method="GET")
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise Exception("error on retrieving the flavors list: %s"
                            % response["error"]["message"])

        if response_data:
            response_data = response_data["flavors"]

        return response_data

    def getFlavor(self, id):
        try:
            response_data = self.getResource("flavors/" + id, "GET")
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise Exception("error on retrieving the flavor info (id=%r)"
                            ": %s" % (id, response["error"]["message"]))

        if response_data:
            response_data = response_data["flavor"]

        return response_data

    def getServers(self, detail=False, status=None):
        params = {}
        if status:
            params["status"] = status

        url = "servers"

        if detail:
            url = "servers/detail"

        response_data = self.getResource(url, "GET", params)

        if response_data:
            response_data = response_data["servers"]

        return response_data

    def getServer(self, id):
        try:
            response_data = self.getResource("servers/" + id, "GET")
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise Exception("error on retrieving the server info (id=%r)"
                            ": %s" % (id, response["error"]["message"]))

        if response_data:
            response_data = response_data["server"]

        return response_data

    def buildServer(self, context, instance, image, filter_properties,
                    admin_password, injected_files, requested_networks,
                    security_groups, block_device_mapping=None,
                    legacy_bdm=True):
        self.novaConductorComputeAPI.build_instance(
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

    def deleteServer(self, id):
        # data = { "forceDelete": None }
        # url = "servers/%s/action" % id
        url = "servers/%s" % id

        try:
            # response_data = self.getResource(url, "POST", data)
            response_data = self.getResource(url, "DELETE")
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise Exception("error on deleting the server (id=%r)"
                            ": %s" % (id, response["error"]["message"]))

        if response_data:
            response_data = response_data["server"]

        return response_data

    def startServer(self, id):
        data = {"os-start": None}
        url = "servers/%s/action" % id

        try:
            response_data = self.getResource(url, "POST", data)
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise Exception("error on starting the server info (id=%r)"
                            ": %s" % (id, response["error"]["message"]))

        if response_data:
            response_data = response_data["server"]

        return response_data

    def stopServer(self, id):
        data = {"os-stop": None}
        url = "servers/%s/action" % id

        try:
            response_data = self.getResource(url, "POST", data)
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise Exception("error on stopping the server info (id=%r)"
                            ": %s" % (id, response["error"]["message"]))

        if response_data:
            response_data = response_data["server"]

        return response_data

    def getHypervisors(self):
        data = {"os-stop": None}
        url = "os-hypervisors"
        # /%s" % id

        try:
            response_data = self.getResource(url, "GET", data)
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise Exception("error on retrieving the hypervisors list: %s"
                            % response["error"]["message"])

        if response_data:
            response_data = response_data["hypervisors"]

        return response_data

    def getHypervisor(self, id):
        data = {"os-stop": None}
        url = "os-hypervisors/%s" % id

        try:
            response_data = self.getResource(url, "GET", data)
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise Exception("error on retrieving the hypervisor info (id=%r)"
                            ": %s" % (id, response["error"]["message"]))

        if response_data:
            response_data = response_data["hypervisor"]

        return response_data

    def getQuota(self, id=None, defaults=False):
        if defaults:
            try:
                url = "os-quota-sets/defaults"
                response_data = self.getResource(url, "GET")
            except requests.exceptions.HTTPError as ex:
                response = ex.response.json()
                raise Exception("error on retrieving the quota defaults"
                                ": %s" % response["error"]["message"])
        elif id is not None:
            try:
                url = "os-quota-sets/%s" % id
                response_data = self.getResource(url, "GET")
            except requests.exceptions.HTTPError as ex:
                response = ex.response.json()
                raise Exception("error on retrieving the quota info (id=%r)"
                                ": %s" % (id, response["error"]["message"]))
        else:
            raise Exception("wrong arguments")

        if response_data:
            response_data = response_data["quota_set"]

        return response_data

    def updateQuota(self, id, data):
        url = "os-quota-sets/%s" % id
        quota_set = {"quota_set": data}

        try:
            response_data = self.getResource(url, "PUT", quota_set)
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise Exception("error on updating the quota info (id=%r)"
                            ": %s" % (id, response["error"]["message"]))

        if response_data:
            response_data = response_data["quota_set"]

        return response_data

    def getResource(self, resource, method, data=None):
        self.keystone_manager.authenticate()
        token = self.keystone_manager.getToken()
        url = token.getCatalog("nova")["url"] + "/" + resource

        headers = {"Content-Type": "application/json",
                   "Accept": "application/json",
                   "User-Agent": "python-novaclient",
                   "X-Auth-Project-Id": token.getProject()["name"],
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

    def getResourceUsage(self, prj_ids, from_date, to_date):
        # LOG.info("getUsage: fromDate=%s period_length=%s days"
        #          % (fromDate, period_length))
        # print("getUsage: fromDate=%s period_length=%s days"
        #       % (fromDate, period_length))

        # period = str(period_length)

        resource_usage = {}
        connection = self.db_engine.connect()

        try:
            ids = ""

            if prj_ids is not None:
                ids = " project_id IN ("

                for prj_id in prj_ids:
                    ids += "%r, " % str(prj_id)

                if "," in ids:
                    ids = ids[:-2]

                ids += ") and"

            QUERY = """select user_id, project_id, sum(TIMESTAMPDIFF(SECOND, \
IF(launched_at<='%(from_date)s', '%(from_date)s', IFNULL(launched_at, \
'%(from_date)s')), IF(terminated_at>='%(to_date)s', '%(to_date)s', \
IFNULL(terminated_at, '%(to_date)s')))*memory_mb) as memory_usage, \
sum(TIMESTAMPDIFF(SECOND, IF(launched_at<='%(from_date)s', '%(from_date)s', \
IFNULL(launched_at, '%(from_date)s')), IF(terminated_at>='%(to_date)s', \
'%(to_date)s', IFNULL(terminated_at, '%(to_date)s')))*vcpus) as vcpus_usage \
from nova.instances where %(prj_ids)s launched_at is not NULL and \
launched_at<='%(to_date)s' and (terminated_at>='%(from_date)s' or \
terminated_at is NULL) group by user_id, project_id\
""" % {"prj_ids": ids, "from_date": from_date, "to_date": to_date}

            result = connection.execute(QUERY)

            # LOG.info("QUERY %s\n" % QUERY)
            # print("from_date %s\n" % from_date)
            # print("to_date %s\n" % to_date)

            prj_id = 0
            user_id = 0
            project = None

            # for row in result.fetchall():
            for row in result:
                # LOG.info("row=%s" % row)
                user_id = row[0]
                prj_id = row[1]

                if (prj_ids is not None and prj_id not in prj_ids):
                    LOG.warn("project not found: %s" % prj_id)
                    continue

                if prj_id not in resource_usage:
                    resource_usage[prj_id] = {}

                project = resource_usage.get(prj_id)

                project[user_id] = {"ram": float(row[2]),
                                    "cores": float(row[3])}

        except SQLAlchemyError as ex:
            raise Exception(ex.message)
        finally:
            connection.close()

        return resource_usage

    def getProjectUsage(self, prj_id):
        connection = self.db_engine.connect()
        usage = {"instances": [], "cores": 0, "ram": 0}

        try:
            # retrieve the amount of resources in terms of cores
            # and ram the specified project is consuming
            QUERY = """select uuid, vcpus, memory_mb from nova.instances \
where project_id='%(project_id)s' and deleted_at is NULL and (vm_state in \
('error') or (vm_state in ('active') and terminated_at is NULL))\
""" % {"project_id": prj_id}

            result = connection.execute(QUERY)

            for row in result.fetchall():
                usage["instances"].append(str(row[0]))
                usage["cores"] += row[1]
                usage["ram"] += row[2]
        except SQLAlchemyError as ex:
            raise Exception(ex.message)
        finally:
            connection.close()

        return usage

    def getExpiredServers(self, prj_id, instances, TTL):
        uuids = []
        connection = self.db_engine.connect()

        try:
            # retrieve all expired instances for the specified
            # project and expiration time
            ids = "'%s'" % "', '".join(instances)

            QUERY = """select uuid from nova.instances where project_id = \
'%(project_id)s' and deleted_at is NULL and (vm_state in ('error') or \
(uuid in (%(instances)s) and ((vm_state in ('active') and terminated_at is \
NULL and timestampdiff(minute, launched_at, utc_timestamp()) >= \
%(expiration)s) or (vm_state in ('building') and task_state in ('scheduling') \
and created_at != updated_at and timestampdiff(minute, updated_at, \
utc_timestamp()) >= 20))))""" % {"project_id": prj_id,
                                 "instances": ids,
                                 "expiration": TTL}

            # LOG.info(QUERY)
            result = connection.execute(QUERY)

            for row in result.fetchall():
                uuids.append(row[0])
        except SQLAlchemyError as ex:
            raise Exception(ex.message)
        finally:
            connection.close()

        return uuids
