import logging
import oslo_messaging as oslo_msg

from oslo_config import cfg

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


class AMQP(object):

    def __init__(self, url=None, backend=None, username=None, password=None,
                 hosts=None, virt_host=None, exchange=None):
        super(AMQP, self).__init__()

        if exchange:
            oslo_msg.set_transport_defaults(control_exchange=exchange)

        if url:
            self.TRANSPORT = oslo_msg.get_transport(CONF, url=url)
        elif not backend and not hosts:
            raise ValueError("missing AMQP parameters")
        else:
            t_hosts = self._createTransportHosts(username, password, hosts)
            t_url = oslo_msg.TransportURL(CONF,
                                          transport=backend,
                                          virtual_host=virt_host,
                                          hosts=t_hosts,
                                          aliases=None)

            self.TRANSPORT = oslo_msg.get_transport(CONF, url=t_url)

    def _createTransportHosts(self, username, password, hosts):
        """Returns a list of oslo.messaging.TransportHost objects."""
        transport_hosts = []

        for host in hosts:
            host = host.strip()
            host_name, host_port = host.split(":")

            if not host_port:
                msg = "Invalid hosts value: %s. It should be"\
                      " in hostname:port format" % host
                raise ValueError(msg)

            try:
                host_port = int(host_port)
            except ValueError:
                msg = "Invalid port value: %s. It should be an integer"
                raise ValueError(msg % host_port)

            transport_hosts.append(oslo_msg.TransportHost(
                hostname=host_name,
                port=host_port,
                username=username,
                password=password))
        return transport_hosts

    def getTarget(self, topic, exchange=None, namespace=None,
                  version=None, server=None):
        return oslo_msg.Target(topic=topic,
                               exchange=exchange,
                               namespace=namespace,
                               version=version,
                               server=server)

    def getRPCClient(self, target, version_cap=None, serializer=None):
        assert self.TRANSPORT is not None

        return oslo_msg.RPCClient(self.TRANSPORT,
                                  target,
                                  version_cap=version_cap,
                                  serializer=serializer)

    def getRPCServer(self, target, endpoints, serializer=None):
        assert self.TRANSPORT is not None

        return oslo_msg.get_rpc_server(self.TRANSPORT,
                                       target,
                                       endpoints,
                                       executor="eventlet",
                                       serializer=serializer)

    def getNotificationListener(self, targets, endpoints):
        assert self.TRANSPORT is not None

        return oslo_msg.get_notification_listener(self.TRANSPORT,
                                                  targets,
                                                  endpoints,
                                                  allow_requeue=False,
                                                  executor="eventlet")
