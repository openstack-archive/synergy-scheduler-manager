import json
import logging
import requests

try:
    from oslo_config import cfg
except ImportError:
    from oslo.config import cfg

from common.endpoint import Endpoint
from common.project import Project
from common.role import Role
from common.service import Service
from common.token import Token
from common.trust import Trust
from common.user import User


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


class KeystoneManager(Manager):

    def __init__(self):
        super(KeystoneManager, self).__init__("KeystoneManager")

        self.config_opts = [
            cfg.StrOpt("auth_url",
                       help="the Keystone url (v3 only)",
                       required=True),
            cfg.StrOpt("username",
                       help="the name of user with admin role",
                       required=True),
            cfg.StrOpt("password",
                       help="the password of user with admin role",
                       required=True),
            cfg.StrOpt("project_name",
                       help="the project to request authorization on",
                       required=True),
            cfg.StrOpt("project_id",
                       help="the project id to request authorization on",
                       required=False),
            cfg.IntOpt("timeout",
                       help="set the http connection timeout",
                       default=60,
                       required=False),
            cfg.IntOpt("trust_expiration",
                       help="set the trust expiration",
                       default=24,
                       required=False)
        ]

    def setup(self):
        self.auth_url = CONF.KeystoneManager.auth_url
        self.username = CONF.KeystoneManager.username
        self.password = CONF.KeystoneManager.password
        self.project_name = CONF.KeystoneManager.project_name
        self.project_id = CONF.KeystoneManager.project_id
        self.timeout = CONF.KeystoneManager.timeout
        self.trust_expiration = CONF.KeystoneManager.trust_expiration
        self.token = None
        self.auth_public_url = None

        self.authenticate()

        service = self.getToken().getService("keystone")
        if not service:
            raise Exception("keystone service not found!")

        endpoint = service.getEndpoint("public")
        if not endpoint:
            raise Exception("keystone endpoint not found!")
        self.auth_public_url = endpoint.getURL()

    def task(self):
        pass

    def destroy(self):
        pass

    def execute(self, command, *args, **kargs):
        if command == "GET_USERS":
            return self.getUsers(*args, **kargs)
        elif command == "GET_USER":
            return self.getProject(*args, **kargs)
        elif command == "GET_USER_ROLES":
            return self.getUserRoles(*args, **kargs)
        elif command == "GET_PROJECTS":
            return self.getProjects()
        elif command == "GET_PROJECT":
            return self.getProject(*args, **kargs)
        elif command == "GET_ROLES":
            return self.getRoles()
        elif command == "GET_ROLE":
            return self.getRole(*args, **kargs)
        elif command == "GET_TOKEN":
            return self.getToken()
        elif command == "DELETE_TOKEN":
            return self.deleteToken(*args, **kargs)
        elif command == "VALIDATE_TOKEN":
            return self.validateToken(*args, **kargs)
        elif command == "GET_ENDPOINTS":
            return self.getEndpoints()
        elif command == "GET_ENDPOINT":
            return self.getEndpoints()
        elif command == "GET_SERVICES":
            return self.getServices()
        elif command == "GET_SERVICE":
            return self.getService(*args, **kargs)
        else:
            return None

    def doOnEvent(self, event_type, *args, **kargs):
        if event_type == "GET_PROJECTS":
            kargs["result"].extend(self.getProjects())

    def authenticate(self):
        if self.token is not None:
            if self.token.isExpired():
                try:
                    self.deleteToken(self.token.getId())
                except requests.exceptions.HTTPError:
                    pass
            else:
                return

        headers = {"Content-Type": "application/json",
                   "Accept": "application/json",
                   "User-Agent": "synergy"}

        identity = {"methods": ["password"],
                    "password": {"user": {"name": self.username,
                                          "domain": {"id": "default"},
                                          "password": self.password}}}

        data = {"auth": {}}
        data["auth"]["identity"] = identity

        if self.project_name:
            data["auth"]["scope"] = {"project": {"name": self.project_name,
                                                 "domain": {"id": "default"}}}

        if self.project_id:
            data["auth"]["scope"] = {"project": {"id": self.project_id,
                                                 "domain": {"id": "default"}}}

        response = requests.post(url=self.auth_url + "/auth/tokens",
                                 headers=headers,
                                 data=json.dumps(data),
                                 timeout=self.timeout)

        if response.status_code != requests.codes.ok:
            response.raise_for_status()

        if not response.text:
            raise Exception("authentication failed!")

        token_subject = response.headers["X-Subject-Token"]
        token_data = response.json()

        self.token = Token.parse(token_subject, token_data)

    def getUser(self, id):
        try:
            response = self.getResource("users/%s" % id, "GET")
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise Exception("error on retrieving the user info (id=%r): %s"
                            % (id, response["error"]["message"]))

        user = None

        if response:
            info = response["user"]

            user = User()
            user.setId(info["id"])
            user.setName(info["name"])
            user.setProjectId(info["tenantId"])
            user.setEnabled(info["enabled"])

        return user

    def getUsers(self, prj_id=None):
        if prj_id:
            try:
                response = self.getResource("tenants/%s/users" % prj_id,
                                            "GET",
                                            version="v2.0")
            except requests.exceptions.HTTPError as ex:
                response = ex.response.json()
                raise Exception("error on retrieving the project's users "
                                "(id=%r): %s" % (prj_id,
                                                 response["error"]["message"]))
        else:
            try:
                response = self.getResource("/users", "GET")
            except requests.exceptions.HTTPError as ex:
                response = ex.response.json()
                raise Exception("error on retrieving the users list: %s"
                                % response["error"]["message"])

        users = []

        if response:
            user_info = response["users"]

            for info in user_info:
                user = User()
                user.setId(info["id"])
                user.setName(info["name"])
                user.setProjectId(info["tenantId"])
                user.setEnabled(info["enabled"])

                users.append(user)

        return users

    def getUserRoles(self, user_id, project_id):
        try:
            response = self.getResource("/projects/%s/users/%s/roles"
                                        % (project_id, user_id), "GET")
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise Exception("error on retrieving the user's roles (usrId=%r, "
                            "prjId=%r): %s" % (user_id,
                                               project_id,
                                               response["error"]["message"]))
        roles = []

        if response:
            roles_info = response["roles"]

            for info in roles_info:
                role = Role()
                role.setId(info["id"])
                role.setName(info["name"])

                roles.append(role)

        return roles

    def getProject(self, id):
        try:
            response = self.getResource("/projects/%s" % id, "GET")
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise Exception(
                "error on retrieving the project (id=%r, msg=%s)." %
                (id, response["error"]["message"]))

        project = None

        if response:
            info = response["project"]

            project = Project()
            project.setId(info["id"])
            project.setName(info["name"])
            project.setEnabled(info["enabled"])

        return project

    def getProjects(self, usr_id=None):
        if usr_id:
            try:
                response = self.getResource(
                    "users/%s/projects" % usr_id, "GET")
            except requests.exceptions.HTTPError as ex:
                response = ex.response.json()
                raise Exception("error on retrieving the users's projects (id="
                                "%r): %s" % (usr_id,
                                             response["error"]["message"]))
        else:
            try:
                response = self.getResource("/projects", "GET")
            except requests.exceptions.HTTPError as ex:
                response = ex.response.json()
                raise Exception("error on retrieving the projects list: %s"
                                % response["error"]["message"])

        projects = []

        if response:
            projects_info = response["projects"]

            for info in projects_info:
                project = Project()
                project.setId(info["id"])
                project.setName(info["name"])
                project.setEnabled(info["enabled"])

                projects.append(project)

        return projects

    def getRole(self, id):
        try:
            response = self.getResource("/roles/%s" % id, "GET")
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise Exception("error on retrieving the role info (id=%r): %s"
                            % (id, response["error"]["message"]))

        role = None

        if response:
            info = response["role"]
            role = Role()
            role.setId(info["id"])
            role.setName(info["name"])

        return role

    def getRoles(self):
        try:
            response = self.getResource("/roles", "GET")
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise Exception("error on retrieving the roles list: %s"
                            % response["error"]["message"])

        roles = []

        if response:
            roles = response["roles"]

            for info in roles:
                role = Role()
                role.setId(info["id"])
                role.setName(info["name"])

                roles.append(role)

        return roles

    def makeTrust(self, trustee_user_id, token=None,
                  expires_at=None, impersonation=True):
        project_id = token.getProject().getId()
        roles = token.getRoles()
        roles_data = []

        for role in roles:
            roles_data.append({"id": role.getId(), "name": role.getName()})

        data = {}
        data["trust"] = {"impersonation": impersonation,
                         "project_id": project_id,
                         "roles": roles_data,
                         "trustee_user_id": trustee_user_id,
                         "trustor_user_id": token.getUser().getId()}

        if expires_at is not None:
            data["trust"]["expires_at"] = token.isotime(expires_at, True)

        try:
            response = self.getResource("/OS-TRUST/trusts",
                                        "POST", data=data, token=token)
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise Exception("error on retrieving the trust info (id=%r): %s"
                            % (id, response["error"]["message"]))

        trust = Trust(response["trust"])
        trust.keystone_url = self.auth_public_url

        return trust

    def getTrust(self, id):
        try:
            response = self.getResource("/OS-TRUST/trusts/%s" % id, "GET")
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise Exception("error on retrieving the trust info (id=%r): %s"
                            % (id, response["error"]["message"]))

        trust = None

        if response:
            trust = Trust(response["trust"])
            trust.keystone_url = self.auth_public_url

        return trust

    def deleteTrust(self, id, token=None):
        if not token:
            token = self.getToken()

        if token.isExpired():
            raise Exception("token expired!")

        try:
            self.getResource("/OS-TRUST/trusts/%s" % id, "DELETE", token=token)
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise Exception("error on deleting the trust (id=%r): %s"
                            % (id, response["error"]["message"]))

    def getTrusts(self, user_id=None, isTrustor=True, token=None):
        url = "/OS-TRUST/trusts"

        if user_id:
            if isTrustor:
                url += "?trustor_user_id=%s" % user_id
            else:
                url += "?trustee_user_id=%s" % user_id

        try:
            response = self.getResource(url, "GET", token=token)
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise Exception("error on retrieving the trust list (id=%r): %s"
                            % (id, response["error"]["message"]))

        trusts = []

        if response:
            for data in response["trusts"]:
                trust = Trust(data)
                trust.keystone_url = self.auth_public_url

                trusts.append(trust)

        return trusts

    def getToken(self):
        self.authenticate()
        return self.token

    def deleteToken(self, id):
        if self.token is None:
            return

        headers = {"Content-Type": "application/json",
                   "Accept": "application/json",
                   "User-Agent": "synergy",
                   "X-Auth-Project-Id": self.token.getProject().getName(),
                   "X-Auth-Token": self.token.getId(),
                   "X-Subject-Token": id}

        response = requests.delete(url=self.auth_url + "/auth/tokens",
                                   headers=headers,
                                   timeout=self.timeout)

        self.token = None

        if response.status_code != requests.codes.ok:
            response.raise_for_status()

    def validateToken(self, id):
        self.authenticate()

        headers = {"Content-Type": "application/json",
                   "Accept": "application/json",
                   "User-Agent": "synergy",
                   "X-Auth-Project-Id": self.token.getProject().getName(),
                   "X-Auth-Token": self.token.getId(),
                   "X-Subject-Token": id}

        response = requests.get(url=self.auth_url + "/auth/tokens",
                                headers=headers,
                                timeout=self.timeout)

        if response.status_code != requests.codes.ok:
            response.raise_for_status()

        if not response.text:
            raise Exception("token not found!")

        token_subject = response.headers["X-Subject-Token"]
        token_data = response.json()

        return Token.parse(token_subject, token_data)

    def getEndpoint(self, id=None, service_id=None):
        if id:
            try:
                response = self.getResource("/endpoints/%s" % id, "GET")
            except requests.exceptions.HTTPError as ex:
                response = ex.response.json()
                raise Exception("error on retrieving the endpoint (id=%r): %s"
                                % (id, response["error"]["message"]))
            if response:
                info = response["endpoint"]

                endpoint = Endpoint()
                endpoint.setId(info["id"])
                endpoint.setName(info["name"])
                endpoint.setInterface(info["interface"])
                endpoint.setRegion(info["region"])
                endpoint.setRegionId(info["region_id"])
                endpoint.setServiceId(info["service_id"])
                endpoint.setURL(info["url"])
                endpoint.setEnabled(info["enabled"])

                return endpoint
        elif service_id:
            try:
                endpoints = self.getEndpoints()
            except requests.exceptions.HTTPError as ex:
                response = ex.response.json()
                raise Exception(
                    "error on retrieving the endpoints list (serviceId=%r)" %
                    response["error"]["message"])

            if endpoints:
                for endpoint in endpoints:
                    if endpoint.getServiceId() == service_id:
                        return endpoint

        return None

    def getEndpoints(self):
        try:
            response = self.getResource("/endpoints", "GET")
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise Exception("error on retrieving the endpoints list: %s"
                            % response["error"]["message"])

        endpoints = []

        if response:
            endpoints_info = response["endpoints"]

            for info in endpoints_info:
                endpoint = Endpoint()
                endpoint.setId(info["id"])
                endpoint.setName(info["name"])
                endpoint.setInterface(info["interface"])
                endpoint.setRegion(info["region"])
                endpoint.setRegionId(info["region_id"])
                endpoint.setServiceId(info["service_id"])
                endpoint.setURL(info["url"])
                endpoint.setEnabled(info["enabled"])

                endpoints.append(endpoint)

        return endpoints

    def getService(self, id=None, name=None):
        if id:
            try:
                response = self.getResource("/services/%s" % id, "GET")
            except requests.exceptions.HTTPError as ex:
                response = ex.response.json()
                raise Exception("error on retrieving the service info (id=%r)"
                                ": %s" % (id, response["error"]["message"]))

            if response:
                info = response["service"]

                service = Service()
                service.setId(info["id"])
                service.setName(info["name"])
                service.setType(info["type"])
                service.setDescription(info["description"])
                service.setEnabled(info["enabled"])

                for endpoint_info in info.get("endpoints", []):
                    endpoint = Endpoint()
                    endpoint.setId(endpoint_info["id"])
                    endpoint.setInterface(endpoint_info["interface"])
                    endpoint.setRegion(endpoint_info["region"])
                    endpoint.setRegionId(endpoint_info["region_id"])
                    endpoint.setURL(endpoint_info["url"])

                    service.getEndpoints().append(endpoint)

                return service
        elif name:
            services = self.getServices()

            if services:
                for service in services:
                    if service.getName() == name:
                        return service

        return None

    def getServices(self):
        try:
            response = self.getResource("/services", "GET")
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise Exception("error on retrieving the services list: %s"
                            % response["error"]["message"])

        services = []

        if response:
            services_info = response["services"]

            for info in services_info:
                service = Service()
                service.setId(info["id"])
                service.setName(info["name"])
                service.setType(info["type"])
                service.setDescription(info["description"])
                service.setEnabled(info["enabled"])

                for endpoint_info in service.get("endpoints"):
                    endpoint = Endpoint()
                    endpoint.setId(endpoint_info["id"])
                    endpoint.setInterface(endpoint_info["interface"])
                    endpoint.setRegion(endpoint_info["region"])
                    endpoint.setRegionId(endpoint_info["region_id"])
                    endpoint.setURL(endpoint_info["url"])

                    service.getEndpoints().append(endpoint)

                services.append(service)

        return services

    def getResource(
            self, resource, method, version=None, data=None, token=None):
        if token:
            if token.isExpired():
                raise Exception("token expired!")

            url = self.auth_public_url
        else:
            self.authenticate()
            token = self.getToken()
            url = self.auth_url

        if version:
            url = url[:url.rfind("/") + 1] + version

        url = url + "/" + resource

        headers = {"Content-Type": "application/json",
                   "Accept": "application/json",
                   "User-Agent": "synergy",
                   "X-Auth-Project-Id": token.getProject().getName(),
                   "X-Auth-Token": token.getId()}

        if method == "GET":
            response = requests.get(url,
                                    headers=headers,
                                    params=data,
                                    timeout=self.timeout)
        elif method == "POST":
            response = requests.post(url,
                                     headers=headers,
                                     data=json.dumps(data),
                                     timeout=self.timeout)
        elif method == "PUT":
            response = requests.put(url,
                                    headers=headers,
                                    data=json.dumps(data),
                                    timeout=self.timeout)
        elif method == "HEAD":
            response = requests.head(url,
                                     headers=headers,
                                     data=json.dumps(data),
                                     timeout=self.timeout)
        elif method == "DELETE":
            response = requests.delete(url,
                                       headers=headers,
                                       data=json.dumps(data),
                                       timeout=self.timeout)
        else:
            raise Exception("wrong HTTP method: %s" % method)

        if response.status_code != requests.codes.ok:
            response.raise_for_status()

        if response.text:
            return response.json()
        else:
            return None
