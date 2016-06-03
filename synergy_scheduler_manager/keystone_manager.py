import json
import logging
import os.path
import requests

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


LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class Trust(object):

    def __init__(self, data):
        data = data["trust"]

        self.id = data["id"]
        self.impersonations = data["impersonation"]
        self.roles_links = data["roles_links"]
        self.trustor_user_id = data["trustor_user_id"]
        self.trustee_user_id = data["trustee_user_id"]
        self.links = data["links"]
        self.roles = data["roles"]
        self.remaining_uses = data["remaining_uses"]
        self.expires_at = None

        if data["expires_at"] is not None:
            self.expires_at = datetime.strptime(data["expires_at"],
                                                "%Y-%m-%dT%H:%M:%S.%fZ")
        self.project_id = data["project_id"]

    def getId(self):
        return self.id

    def isImpersonations(self):
        return self.impersonations

    def getRolesLinks(self):
        return self.roles_links

    def getTrustorUserId(self):
        return self.trustor_user_id

    def getTrusteeUserId(self):
        return self.trustee_user_id

    def getlinks(self):
        return self.links

    def getProjectId(self):
        return self.project_id

    def getRoles(self):
        return self.roles

    def getRemainingUses(self):
        return self.remaining_uses

    def getExpiration(self):
        return self.expires_at

    def isExpired(self):
        if self.getExpiration() is None:
            return False

        return self.getExpiration() < datetime.utcnow()


class Token(object):

    def __init__(self, token, data):
        self.id = token

        data = data["token"]
        self.roles = data["roles"]
        self.catalog = data["catalog"]
        self.issued_at = datetime.strptime(data["issued_at"],
                                           "%Y-%m-%dT%H:%M:%S.%fZ")
        self.expires_at = datetime.strptime(data["expires_at"],
                                            "%Y-%m-%dT%H:%M:%S.%fZ")
        self.project = data["project"]
        self.user = data["user"]
        self.extras = data["extras"]

    def getCatalog(self, service_name=None, interface="public"):
        if service_name:
            for service in self.catalog:
                if service["name"] == service_name:
                    for endpoint in service["endpoints"]:
                        if endpoint["interface"] == interface:
                            return endpoint
            return None
        else:
            return self.catalog

    def getExpiration(self):
        return self.expires_at

    def getId(self):
        return self.id

    def getExtras(self):
        return self.extras

    def getProject(self):
        return self.project

    def getRoles(self):
        return self.roles

    def getUser(self):
        return self.user

    def isAdmin(self):
        if not self.roles:
            return False

        for role in self.roles:
            if role["name"] == "admin":
                return True

        return False

    def issuedAt(self):
        return self.issued_at

    def isExpired(self):
        return self.getExpiration() < datetime.utcnow()

    def save(self, filename):
        # save to file
        with open(filename, 'w') as f:
            token = {}
            token["catalog"] = self.catalog
            token["extras"] = self.extras
            token["user"] = self.user
            token["project"] = self.project
            token["roles"] = self.roles
            token["roles"] = self.roles
            token["issued_at"] = self.issued_at.isoformat()
            token["expires_at"] = self.expires_at.isoformat()

            data = {"id": self.id, "token": token}

            json.dump(data, f)

    @classmethod
    def load(cls, filename):
        if not os.path.isfile(".auth_token"):
            return None

        # load from file:
        with open(filename, 'r') as f:
            try:
                data = json.load(f)
                return Token(data["id"], data)
            # if the file is empty the ValueError will be thrown
            except ValueError as ex:
                raise ex

    def isotime(self, at=None, subsecond=False):
        """Stringify time in ISO 8601 format."""
        if not at:
            at = datetime.utcnow()

        if not subsecond:
            st = at.strftime('%Y-%m-%dT%H:%M:%S')
        else:
            st = at.strftime('%Y-%m-%dT%H:%M:%S.%f')

        if at.tzinfo:
            tz = at.tzinfo.tzname(None)
        else:
            tz = 'UTC'

        st += ('Z' if tz == 'UTC' else tz)
        return st

    """The trustor or grantor of a trust is the person who creates the trust.
    The trustor is the one who contributes property to the trust.
    The trustee is the person who manages the trust, and is usually appointed
    by the trustor. The trustor is also often the trustee in living trusts.
    """
    def trust(self, trustee_user, expires_at=None,
              project_id=None, roles=None, impersonation=True):
        if self.isExpired():
            raise Exception("token expired!")

        headers = {"Content-Type": "application/json",
                   "Accept": "application/json",
                   "User-Agent": "python-novaclient",
                   "X-Auth-Token": self.getId()}

        if roles is None:
            roles = self.getRoles()

        if project_id is None:
            project_id = self.getProject().get("id")

        data = {}
        data["trust"] = {"impersonation": impersonation,
                         "project_id": project_id,
                         "roles": roles,
                         "trustee_user_id": trustee_user,
                         "trustor_user_id": self.getUser().get("id")}

        if expires_at is not None:
            data["trust"]["expires_at"] = self.isotime(expires_at, True)

        endpoint = self.getCatalog(service_name="keystone")

        if not endpoint:
            raise Exception("keystone endpoint not found!")

        if "v2.0" in endpoint["url"]:
            endpoint["url"] = endpoint["url"].replace("v2.0", "v3")

        response = requests.post(url=endpoint["url"] + "/OS-TRUST/trusts",
                                 headers=headers,
                                 data=json.dumps(data))

        if response.status_code != requests.codes.ok:
            response.raise_for_status()

        if not response.text:
            raise Exception("trust token failed!")

        return Trust(response.json())


class KeystoneManager(Manager):

    def __init__(self):
        super(KeystoneManager, self).__init__(name="KeystoneManager")

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

        self.authenticate()

    def task(self):
        pass

    def destroy(self):
        pass

    def execute(self, command, *args, **kargs):
        if command == "GET_USERS":
            return self.getUsers()
        elif command == "GET_USER":
            return self.getProject(*args, **kargs)
        elif command == "GET_USER_PROJECTS":
            return self.getUserProjects(*args, **kargs)
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
                   "User-Agent": "python-novaclient"}

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

        # print(response.__dict__)

        token_subject = response.headers["X-Subject-Token"]
        token_data = response.json()

        self.token = Token(token_subject, token_data)

    def getUser(self, id):
        try:
            response = self.getResource("users/%s" % id, "GET")
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise Exception("error on retrieving the user info (id=%r): %s"
                            % (id, response["error"]["message"]))

        if response:
            response = response["user"]

        return response

    def getUsers(self):
        try:
            response = self.getResource("users", "GET")
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise Exception("error on retrieving the users list: %s"
                            % response["error"]["message"])

        if response:
            response = response["users"]

        return response

    def getUserProjects(self, id):
        try:
            response = self.getResource("users/%s/projects" % id, "GET")
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise Exception("error on retrieving the users's projects "
                            "(id=%r): %s" % (id, response["error"]["message"]))

        if response:
            response = response["projects"]

        return response

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

        if response:
            response = response["roles"]

        return response

    def getProject(self, id):
        try:
            response = self.getResource("/projects/%s" % id, "GET")
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise Exception(
                "error on retrieving the project (id=%r, msg=%s)." %
                (id, response["error"]["message"]))

        if response:
            response = response["project"]

        return response

    def getProjects(self):
        try:
            response = self.getResource("/projects", "GET")
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise Exception("error on retrieving the projects list: %s"
                            % response["error"]["message"])

        if response:
            response = response["projects"]

        return response

    def getRole(self, id):
        try:
            response = self.getResource("/roles/%s" % id, "GET")
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise Exception("error on retrieving the role info (id=%r): %s"
                            % (id, response["error"]["message"]))

        if response:
            response = response["role"]

        return response

    def getRoles(self):
        try:
            response = self.getResource("/roles", "GET")
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise Exception("error on retrieving the roles list: %s"
                            % response["error"]["message"])

        if response:
            response = response["roles"]

        return response

    def getToken(self):
        self.authenticate()
        return self.token

    def deleteToken(self, id):
        if self.token is None:
            return

        headers = {"Content-Type": "application/json",
                   "Accept": "application/json",
                   "User-Agent": "python-novaclient",
                   "X-Auth-Project-Id": self.token.getProject()["name"],
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
                   "User-Agent": "python-novaclient",
                   "X-Auth-Project-Id": self.token.getProject()["name"],
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

        return Token(token_subject, token_data)

    def getEndpoint(self, id=None, service_id=None):
        if id:
            try:
                response = self.getResource("/endpoints/%s" % id, "GET")
            except requests.exceptions.HTTPError as ex:
                response = ex.response.json()
                raise Exception("error on retrieving the endpoint (id=%r): %s"
                                % (id, response["error"]["message"]))
            if response:
                response = response["endpoint"]

            return response
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
                    if endpoint["service_id"] == service_id:
                        return endpoint

        return None

    def getEndpoints(self):
        try:
            response = self.getResource("/endpoints", "GET")
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise Exception("error on retrieving the endpoints list: %s"
                            % response["error"]["message"])

        if response:
            response = response["endpoints"]

        return response

    def getService(self, id=None, name=None):
        if id:
            try:
                response = self.getResource("/services/%s" % id, "GET")
            except requests.exceptions.HTTPError as ex:
                response = ex.response.json()
                raise Exception("error on retrieving the service info (id=%r)"
                                ": %s" % (id, response["error"]["message"]))

            if response:
                response = response["service"]
            return response
        elif name:
            services = self.getServices()

            if services:
                for service in services:
                    if service["name"] == name:
                        return service

        return None

    def getServices(self):
        try:
            response = self.getResource("/services", "GET")
        except requests.exceptions.HTTPError as ex:
            response = ex.response.json()
            raise Exception("error on retrieving the services list: %s"
                            % response["error"]["message"])

        if response:
            response = response["services"]

        return response

    def getResource(self, resource, method, data=None):
        self.authenticate()

        url = self.auth_url + "/" + resource

        headers = {"Content-Type": "application/json",
                   "Accept": "application/json",
                   "User-Agent": "python-novaclient",
                   "X-Auth-Project-Id": self.token.getProject()["name"],
                   "X-Auth-Token": self.token.getId()}

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
