import json
import requests

from datetime import datetime
from token import Token

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


class Trust(object):

    def __init__(self, data):
        self.id = data["id"]
        self.impersonations = data["impersonation"]
        self.trustor_user_id = data["trustor_user_id"]
        self.trustee_user_id = data["trustee_user_id"]
        self.links = data.get("links", [])
        self.roles = data.get("roles", [])
        self.remaining_uses = data["remaining_uses"]
        self.expires_at = None
        self.keystone_url = None
        self.ssl_ca_file = None
        self.ssl_cert_file = None

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

    def getToken(self, token_id):
        headers = {"Content-Type": "application/json",
                   "Accept": "application/json",
                   "User-Agent": "synergy"}
        data = {}
        data["auth"] = {"identity": {"methods": ["token"],
                                     "token": {"id": token_id}},
                        "scope": {"OS-TRUST:trust": {"id": self.getId()}}}

        response = requests.post(url=self.keystone_url + "/auth/tokens",
                                 headers=headers,
                                 data=json.dumps(data),
                                 verify=self.ssl_ca_file,
                                 cert=self.ssl_cert_file)

        if response.status_code != requests.codes.ok:
            response.raise_for_status()

        if not response.text:
            raise Exception("authentication failed!")

        token_subject = response.headers["X-Subject-Token"]
        token_data = response.json()

        return Token.parse(token_subject, token_data)

    @staticmethod
    def makeTrust(trustee_user_id, token, expires_at=None, impersonation=True):
        if token.isExpired():
            raise Exception("token expired!")

        headers = {"Content-Type": "application/json",
                   "Accept": "application/json",
                   "User-Agent": "synergy",
                   "X-Auth-Token": token.getId()}

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

        response = requests.post(url=Trust.keystone_url + "/OS-TRUST/trusts",
                                 headers=headers,
                                 data=json.dumps(data),
                                 verify=Trust.ssl_ca_file,
                                 cert=Trust.ssl_cert_file)

        if response.status_code != requests.codes.ok:
            response.raise_for_status()

        if not response.text:
            raise Exception("trust token failed!")

        response = response.json()

        trust = Trust(response["trust"])
        trust.keystone_url = Trust.keystone_url
        trust.ssl_ca_file = Trust.ssl_ca_file
        trust.ssl_cert_file = Trust.ssl_cert_file

        return trust
