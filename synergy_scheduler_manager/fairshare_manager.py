import logging
import threading

from datetime import datetime
from datetime import timedelta

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


class FairShareManager(Manager):

    def __init__(self):
        super(FairShareManager, self).__init__(name="FairShareManager")

        self.config_opts = [
            cfg.IntOpt('periods', default=3),
            cfg.IntOpt('period_length', default=7),
            cfg.FloatOpt('default_share', default=10.0),
            cfg.FloatOpt('decay_weight', default=0.5, help="the decay weight"),
            cfg.IntOpt('age_weight', default=1000, help="the age weight"),
            cfg.IntOpt('vcpus_weight', default=10000, help="the vcpus weight"),
            cfg.IntOpt('memory_weight', default=7000, help="the memory weight")
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

        self.periods = CONF.FairShareManager.periods
        self.period_length = CONF.FairShareManager.period_length
        self.default_share = float(CONF.FairShareManager.default_share)
        self.decay_weight = CONF.FairShareManager.decay_weight
        self.vcpus_weight = CONF.FairShareManager.vcpus_weight
        self.age_weight = CONF.FairShareManager.age_weight
        self.memory_weight = CONF.FairShareManager.memory_weight
        self.projects = {}
        self.workers = []
        self.exit = False
        self.nova_manager = self.getManager("NovaManager")
        self.queue_manager = self.getManager("QueueManager")
        self.quota_manager = self.getManager("QuotaManager")
        self.keystone_manager = self.getManager("KeystoneManager")
        self.condition = threading.Condition()

    def execute(self, command, *args, **kargs):
        if command == "ADD_PROJECT":
            return self.addProject(*args, **kargs)
        elif command == "GET_PROJECT":
            return self.getProject(*args, **kargs)
        elif command == "GET_PROJECTS":
            return self.getProjects()
        elif command == "REMOVE_PROJECT":
            return self.removeProject(*args, **kargs)
        elif command == "GET_PRIORITY":
            result = {}
            for prj_id, project in self.projects.items():
                users = {}

                for user_id, user in project["users"].items():
                    p = self.calculatePriority(user_id=user_id, prj_id=prj_id)
                    users[user["name"]] = p

                result[project["name"]] = users
            return result
        elif command == "CALCULATE_PRIORITY":
            return self.calculatePriority(*args, **kargs)
        else:
            raise Exception("command=%r not supported!" % command)

    def task(self):
        with self.condition:
            try:
                self.calculateFairShare()
            except Exception as ex:
                LOG.error(ex)
                raise ex
            finally:
                self.condition.notifyAll()

    def destroy(self):
        pass

    def calculatePriority(self, user_id, prj_id, timestamp=None, retry=0):
        if prj_id not in self.projects:
            raise Exception("project=%s not found!" % prj_id)

        if user_id not in self.projects[prj_id]["users"]:
            raise Exception("user=%s not found!" % user_id)

        fair_share_cores = 0
        fair_share_ram = 0

        with self.condition:
            user = self.projects[prj_id]["users"].get(user_id)
            fair_share_cores = user["fairshare_cores"]
            fair_share_ram = user["fairshare_ram"]

            self.condition.notifyAll()

        if not timestamp:
            timestamp = datetime.utcnow()

        now = datetime.utcnow()

        diff = (now - timestamp)
        minutes = diff.seconds / 60
        priority = (float(self.age_weight) * minutes +
                    float(self.vcpus_weight) * fair_share_cores +
                    float(self.memory_weight) * fair_share_ram -
                    float(self.age_weight) * retry)

        return int(priority)

    def addProject(self, prj_id, prj_name, share=float(0)):
        if prj_id not in self.projects:
            if share == 0:
                share = self.default_share

            with self.condition:
                self.projects[prj_id] = {"id": prj_id,
                                         "name": prj_name,
                                         "type": "dynamic",
                                         "users": {},
                                         "usage": {},
                                         "share": share}
                self.condition.notifyAll()

    def getProject(self, prj_id):
        if prj_id not in self.projects:
            raise Exception("project name=%r not found!" % prj_id)

        return self.projects.get(prj_id)

    def getProjects(self):
        return self.projects

    def removeProject(self, prj_id):
        if prj_id in self.projects:
            with self.condition:
                del self.projects[prj_id]
                self.condition.notifyAll()

    def calculateFairShare(self):
        total_prj_share = float(0)
        total_usage_ram = float(0)
        total_usage_cores = float(0)
        total_actual_usage_cores = float(0)
        total_actual_usage_ram = float(0)

        users = self.keystone_manager.execute("GET_USERS")

        if not users:
            LOG.error("cannot retrieve the users list from KeystoneManager")
            return

        for user in users:
            user_id = str(user["id"])
            user_name = str(user["name"])
            user_projects = self.keystone_manager.execute("GET_USER_PROJECTS",
                                                          id=user_id)

            for project in user_projects:
                prj_id = str(project["id"])

                if prj_id not in self.projects:
                    continue

                p_users = self.projects[prj_id]["users"]

                if user_id not in p_users:
                    p_users[user_id] = {"name": user_name,
                                        "share": self.default_share,
                                        "usage": {"ram": float(0),
                                                  "cores": float(0)}}
                else:
                    p_users[user_id]["usage"]["ram"] = float(0)
                    p_users[user_id]["usage"]["cores"] = float(0)

        to_date = datetime.utcnow()

        for x in xrange(self.periods):
            default_share = self.default_share
            decay = self.decay_weight ** x
            from_date = to_date - timedelta(days=(self.period_length))

            usages = self.nova_manager.execute("GET_RESOURCE_USAGE",
                                               prj_ids=self.projects.keys(),
                                               from_date=from_date,
                                               to_date=to_date)

            for prj_id, users in usages.items():
                project = self.projects[prj_id]

                for user_id, usage_record in users.items():
                    if user_id not in project["users"]:
                        project["users"][user_id] = {"name": user_name,
                                                     "share": default_share,
                                                     "usage": {}}

                    user_usage = project["users"][user_id]["usage"]
                    user_usage["ram"] += decay * usage_record["ram"]
                    user_usage["cores"] += decay * usage_record["cores"]

                    total_usage_ram += user_usage["ram"]
                    total_usage_cores += user_usage["cores"]

            to_date = from_date

        for project in self.projects.values():
            if "share" not in project or project["share"] == 0:
                project["share"] = self.default_share

            # check the share for each user and update the usage_record
            users = project["users"]
            prj_id = project["id"]
            # prj_name = project["name"]
            prj_share = project["share"]
            sibling_share = float(0)

            for user_id, user in users.items():
                if "share" not in user or user["share"] == 0:
                    user["share"] = self.default_share

                if len(users) == 1:
                    user["share"] = prj_share
                    sibling_share = prj_share
                else:
                    sibling_share += user["share"]

            project["sibling_share"] = sibling_share
            total_prj_share += prj_share

        for prj_id, project in self.projects.items():
            sibling_share = project["sibling_share"]
            prj_share = project["share"]
            actual_usage_cores = float(0)
            actual_usage_ram = float(0)

            users = project["users"]

            for user_id, user in users.items():
                # for each user the normalized share
                # is calculated (0 <= user_norm_share <= 1)
                user_share = user["share"]
                user_usage = user["usage"]
                user_usage["norm_ram"] = user_usage["ram"]
                user_usage["norm_cores"] = user_usage["cores"]

                if prj_share > 0 and sibling_share > 0 and total_prj_share > 0:
                    user["norm_share"] = (user_share / sibling_share) * \
                                         (prj_share / total_prj_share)
                else:
                    user["norm_share"] = user_share

                if total_usage_ram > 0:
                    user_usage["norm_ram"] /= total_usage_ram

                if total_usage_cores > 0:
                    user_usage["norm_cores"] /= total_usage_cores

                actual_usage_ram += user_usage["norm_ram"]
                actual_usage_cores += user_usage["norm_cores"]

            project["usage"]["actual_ram"] = actual_usage_ram
            project["usage"]["actual_cores"] = actual_usage_cores

            total_actual_usage_ram += actual_usage_ram
            total_actual_usage_cores += actual_usage_cores

        for project in self.projects.values():
            actual_usage_ram = project["usage"]["actual_ram"]
            actual_usage_cores = project["usage"]["actual_cores"]
            prj_share = project["share"]
            sibling_share = project["sibling_share"]
            users = project["users"]

            # effect_prj_cores_usage = actual_usage_cores +
            # ((total_actual_usage_cores - actual_usage_cores) *
            # prj_share / total_prj_share)

            # effect_prj_cores_usage = actual_usage_ram +
            # ((total_actual_usage_ram - actual_usage_ram) *
            # prj_share / total_prj_share)

            effect_prj_ram_usage = actual_usage_ram
            effect_prj_cores_usage = actual_usage_cores

            project["usage"]["effective_ram"] = effect_prj_ram_usage
            project["usage"]["effective_cores"] = effect_prj_cores_usage

            for user in users.values():
                user["fairshare_ram"] = float(0)
                user["fairshare_cores"] = float(0)
                user_share = user["share"]
                user_usage = user["usage"]
                user_usage["effective_cores"] = float(0)
                user_usage["effective_ram"] = float(0)

                if user_share > 0:
                    norm_share = user["norm_share"]
                    norm_usage_ram = user_usage["norm_ram"]
                    norm_usage_cores = user_usage["norm_cores"]

                    effect_usage_ram = (norm_usage_ram + (
                                        (effect_prj_cores_usage -
                                         norm_usage_ram) *
                                        user_share / sibling_share))

                    effect_usage_cores = (norm_usage_cores + (
                                          (effect_prj_cores_usage -
                                           norm_usage_cores) *
                                          user_share / sibling_share))

                    user_usage["effective_ram"] = effect_usage_ram
                    user_usage["effective_rel_ram"] = float(0)

                    user_usage["effective_cores"] = effect_usage_cores
                    user_usage["effective_rel_cores"] = float(0)

                    if actual_usage_cores > 0:
                        user_usage["effective_rel_cores"] = norm_usage_cores
                        user_usage["effective_rel_cores"] /= actual_usage_cores

                    if actual_usage_ram > 0:
                        user_usage["effect_rel_ram"] = norm_usage_ram
                        user_usage["effect_rel_ram"] /= actual_usage_ram

                    # user["effect_usage_rel_cores"] = effect_usage_cores /
                    # effect_prj_cores_usage
                    # user["effect_usage_rel_ram"] = effect_usage_ram /
                    # effect_prj_cores_usage

                    if norm_share > 0:
                        f_ram = 2 ** (-effect_usage_ram / norm_share)
                        user["fairshare_ram"] = f_ram

                        f_cores = 2 ** (-effect_usage_cores / norm_share)
                        user["fairshare_cores"] = f_cores

            LOG.debug("fairshare project %s" % project)
