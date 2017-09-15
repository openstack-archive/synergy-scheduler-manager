import logging

from datetime import datetime
from datetime import timedelta
from oslo_config import cfg
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


CONF = cfg.CONF
LOG = logging.getLogger(__name__)


class FairShareManager(Manager):

    def __init__(self):
        super(FairShareManager, self).__init__("FairShareManager")

        self.config_opts = [
            cfg.IntOpt('periods', default=3),
            cfg.IntOpt('period_length', default=7),
            cfg.FloatOpt('default_share', default=10.0),
            cfg.FloatOpt('decay_weight', default=0.5,
                         help="the decay weight (float value [0,1])"),
            cfg.IntOpt('age_weight', default=10, help="the age weight"),
            cfg.IntOpt('vcpus_weight', default=100, help="the vcpus weight"),
            cfg.IntOpt('memory_weight', default=70, help="the memory weight")
        ]

    def setup(self):
        if self.getManager("NovaManager") is None:
            raise SynergyError("NovaManager not found!")

        if self.getManager("ProjectManager") is None:
            raise SynergyError("ProjectManager not found!")

        self.nova_manager = self.getManager("NovaManager")
        self.project_manager = self.getManager("ProjectManager")

        self.periods = CONF.FairShareManager.periods
        self.period_length = CONF.FairShareManager.period_length
        self.default_share = float(CONF.FairShareManager.default_share)
        self.decay_weight = CONF.FairShareManager.decay_weight
        self.vcpus_weight = CONF.FairShareManager.vcpus_weight
        self.age_weight = CONF.FairShareManager.age_weight
        self.memory_weight = CONF.FairShareManager.memory_weight

        if self.decay_weight < 0:
            self.decay_weight = float(0)
        elif self.decay_weight > 1:
            self.decay_weight = float(1)

    def task(self):
        try:
            self._calculateFairShare()
        except Exception as ex:
            LOG.error(ex)
            raise ex

    def destroy(self):
        pass

    def doOnEvent(self, event_type, *args, **kwargs):
        if event_type == "USER_ADDED":
            user = kwargs.get("user", None)

            if not user:
                return

            share = user.getShare()

            if not share or share.getValue() == 0:
                share.setValue(self.default_share)

        elif event_type == "PROJECT_ADDED":
            project = kwargs.get("project", None)

            if not project:
                return

            share = project.getShare()

            if not share or share.getValue() == 0:
                share.setValue(self.default_share)

        elif event_type == "PROJECT_REMOVED" or\
                event_type == "PROJECT_UPDATED":
            pass
        else:
            return

        self._calculateShares()
        self._calculateFairShare()

    def _calculateShares(self):
        total_prj_share = float(0)
        projects = self.project_manager.getProjects()

        for project in projects:
            prj_share = project.getShare()

            # check the share for each user and update the usage_record
            sibling_share = float(0)

            for user in project.getUsers():
                user_share = user.getShare()

                if user_share.getValue() == 0:
                    user_share.setValue(self.default_share)

                if len(project.getUsers()) == 1:
                    user_share.setValue(prj_share.getValue())
                    sibling_share = prj_share.getValue()
                else:
                    sibling_share += user_share.getValue()

            for user in project.getUsers():
                user_share = user.getShare()
                user_share.setSiblingValue(sibling_share)

            total_prj_share += prj_share.getValue()

        for project in projects:
            prj_share = project.getShare()
            prj_share.setSiblingValue(total_prj_share)
            prj_share.setNormalizedValue(
                prj_share.getValue() / prj_share.getSiblingValue())

            for user in project.getUsers():
                # for each user the normalized share
                # is calculated (0 <= user_norm_share <= 1)
                usr_share = user.getShare()
                usr_share.setNormalizedValue(
                    usr_share.getValue() / usr_share.getSiblingValue() *
                    prj_share.getNormalizedValue())

    def _calculateFairShare(self):
        projects = self.project_manager.getProjects()

        if not projects:
            return

        total_memory = float(0)
        total_vcpus = float(0)

        to_date = datetime.utcnow()

        time_window_from_date = to_date
        time_window_to_date = to_date

        for project in projects:
            prj_data = project.getData()
            prj_data["actual_vcpus"] = float(0)
            prj_data["actual_memory"] = float(0)
            prj_data["effective_vcpus"] = float(0)
            prj_data["effective_memory"] = float(0)
            prj_data["time_window_from_date"] = time_window_from_date
            prj_data["time_window_to_date"] = time_window_to_date

            for user in project.getUsers():
                usr_data = user.getData()
                usr_data["vcpus"] = float(0)
                usr_data["memory"] = float(0)
                usr_data["actual_vcpus"] = float(0)
                usr_data["actual_memory"] = float(0)
                usr_data["effective_vcpus"] = float(0)
                usr_data["effective_memory"] = float(0)
                usr_data["actual_rel_vcpus"] = float(0)
                usr_data["actual_rel_memory"] = float(0)
                usr_data["time_window_from_date"] = time_window_from_date
                usr_data["time_window_to_date"] = time_window_to_date

        for period in xrange(self.periods):
            decay = self.decay_weight ** period
            from_date = to_date - timedelta(days=(self.period_length))
            time_window_from_date = from_date

            for project in projects:
                usages = self.nova_manager.getProjectUsage(
                    project.getId(), from_date, to_date)

                for user_id, usage_rec in usages.items():
                    decay_vcpus = decay * usage_rec["vcpus"]
                    decay_memory = decay * usage_rec["memory"]

                    user = project.getUser(id=user_id)

                    if user:
                        data = user.getData()
                        data["vcpus"] += decay_vcpus
                        data["memory"] += decay_memory

                    total_vcpus += decay_vcpus
                    total_memory += decay_memory

            to_date = from_date

        for project in projects:
            prj_data = project.getData()
            prj_data["time_window_to_date"] = time_window_to_date

            for user in project.getUsers():
                usr_data = user.getData()
                usr_data["actual_memory"] = usr_data["memory"]
                usr_data["actual_vcpus"] = usr_data["vcpus"]
                usr_data["time_window_to_date"] = time_window_to_date

                if total_memory > 0:
                    usr_data["actual_memory"] /= total_memory

                if total_vcpus > 0:
                    usr_data["actual_vcpus"] /= total_vcpus

                prj_data["actual_memory"] += usr_data["actual_memory"]
                prj_data["actual_vcpus"] += usr_data["actual_vcpus"]

        for project in projects:
            prj_data = project.getData()
            prj_data["effective_memory"] = prj_data["actual_memory"]
            prj_data["effective_vcpus"] = prj_data["actual_vcpus"]

            for user in project.getUsers():
                usr_priority = user.getPriority()
                usr_share = user.getShare()
                share = usr_share.getValue()
                sibling_share = usr_share.getSiblingValue()
                norm_share = usr_share.getNormalizedValue()
                usr_data = user.getData()

                if prj_data["actual_vcpus"] > 0:
                    usr_data["actual_rel_vcpus"] = usr_data["actual_vcpus"]
                    usr_data["actual_rel_vcpus"] /= prj_data["actual_vcpus"]

                if prj_data["actual_memory"] > 0:
                    usr_data["actual_rel_memory"] = usr_data["actual_memory"]
                    usr_data["actual_rel_memory"] /= prj_data["actual_memory"]

                effective_memory = (usr_data["actual_memory"] + (
                                    (prj_data["effective_memory"] -
                                     usr_data["actual_memory"]) *
                                    share / sibling_share))

                effective_vcpus = (usr_data["actual_vcpus"] + (
                                   (prj_data["effective_vcpus"] -
                                    usr_data["actual_vcpus"]) *
                                   share / sibling_share))

                usr_data["effective_memory"] = effective_memory
                usr_data["effective_vcpus"] = effective_vcpus

                f_memory = 2 ** (-effective_memory / norm_share)
                usr_priority.setFairShare("memory", f_memory)

                f_vcpus = 2 ** (-effective_vcpus / norm_share)
                usr_priority.setFairShare("vcpus", f_vcpus)

                usr_priority.setValue(float(self.vcpus_weight) * f_vcpus +
                                      float(self.memory_weight) * f_memory)

                self.notify(event_type="USER_PRIORITY_UPDATED", user=user)
