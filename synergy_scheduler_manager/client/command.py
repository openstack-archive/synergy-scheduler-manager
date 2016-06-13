from synergy.client.command import Execute

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


class GetQuota(Execute):

    def __init__(self):
        super(GetQuota, self).__init__("GET_DYNAMIC_QUOTA")

    def configureParser(self, subparser):
        parser = subparser.add_parser("get_quota",
                                      add_help=True,
                                      help="shows the dynamic quota info")
        parser.add_argument("--long",
                            action='store_true',
                            help="shows more details")

    def sendRequest(self, synergy_url, args):
        self.long = args.long

        super(GetQuota, self).sendRequest(
            synergy_url + "/synergy/execute", "QuotaManager", self.getName())

    def log(self):
        quota = self.getResults()

        if not self.long:
            cores_in_use = "{:d}".format(quota["cores"]["in_use"])
            max_cores_in_use = max(len(cores_in_use), len("in use"))

            cores_limit = "{:.2f}".format(quota["cores"]["limit"])
            max_cores_limit = max(len(cores_limit), len("limit"))

            ram_in_use = "{:d}".format(quota["ram"]["in_use"])
            max_ram_in_use = max(len(ram_in_use), len("in use"))

            ram_limit = "{:.2f}".format(quota["ram"]["limit"])
            max_ram_limit = max(len(ram_limit), len("limit"))

            separator = "-" * (max_cores_in_use + max_cores_limit +
                               max_ram_in_use + max_ram_limit + 7) + "\n"

            raw = "| {0:%ss} | {1:%ss} | {2:%ss} |\n" % (
                len("ram (MB)"),
                max(max_cores_in_use, max_ram_in_use),
                max(max_cores_limit, max_ram_limit))

            msg = separator
            msg += raw.format("type", "in use", "limit")
            msg += separator
            msg += raw.format("ram (MB)", ram_in_use, ram_limit)
            msg += raw.format("cores", cores_in_use, cores_limit)
            msg += separator

            print(msg)
        else:
            max_ram = 0
            max_ram_in_use = len("{:d}".format(quota["ram"]["in_use"]))
            max_ram_limit = len("{:.2f}".format(quota["ram"]["limit"]))
            max_cores = 0
            max_cores_in_use = len("{:d}".format(quota["cores"]["in_use"]))
            max_cores_limit = len("{:.2f}".format(quota["cores"]["limit"]))
            max_prj_name = len("project")

            for project in quota["projects"].values():
                max_prj_name = max(len(project["name"]), max_prj_name)
                max_ram = max(len("{:d}".format(project["ram"])), max_ram)
                max_cores = max(len("{:d}".format(project["cores"])),
                                max_cores)

            separator = "-" * (max_prj_name + max_cores + max_cores_in_use +
                               max_cores_limit + max_ram + max_ram_in_use +
                               max_ram_limit + 48)

            title = "| {0:%ss} | {1:%ss} | {2:%ss} |\n" % (
                max_prj_name,
                max_cores + max_cores_in_use + max_cores_limit + 19,
                max_ram + max_ram_in_use + max_ram_limit + 19)

            raw = "| {0:%ss} | in use={1:%d} ({2:%d}) | limit={3:%ss} |" \
                  " in use={4:%d} ({5:%d}) | limit={6:%ss} |\n"
            raw = raw % (max_prj_name, max_cores, max_cores_in_use,
                         max_cores_limit, max_ram, max_ram_in_use,
                         max_ram_limit)

            msg = separator + "\n"
            msg += title.format("project", "cores", "ram (MB)")
            msg += separator + "\n"

            for project in quota["projects"].values():
                msg += raw.format(
                    project["name"], project["cores"],
                    quota["cores"]["in_use"],
                    "{:.2f}".format(quota["cores"]["limit"]),
                    project["ram"],
                    quota["ram"]["in_use"],
                    "{:.2f}".format(quota["ram"]["limit"]))
            msg += separator + "\n"

            print(msg)


class GetPriority(Execute):

    def __init__(self):
        super(GetPriority, self).__init__("GET_PRIORITY")

    def configureParser(self, subparser):
        subparser.add_parser("get_priority",
                             add_help=True,
                             help="shows the users priority")

    def sendRequest(self, synergy_url, args):
        super(GetPriority, self).sendRequest(
            synergy_url + "/synergy/execute",
            "FairShareManager",
            self.getName())

    def log(self):
        projects = self.getResults()

        max_prj = len("project")
        max_user = len("user")
        max_priority = len("priority")

        for prj_name, users in projects.items():
            max_prj = max(len(prj_name), max_prj)

            for user_name, priority in users.items():
                max_user = max(len(user_name), max_user)
                max_priority = max(len("{:.2f}".format(priority)),
                                   max_priority)

        separator = "-" * (max_prj + max_user + max_priority + 10) + "\n"

        raw = "| {0:%ss} | {1:%ss} | {2:%ss} |\n" % (
            max_prj, max_user, max_priority)

        msg = separator
        msg += raw.format("project", "user", "priority")
        msg += separator

        for prj_name in sorted(projects.keys()):
            for user_name in sorted(projects[prj_name].keys()):
                msg += raw.format(
                    prj_name,
                    user_name,
                    "{:.2f}".format(projects[prj_name][user_name]))

        msg += separator

        print(msg)


class GetQueue(Execute):

    def __init__(self):
        super(GetQueue, self).__init__("GET_QUEUE")

    def configureParser(self, subparser):
        subparser.add_parser("get_queue",
                             add_help=True,
                             help="shows the queue info")

    def sendRequest(self, synergy_url, args):
        super(GetQueue, self).sendRequest(
            synergy_url + "/synergy/execute",
            "QueueManager",
            self.getName(),
            {"name": "DYNAMIC"})

    def log(self):
        queue = self.getResults()

        max_status = len("status")
        max_queue = max(len(queue["name"]), len("queue"))
        max_size = max(len("{:d}".format(queue["size"])), len("size"))

        separator = "-" * (max_queue + max_status + max_size + 10) + "\n"

        raw = "| {0:%ss} | {1:%ss} | {2:%ss} |\n" % (
            max_queue, max_status, max_size)

        msg = separator
        msg += raw.format("queue", "status", "size")
        msg += separator

        msg += raw.format(queue["name"],
                          queue["status"],
                          "{:d}".format(queue["size"]))

        msg += separator

        print(msg)


class GetShare(Execute):

    def __init__(self):
        super(GetShare, self).__init__("GET_SHARE")

    def configureParser(self, subparser):
        parser = subparser.add_parser("get_share",
                                      add_help=True,
                                      help="shows the users share")

        parser.add_argument("--long",
                            action='store_true',
                            help="shows more details")

    def sendRequest(self, synergy_url, args):
        self.long = args.long

        super(GetShare, self).sendRequest(
            synergy_url + "/synergy/execute",
            "FairShareManager",
            "GET_PROJECTS")

    def log(self):
        projects = self.getResults()

        max_prj = len("project")
        max_usr = len("user")
        max_prj_share = len("share")
        max_usr_share = len("share")

        if self.long:
            for project in projects.values():
                max_prj = max(len(project["name"]), max_prj)
                max_prj_share = max(len("{:.2f}% ({:.2f})".format(
                    project["norm_share"] * 100, project["share"])),
                    max_prj_share)

                for user in project["users"].values():
                    max_usr = max(len(user["name"]), max_usr)
                    max_usr_share = max(
                        len("{:.2f}%".format(user["norm_share"] * 100)),
                        max_usr_share)

            separator = "-" * (max_prj + max_usr + max_prj_share +
                               max_usr_share + 13) + "\n"

            raw = "| {0:%ss} | {1:%ss} | {2:%ss} | {3:%ss} |\n" % (
                  max_prj, max_prj_share, max_usr, max_usr_share)

            msg = separator
            msg += raw.format("project", "share", "user", "share")
            msg += separator

            for project in projects.values():
                for user in project["users"].values():
                    msg += raw.format(
                        project["name"],
                        "{:.2f}% ({:.2f})".format(project["norm_share"] * 100,
                                                  project["share"]),
                        user["name"],
                        "{:.2f}%".format(user["norm_share"] * 100))

            msg += separator
            print(msg)
        else:
            for project in projects.values():
                max_prj = max(len(project["name"]), max_prj)
                max_prj_share = max(len("{:.2f}% ({:.2f})".format(
                    project["norm_share"] * 100, project["share"])),
                    max_prj_share)

            separator = "-" * (max_prj + max_prj_share + 7) + "\n"

            raw = "| {0:%ss} | {1:%ss} |\n" % (max_prj, max_prj_share)

            msg = separator
            msg += raw.format("project", "share")
            msg += separator

            for project in projects.values():
                msg += raw.format(
                    project["name"],
                    "{:.2f}% ({:.2f})".format(project["norm_share"] * 100,
                                              project["share"]))

            msg += separator
            print(msg)


class GetUsage(Execute):

    def __init__(self):
        super(GetUsage, self).__init__("GET_USAGE")

    def configureParser(self, subparser):
        subparser.add_parser("get_usage",
                             add_help=True,
                             help="retrieve the resource usages")

    def sendRequest(self, synergy_url, args):
        super(GetUsage, self).sendRequest(
            synergy_url + "/synergy/execute",
            "FairShareManager",
            "GET_PROJECTS")

    def log(self):
        projects = self.getResults()

        max_prj = len("project")
        max_usr = len("user")
        max_prj_cores = len("cores")
        max_usr_cores = len("cores")
        max_prj_ram = len("ram")
        max_usr_ram = len("ram (abs)")

        for project in projects.values():
            usage = project["usage"]

            max_prj = max(len(project["name"]), max_prj)
            max_prj_cores = max(len(
                "{:.2f}%".format(usage["effective_cores"] * 100)),
                max_prj_cores)

            max_prj_ram = max(len(
                "{:.2f}%".format(usage["effective_ram"] * 100)),
                max_prj_ram)

            for user in project["users"].values():
                usage = user["usage"]

                max_usr = max(len(user["name"]), max_usr)
                max_usr_cores = max(len("{:.2f}% ({:.2f})%".format(
                                    usage["effective_rel_cores"] * 100,
                                    usage["norm_cores"] * 100)),
                                    max_usr_cores)

                max_usr_ram = max(len("{:.2f}% ({:.2f})%".format(
                                  usage["effective_rel_ram"] * 100,
                                  usage["norm_ram"] * 100)),
                                  max_usr_ram)

        separator = "-" * (max_prj + max_usr + max_prj_cores +
                           max_usr_cores + max_prj_ram +
                           max_usr_ram + 19) + "\n"

        raw = "| {0:%ss} | {1:%ss} | {2:%ss} | {3:%ss} | {4:%ss} | " \
              "{5:%ss} | \n" % (max_prj, max_prj_cores, max_prj_ram,
                                max_usr, max_usr_cores, max_usr_ram)

        msg = separator
        msg += raw.format("project", "cores", "ram",
                          "user", "cores (abs)", "ram (abs)")
        msg += separator

        for project in projects.values():
            prj_usage = project["usage"]

            for user in project["users"].values():
                usr_usage = user["usage"]

                prj_cores = "{:.2f}%".format(
                    prj_usage["effective_cores"] * 100)

                prj_ram = "{:.2f}%".format(prj_usage["effective_ram"] * 100)

                usr_cores = "{:.2f}% ({:.2f}%)".format(
                    usr_usage["effective_rel_cores"] * 100,
                    usr_usage["norm_cores"] * 100)

                usr_ram = "{:.2f}% ({:.2f}%)".format(
                    usr_usage["effective_rel_ram"] * 100,
                    usr_usage["norm_ram"] * 100)

                msg += raw.format(
                    project["name"], prj_cores, prj_ram,
                    user["name"], usr_cores, usr_ram)
        msg += separator
        print(msg)
