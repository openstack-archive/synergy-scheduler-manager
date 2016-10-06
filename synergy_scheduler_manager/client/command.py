from synergy.client.command import ExecuteCommand
from synergy_scheduler_manager.common.quota import SharedQuota
from synergy_scheduler_manager.common.project import Project
from synergy_scheduler_manager.common.queue import Queue
from synergy_scheduler_manager.common.user import User
from tabulate import tabulate


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


class QueueCommand(ExecuteCommand):

    def __init__(self):
        super(QueueCommand, self).__init__("QueueCommand")

    def configureParser(self, subparser):
        queue_parser = subparser.add_parser('queue')
        queue_subparsers = queue_parser.add_subparsers(dest="command")
        show_parser = queue_subparsers.add_parser("show", add_help=True,
                                                  help="shows the queue info")

    def execute(self, synergy_url, args):
        if args.command == "show":
            command = "GET_QUEUE"
            cmd_args = {"name": "DYNAMIC"}

            queue = super(QueueCommand, self).execute(synergy_url,
                                                      "QueueManager",
                                                      command,
                                                      args=cmd_args)

            if not isinstance(queue, Queue):
                print("wrong data")

            table = []
            headers = ["name", "size", "is open"]

            row = []
            row.append(queue.getName())
            row.append(queue.getSize())
            row.append(str(queue.isOpen()).lower())

            table.append(row)

        print tabulate(table, headers, tablefmt="fancy_grid")


class QuotaCommand(ExecuteCommand):

    def __init__(self):
        super(QuotaCommand, self).__init__("QuotaCommand")

    def configureParser(self, subparser):
        quota_parser = subparser.add_parser('quota')
        quota_subparsers = quota_parser.add_subparsers(dest="command")
        show_parser = quota_subparsers.add_parser("show", add_help=True,
                                                  help="shows the quota info")
        group = show_parser.add_mutually_exclusive_group()
        group.add_argument("-i", "--project_id", metavar="<id>")
        group.add_argument("-n", "--project_name", metavar="<name>")
        group.add_argument("-a", "--all_projects", action="store_true")
        group.add_argument("-s", "--shared", action="store_true")

    def execute(self, synergy_url, args):
        if args.command == "show":
            command = "show"
            cmd_args = {"shared": args.shared,
                        "project_id": args.project_id,
                        "project_name": args.project_name,
                        "all_projects": args.all_projects}

            result = super(QuotaCommand, self).execute(synergy_url,
                                                       "QuotaManager",
                                                       command,
                                                       args=cmd_args)

            if isinstance(result, SharedQuota):
                self.printSharedQuota(result)
            elif isinstance(result, Project):
                self.printProjects([result])
            else:
                self.printProjects(result)

    def printProjects(self, projects):
        table = []
        headers = ["project", "private quota", "shared quota", "share", "TTL"]

        for project in projects:
            data = project.getData()
            share = project.getShare()
            quota = project.getQuota()
            vcpus_size = quota.getSize("vcpus", private=False)
            vcpus_usage = quota.getUsage("vcpus", private=False)
            memory_size = quota.getSize("memory", private=False)
            memory_usage = quota.getUsage("memory", private=False)

            row = []
            row.append(project.getName())

            private = "vcpus: {:.2f} of {:.2f} | memory: {:.2f} of {:.2f}".format(
                quota.getUsage("vcpus"), quota.getSize("vcpus"),
                quota.getUsage("memory"), quota.getSize("memory"))

            shared = "vcpus: {:.2f} of {:.2f} | memory: {:.2f} of {:.2f} | share: {:.2f}% | TTL: {:.2f}".format(
                vcpus_usage, vcpus_size, memory_usage, memory_size,
                share.getNormalizedValue(), project.getTTL())

            row.append(private)
            row.append(shared)
            # row.append(str(project.isEnabled()).lower())

            table.append(row)

        print tabulate(table, headers, tablefmt="fancy_grid")

    def printSharedQuota(self, quota):
        table = []
        headers = ["resource", "used", "size"]
        resources = ["vcpus", "memory", "instances"]

        for resource in resources:
            row = [resource, quota.getUsage(resource), quota.getSize(resource)]
            table.append(row)

        print tabulate(table, headers, tablefmt="fancy_grid")


class UsageCommand(ExecuteCommand):

    def __init__(self):
        super(UsageCommand, self).__init__("UsageCommand")

    def configureParser(self, subparser):
        usage_parser = subparser.add_parser('usage')
        usage_subparsers = usage_parser.add_subparsers(dest="command")
        show_parser = usage_subparsers.add_parser("show", add_help=True,
                                                  help="shows the usage info")
        group = show_parser.add_mutually_exclusive_group()
        group.add_argument("-i", "--user_id", metavar="<id>")
        group.add_argument("-n", "--user_name", metavar="<name>")
        group.add_argument("-a", "--all_users", action="store_true")

        ##### REQUIRED!!!!
        group = show_parser.add_mutually_exclusive_group()
        group.add_argument("-d", "--project_id", metavar="<id>")
        group.add_argument("-m", "--project_name", metavar="<name>")

    def execute(self, synergy_url, args):
        if args.command == "show":
            command = "show"
            cmd_args = {"user_id": args.user_id,
                        "user_name": args.user_name,
                        "all_users": args.all_users,
                        "project_id": args.project_id,
                        "project_name": args.project_name}

            result = super(UsageCommand, self).execute(synergy_url,
                                                      "SchedulerManager",
                                                      command,
                                                      args=cmd_args)

            if isinstance(result, Project):
                self.printProject(result)
            elif isinstance(result, User):
                self.printUsers([result])
            else:
                self.printUsers(result)

    def printProject(self, project):
        if not project:
            return

        data = project.getData()
        share = project.getShare()
        date_format = "{:%d %b %Y %H:%M:%S}"
        from_date = date_format.format(data["time_window_from_date"])
        to_date = date_format.format(data["time_window_to_date"])

        headers = ["project",
                   "shared quota (%s - %s)" % (from_date, to_date),
                   "share"]

        table = []
        row = []
        row.append(project.getName())

        shared = "vcpus: {:.2f}% | memory: {:.2f}%".format(
            data["effective_vcpus"] * 100, data["effective_memory"] * 100)

        row.append(shared)
        row.append("{:.2f}%".format(share.getNormalizedValue() * 100))

        table.append(row)

        print tabulate(table, headers, tablefmt="fancy_grid")

    def printUsers(self, users):
        if not users:
            return

        table = []

        if not users:
            return

        date_format = "{:%d %b %Y %H:%M:%S}"
        data = users[0].getData()
        from_date = date_format.format(data["time_window_from_date"])
        to_date = date_format.format(data["time_window_to_date"])

        headers = ["user",
                   "shared quota (%s - %s)" % (from_date, to_date),
                   "share",
                   "priority"]

        for user in users:
            share = user.getShare()
            data = user.getData()
            priority = user.getPriority()

            row = []
            row.append(user.getName())

            row.append("vcpus: {:.2f}% | memory: {:.2f}%".format(
                data["actual_rel_vcpus"] * 100,
                data["actual_rel_memory"] * 100))

            row.append("{:.2f}%".format(share.getNormalizedValue() * 100))
            row.append("{:.2f}".format(priority.getValue()))

            table.append(row)

        print tabulate(table, headers, tablefmt="fancy_grid")
