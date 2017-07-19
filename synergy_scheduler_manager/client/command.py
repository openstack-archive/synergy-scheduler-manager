from synergy.client.command import ExecuteCommand
from synergy.client.tabulate import tabulate
from synergy_scheduler_manager.common.project import Project
from synergy_scheduler_manager.common.quota import SharedQuota
from synergy_scheduler_manager.common.user import User


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


class ProjectCommand(ExecuteCommand):

    def __init__(self):
        super(ProjectCommand, self).__init__("ProjectCommand")

    def configureParser(self, subparser):
        prj_parser = subparser.add_parser('project')
        prj_subparsers = prj_parser.add_subparsers(dest="command")
        prj_subparsers.add_parser("list", add_help=True,
                                  help="shows the projects list")

        show_parser = prj_subparsers.add_parser("show", add_help=True,
                                                help="shows the project info")
        group = show_parser.add_mutually_exclusive_group(required=True)
        group.add_argument("-i", "--id", metavar="<id>")
        group.add_argument("-n", "--name", metavar="<name>")
        group.add_argument("-a", "--all", action="store_true")
        show_parser.add_argument("-r", "--share", action="store_true")
        show_parser.add_argument("-t", "--ttl", action="store_true")
        show_parser.add_argument("-p", "--p_quota", action="store_true")
        show_parser.add_argument("-s", "--s_quota", action="store_true")
        show_parser.add_argument("-q", "--queue", action="store_true")
        show_parser.add_argument("-l", "--long", action="store_true")
        show_parser.add_argument("-u", "--usage", action="store_true")

        add_parser = prj_subparsers.add_parser("add", add_help=True,
                                               help="adds a new project")
        group = add_parser.add_mutually_exclusive_group(required=True)
        group.add_argument("-i", "--id", metavar="<id>")
        group.add_argument("-n", "--name", metavar="<name>")
        add_parser.add_argument("-s", "--share", metavar="<share>")
        add_parser.add_argument("-t", "--ttl", metavar="<TTL>")

        remove_parser = prj_subparsers.add_parser("remove", add_help=True,
                                                  help="removes a project")
        group = remove_parser.add_mutually_exclusive_group(required=True)
        group.add_argument("-i", "--id", metavar="<id>")
        group.add_argument("-n", "--name", metavar="<name>")

        set_parser = prj_subparsers.add_parser("set", add_help=True,
                                               help="sets the project values")
        group = set_parser.add_mutually_exclusive_group(required=True)
        group.add_argument("-i", "--id", metavar="<id>")
        group.add_argument("-n", "--name", metavar="<name>")
        set_parser.add_argument("-s", "--share", metavar="<share>")
        set_parser.add_argument("-t", "--ttl", metavar="<TTL>")

    def execute(self, synergy_url, args):
        id = getattr(args, 'id', None)
        name = getattr(args, 'name', None)
        command = getattr(args, 'command', None)
        headers = ["name"]

        if command == "list":
            cmd_args = {}
            command = "GET_PROJECTS"

        elif command == "show":
            if args.all:
                cmd_args = {}
                command = "GET_PROJECTS"
            else:
                cmd_args = {"id": id, "name": name}
                command = "GET_PROJECT"

            if args.long:
                headers.insert(0, "id")
            if args.usage:
                headers.append("usage")
            if args.p_quota:
                headers.append("private quota")
            if args.s_quota:
                headers.append("shared quota")
            if args.queue:
                headers.append("queue")
            if args.share:
                headers.append("share")
            if args.ttl:
                headers.append("TTL")

        elif command == "remove":
            cmd_args = {"id": id, "name": name}
            command = "REMOVE_PROJECT"

        else:
            cmd_args = {"id": id, "name": name}

            TTL = getattr(args, 'ttl', None)
            share = getattr(args, 'share', None)

            if TTL:
                cmd_args["TTL"] = TTL

            if share:
                cmd_args["share"] = share

            if command == "add":
                command = "ADD_PROJECT"
                headers.append("share")
                headers.append("TTL")
            elif command == "set":
                command = "UPDATE_PROJECT"

                if TTL:
                    headers.append("TTL")

                if share:
                    headers.append("share")

        result = super(ProjectCommand, self).execute(synergy_url,
                                                     "ProjectManager",
                                                     command,
                                                     args=cmd_args)

        if isinstance(result, Project):
            self.printProjects([result], headers)
        else:
            self.printProjects(result, headers)

    def printProjects(self, projects, headers):
        if not projects:
            return

        table = []

        for project in projects:
            row = []
            for attribute in headers:
                if attribute == "id":
                    row.append(project.getId())

                if attribute == "name":
                    row.append(project.getName())

                if attribute == "share":
                    share = project.getShare()
                    share_value = share.getValue()
                    share_norm = share.getNormalizedValue()
                    row.append("{:.2f}% | {:.2f}%".format(share_value,
                                                          share_norm * 100))

                if attribute == "TTL":
                    row.append(project.getTTL())

                if attribute == "private quota":
                    quota = project.getQuota()

                    private = "vcpus: {:.1f} of {:.1f} | ram: "\
                        "{:.1f} of {:.1f}".format(quota.getUsage("vcpus"),
                                                  quota.getSize("vcpus"),
                                                  quota.getUsage("memory"),
                                                  quota.getSize("memory"))

                    row.append(private)

                if attribute == "shared quota":
                    quota = project.getQuota()

                    vcpus_size = quota.getSize("vcpus", private=False)
                    vcpus_usage = quota.getUsage("vcpus", private=False)
                    memory_size = quota.getSize("memory", private=False)
                    memory_usage = quota.getUsage("memory", private=False)

                    shared = "vcpus: {:.1f} of {:.1f} | "\
                             "ram: {:.1f} of {:.1f}".format(
                                 vcpus_usage, vcpus_size,
                                 memory_usage, memory_size)

                    row.append(shared)

                if attribute == "usage":
                    data = project.getData()

                    usage = "vcpus: {:.2f}% | ram: {:.2f}%".format(
                        data["effective_vcpus"] * 100,
                        data["effective_memory"] * 100)

                    row.append(usage)
            table.append(row)

        print(tabulate(table, headers, tablefmt="fancy_grid"))


class QueueCommand(ExecuteCommand):

    def __init__(self):
        super(QueueCommand, self).__init__("QueueCommand")

    def configureParser(self, subparser):
        queue_parser = subparser.add_parser('queue')
        queue_subparsers = queue_parser.add_subparsers(dest="command")
        queue_subparsers.add_parser("show", add_help=True,
                                    help="shows the queue info")

    def execute(self, synergy_url, args):
        if args.command == "show":
            command = "GET_QUEUE"
            cmd_args = {"name": "DYNAMIC"}

            queue = super(QueueCommand, self).execute(synergy_url,
                                                      "QueueManager",
                                                      command,
                                                      args=cmd_args)
            table = []
            headers = ["name", "size", "is open"]

            row = []
            row.append(queue.getName())
            row.append(queue.getSize())
            row.append(str(queue.isOpen()).lower())

            table.append(row)

            print(tabulate(table, headers, tablefmt="fancy_grid"))


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
            share = project.getShare()
            norm_share = share.getNormalizedValue()
            quota = project.getQuota()
            vcpus_size = quota.getSize("vcpus", private=False)
            vcpus_usage = quota.getUsage("vcpus", private=False)
            memory_size = quota.getSize("memory", private=False)
            memory_usage = quota.getUsage("memory", private=False)

            row = []
            row.append(project.getName())

            private = "vcpus: {:.2f} of {:.2f} | memory: {:.2f} of "\
                      "{:.2f}".format(quota.getUsage("vcpus"),
                                      quota.getSize("vcpus"),
                                      quota.getUsage("memory"),
                                      quota.getSize("memory"))

            shared = "vcpus: {:.2f} of {:.2f} | memory: {:.2f} of {:.2f} | "\
                     "share: {:.2f}% | TTL: {:.2f}".format(vcpus_usage,
                                                           vcpus_size,
                                                           memory_usage,
                                                           memory_size,
                                                           norm_share * 100,
                                                           project.getTTL())

            row.append(private)
            row.append(shared)

            table.append(row)

        print(tabulate(table, headers, tablefmt="fancy_grid"))

    def printSharedQuota(self, quota):
        table = []
        headers = ["resource", "used", "size"]
        resources = ["vcpus", "memory", "instances"]

        for resource in resources:
            row = [resource, quota.getUsage(resource), quota.getSize(resource)]
            table.append(row)

        print(tabulate(table, headers, tablefmt="fancy_grid"))


class UsageCommand(ExecuteCommand):

    def __init__(self):
        super(UsageCommand, self).__init__("UsageCommand")

    def configureParser(self, subparser):
        usage_parser = subparser.add_parser('usage')
        usage_subparsers = usage_parser.add_subparsers(dest="command")
        show_parser = usage_subparsers.add_parser("show", add_help=True,
                                                  help="shows the usage info")

        subparsers = show_parser.add_subparsers()
        parser_a = subparsers.add_parser('project', help='project help')

        group = parser_a.add_mutually_exclusive_group()
        group.add_argument("-d", "--project_id", metavar="<id>")
        group.add_argument("-m", "--project_name", metavar="<name>")
        group.add_argument("-a", "--all_projects", action="store_true")

        parser_b = subparsers.add_parser('user', help='user help')

        group = parser_b.add_mutually_exclusive_group(required=True)
        group.add_argument("-d", "--project_id", metavar="<id>")
        group.add_argument("-m", "--project_name", metavar="<name>")

        group = parser_b.add_mutually_exclusive_group(required=True)
        group.add_argument("-i", "--user_id", metavar="<id>")
        group.add_argument("-n", "--user_name", metavar="<name>")
        group.add_argument("-a", "--all_users", action="store_true")

    def execute(self, synergy_url, args):
        if args.command == "show":
            command = "show"
            user_id = None
            if hasattr(args, "user_id"):
                user_id = args.user_id

            user_name = None
            if hasattr(args, "user_name"):
                user_name = args.user_name

            all_users = False
            if hasattr(args, "all_users"):
                all_users = args.all_users

            project_id = None
            if hasattr(args, "project_id"):
                project_id = args.project_id

            project_name = None
            if hasattr(args, "project_name"):
                project_name = args.project_name

            all_projects = False
            if hasattr(args, "all_projects"):
                all_projects = args.all_projects

            cmd_args = {"user_id": user_id,
                        "user_name": user_name,
                        "all_users": all_users,
                        "project_id": project_id,
                        "project_name": project_name,
                        "all_projects": all_projects}

            result = super(UsageCommand, self).execute(synergy_url,
                                                       "SchedulerManager",
                                                       command,
                                                       args=cmd_args)

            if isinstance(result, Project):
                self.printProjects([result])
            elif isinstance(result, User):
                self.printUsers([result])
            elif isinstance(result, list):
                if all(isinstance(n, Project) for n in result):
                    self.printProjects(result)
                else:
                    self.printUsers(result)

    def printProjects(self, projects):
        if not projects:
            return

        data = projects[0].getData()
        date_format = "{:%d %b %Y %H:%M:%S}"
        from_date = date_format.format(data["time_window_from_date"])
        to_date = date_format.format(data["time_window_to_date"])

        headers = ["project",
                   "shared quota (%s - %s)" % (from_date, to_date),
                   "share"]

        table = []

        for project in projects:
            data = project.getData()
            share = project.getShare()
            row = []
            row.append(project.getName())

            shared = "vcpus: {:.2f}% | memory: {:.2f}%".format(
                data["effective_vcpus"] * 100, data["effective_memory"] * 100)

            row.append(shared)
            row.append("{:.2f}%".format(share.getNormalizedValue() * 100))

            table.append(row)

        print(tabulate(table, headers, tablefmt="fancy_grid"))

    def printUsers(self, users):
        if not users:
            return

        table = []

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

        print(tabulate(table, headers, tablefmt="fancy_grid"))
