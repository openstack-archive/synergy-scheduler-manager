from synergy.client.command import ExecuteCommand
from synergy.client.tabulate import tabulate
from synergy_scheduler_manager.common.project import Project


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
                headers.append("queue usage")
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
                    row.append("{:.2f}% | {:.2%}".format(share_value,
                                                         share_norm))

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

                    usage = "vcpus: {:.2%} | ram: {:.2%}".format(
                        data["effective_vcpus"], data["effective_memory"])

                    row.append(usage)

                if attribute == "queue usage":
                    data = project.getData()
                    q_usage = data.get("queue_usage", 0)
                    q_size = data.get("queue_size", 0)

                    if q_size:
                        usage = float(q_usage) / float(q_size)
                        row.append("{:d} ({:.2%})".format(q_usage, usage))
                    else:
                        row.append("0 (0%)")

            table.append(row)

        print(tabulate(table, headers, tablefmt="fancy_grid"))


class UserCommand(ExecuteCommand):

    def __init__(self):
        super(UserCommand, self).__init__("UserCommand")

    def configureParser(self, subparser):
        usr_parser = subparser.add_parser('user')
        usr_subparsers = usr_parser.add_subparsers(dest="command")

        show_parser = usr_subparsers.add_parser("show", add_help=True,
                                                help="shows the user info")
        group = show_parser.add_mutually_exclusive_group(required=True)
        group.add_argument("-i", "--id", metavar="<id>")
        group.add_argument("-n", "--name", metavar="<name>")
        group.add_argument("-a", "--all", action="store_true")
        group2 = show_parser.add_mutually_exclusive_group(required=True)
        group2.add_argument("-j", "--prj_id", metavar="<id>")
        group2.add_argument("-m", "--prj_name", metavar="<name>")

        show_parser.add_argument("-s", "--share", action="store_true")
        show_parser.add_argument("-u", "--usage", action="store_true")
        show_parser.add_argument("-p", "--priority", action="store_true")
        show_parser.add_argument("-l", "--long", action="store_true")

    def execute(self, synergy_url, args):
        usr_id = getattr(args, 'id', None)
        usr_name = getattr(args, 'name', None)
        prj_id = getattr(args, 'prj_id', None)
        prj_name = getattr(args, 'prj_name', None)
        command = getattr(args, 'command', None)
        headers = ["name"]

        if command == "show":
            if args.long:
                headers.insert(0, "id")
            if args.share:
                headers.append("share")
            if args.usage:
                headers.append("usage")
            if args.priority:
                headers.append("priority")

        cmd_args = {"id": prj_id, "name": prj_name}
        result = super(UserCommand, self).execute(synergy_url,
                                                  "ProjectManager",
                                                  "GET_PROJECT",
                                                  args=cmd_args)

        if not result:
            print("project not found!")
            return

        self.printProject(result, headers, usr_id, usr_name)

    def printProject(self, project, headers, usr_id, usr_name):
        if not project:
            return

        table = []
        users = None

        if usr_id or usr_name:
            user = project.getUser(id=usr_id, name=usr_name)
            if not user:
                print("user not found!")
                return

            users = [user]
        else:
            users = project.getUsers()

        for user in users:
            row = []

            for attribute in headers:
                if attribute == "id":
                    row.append(user.getId())

                if attribute == "name":
                    row.append(user.getName())

                if attribute == "share":
                    share = user.getShare()
                    share_norm = share.getNormalizedValue()

                    row.append("{:.2%}".format(share_norm))

                if attribute == "priority":
                    row.append(user.getPriority().getValue())

                if attribute == "usage":
                    data = user.getData()

                    usage = "vcpus: {:.2%} | ram: {:.2%}".format(
                        data["actual_vcpus"], data["actual_memory"])

                    row.append(usage)

            table.append(row)

        print(tabulate(table, headers, tablefmt="fancy_grid"))
