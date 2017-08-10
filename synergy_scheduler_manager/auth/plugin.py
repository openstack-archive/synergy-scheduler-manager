import logging
import sys

from oslo_config import cfg
from oslo_context.context import RequestContext
from oslo_policy import generator
from oslo_policy import policy
from synergy.exception import AuthorizationError


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

ENFORCER = None
CONF = cfg.CONF
LOG = logging.getLogger(__name__)


class KeystoneAuthorization(object):

    def __init__(self):
        super(KeystoneAuthorization, self).__init__()
        rules = [policy.RuleDefault('admin', 'role:admin or is_admin:True'),
                 policy.RuleDefault('admin_or_owner',
                                    'rule:admin or project_id:%(id)s or '
                                    'project_name:%(name)s'),
                 policy.RuleDefault('cloud_admin',
                                    'rule:admin and project_name:admin'),
                 policy.RuleDefault('default', 'rule:admin'),
                 policy.RuleDefault('synergy:EXECUTE', 'rule:cloud_admin'),
                 policy.RuleDefault('synergy:LIST', 'rule:admin'),
                 policy.RuleDefault('synergy:START', 'rule:admin'),
                 policy.RuleDefault('synergy:STOP', 'rule:admin'),
                 policy.RuleDefault('synergy:STATUS', 'rule:admin'),
                 policy.RuleDefault('ProjectManager:GET_PROJECTS',
                                    'rule:cloud_admin'),
                 policy.RuleDefault('ProjectManager:GET_PROJECT',
                                    'rule:admin_or_owner'),
                 policy.RuleDefault('ProjectManager:ADD_PROJECT',
                                    'rule:admin'),
                 policy.RuleDefault('ProjectManager:REMOVE_PROJECT',
                                    'rule:admin'),
                 policy.RuleDefault('ProjectManager:UPDATE_PROJECT',
                                    'rule:admin')]

        global ENFORCER
        policy_file = CONF.Authorization.policy_file

        if not ENFORCER:
            ENFORCER = policy.Enforcer(CONF, policy_file=policy_file)
            ENFORCER.register_defaults(rules)
            ENFORCER.load_rules(True)

            self.storePolicies(ENFORCER, policy_file)

    def storePolicies(self, enforcer, output_file):
        output_file = (open(output_file, 'w') if output_file else sys.stdout)
        rules = {}
        rules.update(enforcer.registered_rules)
        rules.update(enforcer.file_rules)

        output_file.write("{\n")

        for rule in sorted(rules.keys(), key=lambda v: v.upper()):
            section = generator._format_rule_default_yaml(rules[rule],
                                                          include_help=False)
            output_file.write("    ")
            output_file.write(section.replace('\n', ',\n'))

        output_file.write("}")
        output_file.close()

    def authorize(self, context):
        managers = context.get("managers", None)
        manager = context.get("manager", None)
        manager_args = context.get("args", {})
        command = context.get("command", None)

        action = context.get("PATH_INFO", None)
        token_id = context.get("HTTP_X_AUTH_TOKEN", None)

        if not managers:
            raise AuthorizationError("missing managers!")

        keystone_manager = managers.get("KeystoneManager", None)

        if not managers:
            raise AuthorizationError("missing KeystoneManager!")

        if not action:
            raise AuthorizationError("missing PATH_INFO!")

        if action.startswith("/synergy/"):
            action = "synergy:%s" % action[9:].upper()
            manager = context.get("manager", None)
            command = context.get("command", None)

            if action == "synergy:EXECUTE" and manager in managers:
                action = "%s:%s" % (manager, command)

        if not token_id:
            raise AuthorizationError("missing HTTP_X_AUTH_TOKEN!")

        try:
            token = keystone_manager.validateToken(token_id)
        except Exception as ex:
            LOG.info(ex.message)
            raise AuthorizationError(ex.message)

        project_id = token.getProject().getId()
        project_name = token.getProject().getName()
        roles = [role.getName() for role in token.getRoles()]

        requestContext = RequestContext(auth_token=token.getId(),
                                        user=token.getUser().getId(),
                                        user_name=token.getUser().getName(),
                                        tenant=token.getProject().getId(),
                                        project_name=project_name,
                                        is_admin=token.isAdmin(),
                                        roles=roles)

        try:
            target = requestContext.to_dict()
            target["project_id"] = project_id
            target["project_name"] = project_name
            target["manager"] = manager
            target["command"] = command
            target["roles"] = roles
            target.update(manager_args)

            result = ENFORCER.enforce(action, target, target,
                                      do_raise=True, exc=AuthorizationError)
        except policy.PolicyNotRegistered as ex:
            LOG.info(ex)
            raise AuthorizationError(ex.message)
        except AuthorizationError:
            raise AuthorizationError("You are not authorized!")
        except Exception as ex:
            LOG.info(ex)
            raise AuthorizationError(ex.message)

        return result
