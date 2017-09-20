import logging

from common.project import Project
from oslo_config import cfg
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
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


class ProjectManager(Manager):

    def __init__(self):
        super(ProjectManager, self).__init__(name="ProjectManager")

        self.config_opts = [
            cfg.IntOpt("default_TTL", default=1440, required=False),
            cfg.FloatOpt("default_share", default=10.0, required=False),
            cfg.StrOpt("db_connection", help="the DB url", required=True),
            cfg.IntOpt('db_pool_size', default=10, required=False),
            cfg.IntOpt('db_pool_recycle', default=30, required=False),
            cfg.IntOpt('db_max_overflow', default=5, required=False)
        ]

        self.projects = {}

    def setup(self):
        self.default_TTL = CONF.ProjectManager.default_TTL
        self.default_share = CONF.ProjectManager.default_share

        db_connection = CONF.ProjectManager.db_connection
        pool_size = CONF.ProjectManager.db_pool_size
        pool_recycle = CONF.ProjectManager.db_pool_recycle
        max_overflow = CONF.ProjectManager.db_max_overflow

        try:
            self.db_engine = create_engine(db_connection,
                                           pool_size=pool_size,
                                           pool_recycle=pool_recycle,
                                           max_overflow=max_overflow)
        except SQLAlchemyError as ex:
            LOG.error(ex)
            raise ex

        self.configured = False
        self.keystone_manager = self.getManager("KeystoneManager")
        self.createTable()

    def task(self):
        if not self.configured:
            try:
                self.buildFromDB()
                self.configured = True
            except Exception as ex:
                LOG.error(ex)

    def destroy(self):
        pass

    def execute(self, command, *args, **kargs):
        if command == "GET_PROJECTS":
            return self.projects.values()

        prj_id = kargs.get("id", None)
        prj_name = kargs.get("name", None)

        project = self.getProject(prj_id, prj_name)

        if command == "GET_PROJECT":
            if project:
                return project
            else:
                raise SynergyError("project not found!")

        elif command == "ADD_PROJECT":
            if project:
                raise SynergyError("the project id=%s name=%s already exists!"
                                   % (project.getId(), project.getName()))

            TTL = kargs.get("TTL", None)
            share = kargs.get("share", None)

            return self._addProject(prj_id, prj_name, TTL, share)

        elif command == "UPDATE_PROJECT":
            if not project:
                raise SynergyError("project not found!")

            TTL = kargs.get("TTL", None)
            share = kargs.get("share", None)

            return self._updateProject(project, TTL, share)

        elif command == "REMOVE_PROJECT":
            if not project:
                raise SynergyError("project not found!")

            self._removeProject(project)
        else:
            raise SynergyError("command %r not supported!" % command)

    def doOnEvent(self, event_type, *args, **kwargs):
        if event_type == "identity.role_assignment.created":
            usr_id = kwargs.get("user", None)
            prj_id = kwargs.get("project", None)
            project = self.getProject(id=prj_id)

            if project and not project.getUser(id=usr_id):
                user = self.keystone_manager.getUser(usr_id)
                if user:
                    project.addUser(user)

                    self.notify(event_type="USER_ADDED", user=user)

        elif event_type == "identity.role_assignment.deleted":
            usr_id = kwargs.get("user", None)
            prj_id = kwargs.get("project", None)
            project = self.getProject(id=prj_id)

            if project:
                user = project.removeUser(usr_id)
                if user:
                    self.notify(event_type="USER_REMOVED", user=user)

        elif event_type == "identity.user.deleted":
            user_id = kwargs.get("resource_info", None)

            for project in self.projects.values():
                try:
                    user = project.getUser(id=user_id)
                    if user:
                        project.removeUser(user_id)

                    self.notify(event_type="USER_DELETED", user=user)
                except SynergyError as ex:
                    LOG.info(ex)

        elif event_type == "identity.project.deleted":
            LOG.info(kwargs)
            prj_id = kwargs.get("resource_info", None)
            project = self.getProject(id=prj_id, name=prj_id)

            if project:
                self._removeProject(project)

    def _parseNumber(self, value, default=None):
        if not value:
            return default

        try:
            return int(value)
        except SynergyError:
            if default:
                return default
        raise SynergyError("%r is not a number!" % str(value))

    def _addProject(self, prj_id, prj_name, TTL, share):
        project = None

        if prj_id:
            try:
                project = self.keystone_manager.getProject(prj_id)
            except SynergyError:
                raise SynergyError("project not found in Keystone!")
        elif prj_name:
            projects = self.keystone_manager.getProjects(name=prj_name)

            if len(projects) > 1:
                raise SynergyError("ambiguity: found %s projects having %r"
                                   " as name" % (len(projects), prj_name))
            if projects:
                project = projects[0]
        else:
            raise SynergyError("missing project attributes")

        if not project:
            raise SynergyError("project not found in Keystone!")

        prj_TTL = self._parseNumber(TTL, default=self.default_TTL)
        prj_share = self._parseNumber(share, 0)

        project.setTTL(prj_TTL)
        project.getShare().setValue(prj_share)

        QUERY = "insert into project (id, name, share, TTL) " \
                "values (%s, %s, %s, %s)"

        connection = self.db_engine.connect()
        trans = connection.begin()

        try:
            connection.execute(
                QUERY, [project.getId(), project.getName(),
                        project.getShare().getValue(), project.getTTL()])

            trans.commit()
        except SQLAlchemyError as ex:
            trans.rollback()

            if "Duplicate entry" in ex.message:
                raise SynergyError("the project id=%s name=%s already exists!"
                                   % (project.getId(), project.getName()))
            else:
                raise(ex)
        finally:
            connection.close()

        users = self.keystone_manager.getUsers(prj_id=project.getId())

        for user in users:
            project.addUser(user)

        self.projects[project.getId()] = project

        LOG.info("added project %r" % project.getName())
        self.notify(event_type="PROJECT_ADDED", project=project)

        return project

    def _updateProject(self, project, TTL, share):
        if not project:
            return

        TTL = self._parseNumber(TTL)
        if TTL:
            TTL = self._parseNumber(TTL)
            if TTL <= 0:
                raise SynergyError("wrong TTL value: %s <= 0" % TTL)
            project.setTTL(TTL)

        share = self._parseNumber(share)
        if share:
            if share <= 0:
                raise SynergyError("wrong share value: %s <= 0" % share)
            project.getShare().setValue(share)

        connection = self.db_engine.connect()
        trans = connection.begin()

        try:
            QUERY = "update project set share=%s, TTL=%s where id=%s"

            connection.execute(QUERY, [project.getShare().getValue(),
                                       project.getTTL(),
                                       project.getId()])

            trans.commit()
        except SQLAlchemyError as ex:
            trans.rollback()

            raise SynergyError(ex.message)
        finally:
            connection.close()

        LOG.info("updated project %r" % project.getName())
        self.notify(event_type="PROJECT_UPDATED", project=project)

    def _removeProject(self, project, force=False):
        if not force:
            if project.getId() not in self.projects.keys():
                raise SynergyError("project %s not found!" % project.getId())
            self.projects.pop(project.getId())

        connection = self.db_engine.connect()
        trans = connection.begin()

        try:
            QUERY = "delete from project where id=%s"

            connection.execute(QUERY, [project.getId()])

            trans.commit()
        except SQLAlchemyError as ex:
            trans.rollback()

            raise SynergyError(ex.message)
        finally:
            connection.close()

        LOG.info("removed project %r" % project.getName())
        self.notify(event_type="PROJECT_REMOVED", project=project)

    def getProject(self, id=None, name=None):
        if not id and not name:
            raise SynergyError("please define the project id or its name!")

        project = None

        if id:
            project = self.projects.get(id, None)
        elif name:
            for prj in self.projects.values():
                if name == prj.getName():
                    project = prj
                    break

        return project

    def getProjects(self):
        return self.projects.values()

    def createTable(self):
        TABLE = """CREATE TABLE IF NOT EXISTS project (`id` VARCHAR(64) \
NOT NULL PRIMARY KEY, name VARCHAR(64), share INT DEFAULT 0, TTL INT DEFAULT \
1440) ENGINE=InnoDB"""

        connection = self.db_engine.connect()

        try:
            connection.execute(TABLE)
        except SQLAlchemyError as ex:
            raise SynergyError(ex.message)
        except Exception as ex:
            raise SynergyError(ex.message)
        finally:
            connection.close()

    def buildFromDB(self):
        connection = self.db_engine.connect()

        try:
            QUERY = "select id, name, share, TTL from project"
            result = connection.execute(QUERY)

            for row in result:
                project = Project()
                project.setId(row[0])
                project.setName(row[1])
                project.getShare().setValue(row[2])
                project.setTTL(row[3])
                project.setId(row[0])
                project_id = project.getId()
                try:
                    k_project = self.keystone_manager.getProject(project_id)

                    if not k_project:
                        self._removeProject(project)
                        continue

                    users = self.keystone_manager.getUsers(prj_id=project_id)

                    for user in users:
                        project.addUser(user)

                    self.projects[project.getId()] = project

                    self.notify(event_type="PROJECT_ADDED", project=project)
                except SynergyError as ex:
                    LOG.info("the project %s seems not to exist anymore! "
                             "(reason=%s)" % (project.getName(), ex.message))
                    try:
                        self._removeProject(project, force=True)
                    except Exception as ex:
                        LOG.info(ex)
        except SQLAlchemyError as ex:
            raise SynergyError(ex.message)
        finally:
            connection.close()
            self.notify(event_type="PROJECT_DONE")
