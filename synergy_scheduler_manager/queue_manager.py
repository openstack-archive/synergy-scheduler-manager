import logging

from common.queue import QueueDB
from oslo_config import cfg
from sqlalchemy import create_engine
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


class QueueManager(Manager):

    def __init__(self):
        super(QueueManager, self).__init__(name="QueueManager")

        self.config_opts = [
            cfg.StrOpt("db_connection", help="the DB url", required=True),
            cfg.IntOpt('db_pool_size', default=10, required=False),
            cfg.IntOpt('db_pool_recycle', default=30, required=False),
            cfg.IntOpt('db_max_overflow', default=5, required=False)
        ]
        self.queue_list = {}

    def setup(self):
        if self.getManager("FairShareManager") is None:
            raise Exception("FairShareManager not found!")

        self.fairshare_manager = self.getManager("FairShareManager")

        if self.fairshare_manager is None:
            raise Exception("FairShareManager not found!")

        db_connection = CONF.QueueManager.db_connection
        pool_size = CONF.QueueManager.db_pool_size
        pool_recycle = CONF.QueueManager.db_pool_recycle
        max_overflow = CONF.QueueManager.db_max_overflow

        try:
            self.db_engine = create_engine(db_connection,
                                           pool_size=pool_size,
                                           pool_recycle=pool_recycle,
                                           max_overflow=max_overflow)
        except Exception as ex:
            LOG.error(ex)
            raise ex

    def execute(self, command, *args, **kargs):
        if command == "CREATE_QUEUE":
            return self.createQueue(*args, **kargs)
        elif command == "DELETE_QUEUE":
            return self.deleteQueue(*args, **kargs)
        elif command == "GET_QUEUE":
            queue = self.getQueue(kargs.get("name", None))
            return queue

        else:
            raise Exception("command=%r not supported!" % command)

    def task(self):
        try:
            for queue in self.queue_list.values():
                queue.updatePriority()
        except Exception as ex:
            LOG.error("Exception has occured", exc_info=1)
            LOG.error(ex)

    def destroy(self):
        for queue in self.queue_list.values():
            queue.close()

    def createQueue(self, name):
        if name not in self.queue_list:
            queue = QueueDB(name, self.db_engine, self.fairshare_manager)
            self.queue_list[name] = queue
            return queue
        else:
            raise Exception("the queue %r already exists!" % name)

    def deleteQueue(self, name):
        if name not in self.queue_list:
            raise Exception("queue %r not found!" % name)

        del self.queue_list[name]

    def getQueue(self, name):
        if name not in self.queue_list:
            raise Exception("queue %r not found!" % name)

        return self.queue_list[name]
