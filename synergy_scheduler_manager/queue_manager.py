import heapq
import json
import logging
import threading

from datetime import datetime

try:
    from oslo_config import cfg
except ImportError:
    from oslo.config import cfg

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

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


class QueueItem(object):

    def __init__(self, id, user_id, prj_id, priority,
                 retry_count, creation_time, last_update, data=None):
        self.id = id
        self.user_id = user_id
        self.prj_id = prj_id
        self.priority = priority
        self.retry_count = retry_count
        self.creation_time = creation_time
        self.last_update = last_update
        self.data = data

    def getId(self):
        return self.id

    def setId(self, id):
        self.id = id

    def getUserId(self):
        return self.user_id

    def setUserId(self, user_id):
        self.user_id = user_id

    def getProjectId(self):
        return self.prj_id

    def setProjectId(self, prj_id):
        self.prj_id = prj_id

    def getPriority(self):
        return self.priority

    def setPriority(self, priority):
        self.priority = priority

    def getRetryCount(self):
        return self.retry_count

    def setRetryCount(self, retry_count):
        self.retry_count = retry_count

    def incRetryCount(self):
        self.retry_count += 1

    def getCreationTime(self):
        return self.creation_time

    def setCreationTime(self, creation_time):
        self.creation_time = creation_time

    def getLastUpdate(self):
        return self.last_update

    def setLastUpdate(self, last_update):
        self.last_update = last_update

    def getData(self):
        return self.data

    def setData(self, data):
        self.data = data


class PriorityQueue(object):

    def __init__(self):
        self.queue = []
        self._index = 0

    def put(self, priority, item):
        heapq.heappush(self.queue, (-priority, self._index, item))
        self._index += 1

    def get(self):
        return heapq.heappop(self.queue)[-1]

    def size(self):
        return len(self.queue)


class Queue(object):

    def __init__(self, name, db_engine, fairshare_manager=None):
        self.name = name
        self.db_engine = db_engine
        self.fairshare_manager = fairshare_manager
        self.is_closed = False
        self.priority_updater = None
        self.condition = threading.Condition()
        self.pqueue = PriorityQueue()
        self.createTable()
        self.buildFromDB()

    def getName(self):
        return self.name

    def getSize(self):
        connection = self.db_engine.connect()

        try:
            QUERY = "select count(*) from `%s`" % self.name
            result = connection.execute(QUERY)

            row = result.fetchone()

            return row[0]
        except SQLAlchemyError as ex:
            raise Exception(ex.message)
        finally:
            connection.close()

    def createTable(self):
        TABLE = """CREATE TABLE IF NOT EXISTS `%s` (`id` BIGINT NOT NULL \
AUTO_INCREMENT PRIMARY KEY, `priority` INT DEFAULT 0, user_id CHAR(40) \
NOT NULL, prj_id CHAR(40) NOT NULL, `retry_count` INT DEFAULT 0, \
`creation_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, `last_update` \
TIMESTAMP NULL, `data` TEXT NOT NULL ) ENGINE=InnoDB""" % self.name

        connection = self.db_engine.connect()

        try:
            connection.execute(TABLE)
        except SQLAlchemyError as ex:
            raise Exception(ex.message)
        except Exception as ex:
            raise Exception(ex.message)
        finally:
            connection.close()

    def close(self):
        if not self.is_closed:
            self.is_closed = True

            with self.condition:
                self.condition.notifyAll()

    def isClosed(self):
        return self.is_closed

    def buildFromDB(self):
        connection = self.db_engine.connect()

        try:
            QUERY = "select id, user_id, prj_id, priority, retry_count, " \
                    "creation_time, last_update from `%s`" % self.name
            result = connection.execute(QUERY)

            for row in result:
                queue_item = QueueItem(row[0], row[1], row[2],
                                       row[3], row[4], row[5], row[6])

                self.pqueue.put(row[3], queue_item)
        except SQLAlchemyError as ex:
            raise Exception(ex.message)
        finally:
            connection.close()

        with self.condition:
            self.condition.notifyAll()

    def insertItem(self, user_id, prj_id, priority, data):
        with self.condition:
            idRecord = -1
            QUERY = "insert into `%s` (user_id, prj_id, priority, " \
                    "data) values" % self.name
            QUERY += "(%s, %s, %s, %s)"

            connection = self.db_engine.connect()
            trans = connection.begin()

            try:
                result = connection.execute(QUERY,
                                            [user_id, prj_id, priority,
                                             json.dumps(data)])

                idRecord = result.lastrowid

                trans.commit()
            except SQLAlchemyError as ex:
                trans.rollback()
                raise Exception(ex.message)
            finally:
                connection.close()

            now = datetime.now()
            queue_item = QueueItem(idRecord, user_id, prj_id,
                                   priority, 0, now, now)

            self.pqueue.put(priority, queue_item)

            self.condition.notifyAll()

    def reinsertItem(self, queue_item):
        with self.condition:
            self.pqueue.put(queue_item.getPriority(), queue_item)
            self.condition.notifyAll()

    def getItem(self):
        item = None
        queue_item = None

        with self.condition:
            while (queue_item is None and not self.is_closed):
                if self.pqueue.size() > 0:
                    queue_item = self.pqueue.get()

                    # self.pqueue.task_done()
                else:
                    self.condition.wait()

            if (not self.is_closed and queue_item is not None):
                connection = self.db_engine.connect()

                try:
                    QUERY = """select user_id, prj_id, priority, \
retry_count, creation_time, last_update, data from `%s`""" % self.name
                    QUERY += " where id=%s"

                    result = connection.execute(QUERY, [queue_item.getId()])

                    row = result.fetchone()

                    item = QueueItem(queue_item.getId(), row[0], row[1],
                                     row[2], row[3], row[4], row[5],
                                     json.loads(row[6]))
                except SQLAlchemyError as ex:
                    raise Exception(ex.message)
                finally:
                    connection.close()

            self.condition.notifyAll()
        return item

    def deleteItem(self, queue_item):
        if not queue_item:
            return

        with self.condition:
            connection = self.db_engine.connect()
            trans = connection.begin()

            try:
                QUERY = "delete from `%s`" % self.name
                QUERY += " where id=%s"

                connection.execute(QUERY, [queue_item.getId()])

                trans.commit()
            except SQLAlchemyError as ex:
                trans.rollback()

                raise Exception(ex.message)
            finally:
                connection.close()
            self.condition.notifyAll()

    def updateItem(self, queue_item):
        if not queue_item:
            return

        with self.condition:
            connection = self.db_engine.connect()
            trans = connection.begin()

            try:
                queue_item.setLastUpdate(datetime.now())

                QUERY = "update `%s`" % self.name
                QUERY += " set priority=%s, retry_count=%s, " \
                         "last_update=%s where id=%s"

                connection.execute(QUERY, [queue_item.getPriority(),
                                           queue_item.getRetryCount(),
                                           queue_item.getLastUpdate(),
                                           queue_item.getId()])

                trans.commit()
            except SQLAlchemyError as ex:
                trans.rollback()

                raise Exception(ex.message)
            finally:
                connection.close()

            self.pqueue.put(queue_item.getPriority(), queue_item)
            self.condition.notifyAll()

    def updatePriority(self):
        if self.fairshare_manager is None:
            # LOG.warn("priority_updater not found!!!")
            return

        with self.condition:
            # now = datetime.now()
            queue_items = []

            connection = self.db_engine.connect()

            while self.pqueue.size() > 0:
                queue_item = self.pqueue.get()
                priority = queue_item.getPriority()

                try:
                    priority = self.fairshare_manager.execute(
                        "CALCULATE_PRIORITY",
                        user_id=queue_item.getUserId(),
                        prj_id=queue_item.getProjectId(),
                        timestamp=queue_item.getCreationTime(),
                        retry=queue_item.getRetryCount())

                    queue_item.setPriority(priority)
                except Exception as ex:
                    continue
                finally:
                    queue_items.append(queue_item)

                trans = connection.begin()

                try:
                    queue_item.setLastUpdate(datetime.now())

                    QUERY = "update `%s`" % self.name
                    QUERY += " set priority=%s, last_update=%s where id=%s"

                    connection.execute(QUERY, [queue_item.getPriority(),
                                               queue_item.getLastUpdate(),
                                               queue_item.getId()])

                    trans.commit()
                except SQLAlchemyError as ex:
                    trans.rollback()
                    raise Exception(ex.message)

            connection.close()

            if len(queue_items) > 0:
                for queue_item in queue_items:
                    self.pqueue.put(queue_item.getPriority(), queue_item)

                del queue_items

            self.condition.notifyAll()

    def toDict(self):
        queue = {}
        queue["name"] = self.name
        queue["size"] = self.getSize()
        # queue["size"] = self.pqueue.size()

        if self.is_closed:
            queue["status"] = "OFF"
        else:
            queue["status"] = "ON"

        return queue


class QueueManager(Manager):

    def __init__(self):
        super(QueueManager, self).__init__(name="QueueManager")

        self.config_opts = [
            cfg.StrOpt("db_connection", help="the DB url", required=True),
            cfg.IntOpt('db_pool_size', default=10, required=False),
            cfg.IntOpt('db_max_overflow', default=5, required=False)
        ]

    def setup(self):
        if self.getManager("FairShareManager") is None:
            raise Exception("FairShareManager not found!")

        self.fairshare_manager = self.getManager("FairShareManager")

        self.queue_list = {}
        db_connection = CONF.QueueManager.db_connection
        pool_size = CONF.QueueManager.db_pool_size
        max_overflow = CONF.QueueManager.db_max_overflow

        try:
            self.db_engine = create_engine(db_connection,
                                           pool_size=pool_size,
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
            return self.getQueue(*args, **kargs)
        else:
            raise Exception("command=%r not supported!" % command)

    def task(self):
        for queue in self.queue_list.values():
            queue.updatePriority()

    def destroy(self):
        for queue in self.queue_list.values():
            queue.close()

    def createQueue(self, name):
        if name not in self.queue_list:
            queue = Queue(name, self.db_engine, self.fairshare_manager)
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
