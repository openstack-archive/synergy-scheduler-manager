import heapq
import json
import threading

from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError
from synergy.common.serializer import SynergyObject
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


class QueueItem(object):

    def __init__(self):
        self.id = -1
        self.priority = 0
        self.retry_count = 0
        self.user_id = None
        self.prj_id = None
        self.data = None
        self.creation_time = datetime.now()
        self.last_update = self.creation_time

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


class Queue(SynergyObject):

    def __init__(self, name="default", type="PRIORITY", db_engine=None):
        super(Queue, self).__init__()

        if type not in ["FIFO", "LIFO", "PRIORITY"]:
            raise SynergyError("queue type %r not supported" % type)

        self.set("type", type)
        self.set("is_closed", False)
        self.set("size", 0)
        self.setName(name)
        self.db_engine = db_engine
        self._items = []
        self.condition = threading.Condition()

        self._createTable()
        self._buildFromDB()

    def _incSize(self, value):
        size = self.get("size")
        self.set("size", size + value)

    def isOpen(self):
        return not self.get("is_closed")

    def isClosed(self):
        return self.get("is_closed")

    def setClosed(self, is_closed):
        self.set("is_closed", is_closed)

    def isEmpty(self):
        return len(self._items) == 0

    def close(self):
        self.setClosed(True)

    def enqueue(self, user, data):
        if self.isClosed():
            raise SynergyError("the queue is closed!")

        if not user:
            raise SynergyError("user not specified!")

        if not data:
            raise SynergyError("data not specified!")

        item = QueueItem()
        item.setUserId(user.getId())
        item.setProjectId(user.getProjectId())
        item.setPriority(user.getPriority().getValue())
        item.setData(data)

        with self.condition:
            if self.getType() == "FIFO":
                self._items.append(item)
            elif self.getType() == "LIFO":
                self._items.insert(0, item)
            elif self.getType() == "PRIORITY":
                heapq.heappush(self._items, (-item.getPriority(),
                                             item.getCreationTime(), item))

            self._insertItemDB(item)
            self._incSize(1)
            self.condition.notifyAll()

    def dequeue(self, block=True, timeout=None, delete=False):
        if self.isClosed():
            raise SynergyError("the queue is closed!")

        item = None

        with self.condition:
            while (item is None and not self.isClosed()):
                if not self._items:
                    if block:
                        self.condition.wait(timeout)
                        if timeout:
                            break
                    else:
                        break
                elif self.getType() == "PRIORITY":
                    item = heapq.heappop(self._items)[2]
                else:
                    item = self._items.pop(0)

            self.condition.notifyAll()

        if not item:
            return None

        self._getItemDataDB(item)

        if delete:
            self.delete(item)

        return item

    def restore(self, item):
        if self.isClosed():
            raise SynergyError("the queue is closed!")

        with self.condition:
            if self.getType() == "FIFO":
                self._items.append(item)
            elif self.getType() == "LIFO":
                self._items.insert(0, item)
            elif self.getType() == "PRIORITY":
                heapq.heappush(self._items, (-item.getPriority(),
                                             item.getCreationTime(), item))

            self._updateItemDB(item)
            self.condition.notifyAll()

    def updatePriority(self, user):
        if self.isClosed():
            raise SynergyError("the queue is closed!")

        if self.getType() != "PRIORITY":
            raise SynergyError("updatePriority() cannot be applied on this "
                               "queue type")

        new_items = []

        with self.condition:
            while self._items:
                item = heapq.heappop(self._items)[2]

                if item.getUserId() == user.getId() and\
                        item.getProjectId() == user.getProjectId():
                    item.setPriority(user.getPriority().getValue())
                new_items.append(item)

            for item in new_items:
                heapq.heappush(self._items, (-item.getPriority(),
                                             item.getCreationTime(), item))

            self.condition.notifyAll()

    def getType(self):
        return self.get("type")

    def getSize(self):
        return self.get("size")

    def getUsage(self, prj_id):
        result = 0
        connection = self.db_engine.connect()

        try:
            QUERY = "select count(*) from `%s` " % self.getName()
            QUERY += "where prj_id=%s"

            qresult = connection.execute(QUERY, [prj_id])
            row = qresult.fetchone()
            result = row[0]
        except SQLAlchemyError as ex:
            raise SynergyError(ex.message)
        finally:
            connection.close()
        return result

    def delete(self, item):
        if self.isClosed():
            raise SynergyError("the queue is closed!")

        if not item or not self.db_engine:
            return

        connection = self.db_engine.connect()
        trans = connection.begin()

        try:
            QUERY = "delete from `%s`" % self.getName()
            QUERY += " where id=%s"

            connection.execute(QUERY, [item.getId()])

            trans.commit()
            self._incSize(-1)
        except SQLAlchemyError as ex:
            trans.rollback()
            raise SynergyError(ex.message)
        finally:
            connection.close()

    def _createTable(self):
        if not self.db_engine:
            return

        TABLE = """CREATE TABLE IF NOT EXISTS `%s` (`id` BIGINT NOT NULL \
AUTO_INCREMENT PRIMARY KEY, `priority` INT DEFAULT 0, user_id CHAR(40) \
NOT NULL, prj_id CHAR(40) NOT NULL, `retry_count` INT DEFAULT 0, \
`creation_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, `last_update` \
TIMESTAMP NULL, `data` TEXT NOT NULL) ENGINE=InnoDB""" % self.getName()

        connection = self.db_engine.connect()

        try:
            connection.execute(TABLE)
        except SQLAlchemyError as ex:
            raise SynergyError(ex.message)
        except Exception as ex:
            raise SynergyError(ex.message)
        finally:
            connection.close()

    def _buildFromDB(self):
        if not self.db_engine:
            return

        connection = self.db_engine.connect()

        try:
            QUERY = "select id, user_id, prj_id, priority, retry_count, " \
                    "creation_time, last_update from `%s`" % self.getName()
            result = connection.execute(QUERY)

            for row in result:
                item = QueueItem()
                item.setId(row[0])
                item.setUserId(row[1])
                item.setProjectId(row[2])
                item.setPriority(row[3])
                item.setRetryCount(row[4])
                item.setCreationTime(row[5])
                item.setLastUpdate(row[6])

                self.restore(item)
                self._incSize(1)
        except SQLAlchemyError as ex:
            raise SynergyError(ex.message)
        finally:
            connection.close()

    def _insertItemDB(self, item):
        if not item or not self.db_engine:
            return

        QUERY = "insert into `%s` (user_id, prj_id, priority, " \
                "data) values" % self.getName()
        QUERY += "(%s, %s, %s, %s)"

        connection = self.db_engine.connect()
        trans = connection.begin()

        try:
            result = connection.execute(QUERY,
                                        [item.getUserId(),
                                         item.getProjectId(),
                                         item.getPriority(),
                                         json.dumps(item.getData())])

            idRecord = result.lastrowid

            trans.commit()

            item.setId(idRecord)
        except SQLAlchemyError as ex:
            trans.SynergyError()
            raise SynergyError(ex.message)
        finally:
            connection.close()

    def _getItemDataDB(self, item):
        if not item or not self.db_engine:
            return

        data = None
        connection = self.db_engine.connect()

        try:
            QUERY = "select data from `%s`" % self.getName()
            QUERY += " where id=%s"

            result = connection.execute(QUERY, [item.getId()])

            row = result.fetchone()

            data = json.loads(row[0])
        except SQLAlchemyError as ex:
            raise SynergyError(ex.message)
        finally:
            connection.close()

        item.setData(data)
        return data

    def _updateItemDB(self, item):
        if not item or not self.db_engine:
            return

        connection = self.db_engine.connect()
        trans = connection.begin()

        try:
            item.setLastUpdate(datetime.now())

            QUERY = "update `%s`" % self.getName()
            QUERY += " set priority=%s, retry_count=%s, " \
                     "last_update=%s where id=%s"

            connection.execute(QUERY, [item.getPriority(),
                                       item.getRetryCount(),
                                       item.getLastUpdate(),
                                       item.getId()])

            trans.commit()
        except SQLAlchemyError as ex:
            trans.rollback()

            raise SynergyError(ex.message)
        finally:
            connection.close()
