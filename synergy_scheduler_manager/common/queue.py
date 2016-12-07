import heapq
import json
import threading

from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError
from synergy.common.serializer import SynergyObject


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
        self._heap = []

    def __len__(self):
        return len(self._heap)

    def __iter__(self):
        """Get all elements ordered by asc. priority. """
        return self

    def put(self, priority, item):
        heapq.heappush(self._heap, (-priority, item.getCreationTime(), item))

    def get(self):
        return heapq.heappop(self._heap)[2]

    def size(self):
        return len(self._heap)

    def items(self):
        return [heapq.heappop(self._heap)[2] for i in range(len(self._heap))]

    def smallest(self, x):
        result = heapq.nsmallest(x, self._heap, key=lambda s: -s[0])
        return [item[2] for item in result]

    def largest(self, x):
        result = heapq.nlargest(x, self._heap, key=lambda s: -s[0])
        return [item[2] for item in result]


class Queue(SynergyObject):

    def __init__(self):
        super(Queue, self).__init__()

        self.setName("N/A")
        self.set("is_closed", False)
        self.set("size", 0)

    def isOpen(self):
        return not self.get("is_closed")

    def isClosed(self):
        return self.get("is_closed")

    def setClosed(self, is_closed):
        self.set("is_closed", is_closed)

    def getSize(self):
        return self.get("size")

    def setSize(self, size):
        self.set("size", size)


class QueueDB(Queue):

    def __init__(self, name, db_engine, fairshare_manager=None):
        super(QueueDB, self).__init__()
        self.setName(name)

        self.db_engine = db_engine
        self.fairshare_manager = fairshare_manager
        self.priority_updater = None
        self.condition = threading.Condition()
        self.pqueue = PriorityQueue()
        self.createTable()
        self.buildFromDB()
        self.updatePriority()

    def getSize(self):
        connection = self.db_engine.connect()

        try:
            QUERY = "select count(*) from `%s`" % self.getName()
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
TIMESTAMP NULL, `data` TEXT NOT NULL ) ENGINE=InnoDB""" % self.getName()

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
        if not self.isClosed():
            self.setClosed(True)

            with self.condition:
                self.condition.notifyAll()

    def buildFromDB(self):
        connection = self.db_engine.connect()

        try:
            QUERY = "select id, user_id, prj_id, priority, retry_count, " \
                    "creation_time, last_update from `%s`" % self.getName()
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
                    "data) values" % self.getName()
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

    def getItem(self, blocking=True):
        item = None
        queue_item = None

        with self.condition:
            while (queue_item is None and not self.isClosed()):
                if len(self.pqueue):
                    queue_item = self.pqueue.get()
                elif blocking:
                    self.condition.wait()
                elif queue_item is None:
                    break

            if (not self.isClosed() and queue_item is not None):
                connection = self.db_engine.connect()

                try:
                    QUERY = """select user_id, prj_id, priority, \
retry_count, creation_time, last_update, data from `%s`""" % self.getName()
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
                QUERY = "delete from `%s`" % self.getName()
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

                QUERY = "update `%s`" % self.getName()
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
            return

        queue_items = []

        with self.condition:
            while len(self.pqueue) > 0:
                queue_item = self.pqueue.get()
                priority = queue_item.getPriority()

                try:
                    priority = self.fairshare_manager.calculatePriority(
                        user_id=queue_item.getUserId(),
                        prj_id=queue_item.getProjectId(),
                        timestamp=queue_item.getCreationTime(),
                        retry=queue_item.getRetryCount())

                    queue_item.setPriority(priority)
                except Exception:
                    continue
                finally:
                    queue_items.append(queue_item)

            if len(queue_items) > 0:
                for queue_item in queue_items:
                    self.pqueue.put(queue_item.getPriority(), queue_item)

                del queue_items

            self.condition.notifyAll()

    def serialize(self):
        queue = Queue()
        queue.setName(self.getName())
        queue.setSize(self.getSize())
        queue.setClosed(self.isClosed())

        return queue.serialize()
