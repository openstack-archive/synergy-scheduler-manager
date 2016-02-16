# Copyright (c) 2014 INFN - "Istituto Nazionale di Fisica Nucleare" - Italy
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

__author__ = "Lisa Zangrando"
__email__ = "lisa.zangrando[AT]pd.infn.it"

import heapq
#from Queue import PriorityQueue
from DBUtils.PooledDB import PooledDB
import MySQLdb
import json
import mysql.connector
import threading
import datetime

from mysql.connector import errorcode
try:
    from oslo_config import cfg
except ImportError:
    from oslo.config import cfg


from synergy.common import manager
from synergy.common import rpc

from synergy.openstack.common import log as logging

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class QueueItem():

    def __init__(self, id, user_id, project_id, priority,
                 retry_count, creation_time, last_update, data=None):
        self.id = id
        self.user_id = user_id
        self.project_id = project_id
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
        return self.project_id

    def setProjectId(self, project_id):
        self.project_id = project_id

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
        self.data


class PriorityQueue:

    def __init__(self):
        self.queue = []
        self._index = 0

    def put(self, priority, item):

        LOG.info(">>>>>>>>> PriorityQueue: priority=%s" % priority)
        heapq.heappush(self.queue, (-priority, self._index, item))
        self._index += 1

    def get(self):
        return heapq.heappop(self.queue)[-1]

    def qsize(self):
        return len(self.queue)


class Queue():

    def __init__(self, name, pool):
        self.name = name
        self.pool = pool
        self.is_closed = False
        self.priority_updater = None
        self.condition = threading.Condition()
        self.pqueue = PriorityQueue()
        self.createTable()
        self.buildFromDB()

    def getName(self):
        return self.name

    def getSize(self):
        return self.pqueue.qsize()

    def createTable(self):
        TABLE = "CREATE TABLE IF NOT EXISTS `%s` (`id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, `priority` INT DEFAULT 0, user_id CHAR(40) NOT NULL, project_id CHAR(40) NOT NULL, `retry_count` INT DEFAULT 0, `creation_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, `last_update` TIMESTAMP NULL, `data` TEXT NOT NULL ) ENGINE=InnoDB" % self.name

        try:
            dbConnection = self.pool.connection()
            cursor = dbConnection.cursor()
            cursor.execute(TABLE)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                LOG.info("already exists.")
            else:
                LOG.error(err.msg)
                raise err
        finally:
            try:
                cursor.close()
                dbConnection.close()
            except MySQLdb.Error as ex:
                pass

    def close(self):
        if not self.is_closed:
            LOG.info("closing queue '%s'" % self.name)
            self.is_closed = True

            with self.condition:
                self.condition.notifyAll()

            LOG.info("queue '%s' closed" % self.name)
        else:
            LOG.info("queue '%s' already closed" % self.name)

    def isClosed(self):
        return self.is_closed

    def buildFromDB(self):
        try:
            dbConnection = self.pool.connection()

            QUERY = "select id, user_id, project_id, priority, retry_count, creation_time, last_update from `%s`" % self.name
            cursor = dbConnection.cursor()
            cursor.execute(QUERY)

            #QUERY = "select id, user_id, project_id, priority, retry_count, creation_time, last_update from `%(name)s`"
            #cursor = dbConnection.cursor()
            #cursor.execute(QUERY, { "name": self.name })

            for row in cursor.fetchall():
                queue_item = QueueItem(
                    row[0], row[1], row[2], row[3], row[4], row[5], row[6])

                self.pqueue.put(row[3], queue_item)
        except MySQLdb.Error as ex:
            LOG.error(ex)
            if dbConnection:
                dbConnection.rollback()
            raise ex
        except Exception as ex:
            LOG.error(ex)
            raise ex
        finally:
            try:
                cursor.close()
                dbConnection.close()
            except MySQLdb.Error as ex:
                pass

        with self.condition:
            self.condition.notifyAll()

    def insertItem(self, userId, projectId, priority, data):
        with self.condition:
            idRecord = -1
            cursor = None

            try:
                QUERY = "insert into `%s` (user_id, project_id, priority, data) values" % self.name
                QUERY += "(%s, %s, %s, %s)"

                dbConnection = self.pool.connection()

                cursor = dbConnection.cursor()
                cursor.execute(
                    QUERY, [
                        userId, projectId, priority, json.dumps(data)])

                idRecord = cursor.lastrowid

                dbConnection.commit()
            except MySQLdb.Error as ex:
                LOG.error(ex)
                if dbConnection:
                    dbConnection.rollback()
                raise ex
            except Exception as ex:
                LOG.error(ex)
                raise ex
            finally:
                try:
                    cursor.close()
                    dbConnection.close()
                except MySQLdb.Error as ex:
                    pass

            now = datetime.datetime.now()
            queue_item = QueueItem(
                idRecord, userId, projectId, priority, 0, now, now)

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
                if self.pqueue.qsize() > 0:
                    #priority, queue_item = self.pqueue.get()
                    queue_item = self.pqueue.get()

                    # self.pqueue.task_done()
                else:
                    LOG.info("the queue '%s' is empty: waiting..." % self.name)
                    self.condition.wait()

            if (not self.is_closed and queue_item is not None):
                try:
                    QUERY = "select user_id, project_id, priority, retry_count, creation_time, last_update, data from `%s`" % self.name
                    QUERY += " where id=%s"

                    dbConnection = self.pool.connection()

                    cursor = dbConnection.cursor()
                    cursor.execute(QUERY, [queue_item.getId()])

                    row = cursor.fetchone()

                    item = QueueItem(
                        queue_item.getId(),
                        row[0],
                        row[1],
                        row[2],
                        row[3],
                        row[4],
                        row[5],
                        json.loads(
                            row[6]))
                except Exception as ex:
                    LOG.error(ex)
                    raise ex
                finally:
                    try:
                        cursor.close()
                        dbConnection.close()
                    except MySQLdb.Error as ex:
                        pass

            self.condition.notifyAll()

        return item

    def deleteItem(self, queue_item):
        if not queue_item:
            return

        with self.condition:
            try:
                QUERY = "delete from `%s`" % self.name
                QUERY += " where id=%s"

                dbConnection = self.pool.connection()

                cursor = dbConnection.cursor()
                cursor.execute(QUERY, [queue_item.getId()])

                dbConnection.commit()
                """
                if item["id"] in self.fetchedItems:
                    del self.fetchedItems[item["id"]]
                """

            except MySQLdb.Error as ex:
                LOG.error(ex)
                if dbConnection:
                    dbConnection.rollback()
                raise ex
            except Exception as ex:
                LOG.error(ex)
                raise ex
            finally:
                try:
                    cursor.close()
                    dbConnection.close()
                except MySQLdb.Error as ex:
                    pass

            # self.pqueue.task_done()

            self.condition.notifyAll()

    def updateItem(self, queue_item):
        if not queue_item:
            return

        with self.condition:
            try:
                #queue_item.setRetryCount(queue_item.getRetryCount() + 1)
                queue_item.setLastUpdate(datetime.datetime.now())

                cursor = None

                QUERY = "update `%s`" % self.name
                QUERY += " set priority=%s, retry_count=%s, last_update=%s where id=%s"

                dbConnection = self.pool.connection()

                cursor = dbConnection.cursor()
                cursor.execute(QUERY,
                               [queue_item.getPriority(),
                                queue_item.getRetryCount(),
                                   queue_item.getLastUpdate(),
                                   queue_item.getId()])

                dbConnection.commit()

                # self.pqueue.task_done()
            except Exception as ex:
                LOG.error(ex)
                if dbConnection:
                    dbConnection.rollback()
            finally:
                try:
                    cursor.close()
                    dbConnection.close()
                except MySQLdb.Error as ex:
                    pass

            self.pqueue.put(priority, queue_item)
            # self.pqueue.task_done()

            self.condition.notifyAll()

    """
    def getRequest(self, instanceId = None):
        with self.condition:
            LOG.info("getRequest condition acquired")
            requestPQ = None

            if instanceId:
                LOG.info("getRequest instanceId=%s qsize=%s lsize=%s" % (instanceId, self.qsize(), len(self.fetchedRequests)))

                if (instanceId in self.fetchedRequests):
                    requestPQ = self.fetchedRequests[instanceId]
                    LOG.info("getRequest instanceId=%s --- FOUND!!!!!" % (instanceId))
                else:
                    listQueue = []

                    while not self.empty():
                        priority, request = self.get()
                        LOG.info("getRequest instanceId=%s priority=%s idRecord=%s request=%s" % (instanceId, priority, request["idRecord"], request))

                        if (request["instanceId"] == instanceId):
                            requestPQ = request

                            self.fetchedRequests[instanceId] = request

                            LOG.info("getRequest instanceId=%s FOUND!!!!! lsize=%s" % (instanceId, len(self.fetchedRequests)))

                        else:
                            listQueue.append((priority, request))

                    self.putRequestList(listQueue)
            else:
                while ((requestPQ == None) and (not self.is_closed)):
                    if PriorityQueue.qsize(self) > 0:
                        priority, requestPQ = self.get()

                        self.fetchedRequests[requestPQ["instanceId"]] = requestPQ

                        LOG.info("getRequest lsize=%s" % (len(self.fetchedRequests)))

                    else:
                        self.condition.wait()

                LOG.info("is_closed=%s" % self.is_closed)

            requestDB = None

            if (not self.is_closed and requestPQ):
                LOG.info("idRecord %s" % (requestPQ["idRecord"]))
                idRecord = requestPQ["idRecord"]
                requestDB = self.__getDB(idRecord)
                requestDB.update(requestPQ)

            self.condition.notifyAll()

        request = requestDB
        LOG.info("getPQ condition released")
        return request
    """

    def setPriorityUpdater(self, priority_updater):
        self.priority_updater = priority_updater

    def updatePriority(self):
        if not self.priority_updater:
            #LOG.warn("priority_updater not found!!!")
            return

        with self.condition:
            dbConnection = self.pool.connection()
            cursor = None
            now = datetime.datetime.now()
            queue_items = []

            while not self.pqueue.empty():
                priority, queue_item = self.pqueue.get()

                priority = self.priority_updater(queue_item)

                queue_item.setPriority(priority)

                try:
                    queue_item.setLastUpdate(datetime.datetime.now())

                    QUERY = "update `%s`" % self.name
                    QUERY += " set priority=%s, last_update=%s where id=%s"

                    cursor = dbConnection.cursor()
                    cursor.execute(QUERY,
                                   [queue_item.getPriority(),
                                    queue_item.getLastUpdate(),
                                       queue_item.getId()])

                    dbConnection.commit()

                    queue_items.append(queue_item)
                except Exception as ex:
                    LOG.error(ex)
                    if dbConnection:
                        dbConnection.rollback()
                finally:
                    try:
                        cursor.close()
                        dbConnection.close()
                    except MySQLdb.Error as ex:
                        pass

            if len(queue_items) > 0:
                for queue_item in queue_items:
                    self.pqueue.put(queue_item.getPriority(), queue_item)

                del queue_items

                # self.pqueue.task_done()

            self.condition.notifyAll()

    def toDict(self):
        queue = {}
        queue["name"] = self.name
        queue["size"] = self.pqueue.qsize()

        if self.is_closed:
            queue["status"] = "OFF"
        else:
            queue["status"] = "ON"

        return queue

    """
    def createDB(self, db_name):
        try:
            LOG.info(" db_name %s" %  db_name)
            dbConnection = self.pool.connection()
            cursor = dbConnection.cursor()
            cursor.execute("CREATE DATABASE %s DEFAULT CHARACTER SET 'utf8'" % db_name)
        except mysql.connector.Error as err:
            LOG.error("Failed creating database: %s" % err)
            raise err

        try:
            dbConnection.database = db_name
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_BAD_DB_ERROR:
                create_database(cursor)
                cnx.database = db_name
            else:
                LOG.error(err)
            raise err
    """


class QueueManager(manager.Manager):

    def __init__(self):
        super(
            QueueManager,
            self).__init__(
            name="QueueManager",
            autostart=True,
            rate=60)

        self.config_opts = [
            #cfg.StrOpt('mysql_host', default="localhost", help='the mysql host', required=True),
            #cfg.StrOpt('mysql_user', default="root", help='the mysql user', required=True),
            #cfg.StrOpt('mysql_passwd', default="admin", help='the mysql password', required=True),
            #cfg.StrOpt('mysql_db', default="scheduler_priority_queue", help="the mysql database implementing the priority queue"),
            #cfg.IntOpt('mysql_pool_size', default=10)
        ]

    def execute(self, cmd):
        if cmd.getName() == "get_queue":
            queue_name = cmd.getParameter("name")
            item_list = None

            if queue_name:
                if isinstance(queue_name, str):
                    item_list = [queue_name]
                elif isinstance(queue_name, list):
                    item_list = queue_name

                for item in item_list:
                    if item in self.queue_list:
                        queue = self.queue_list.get(item)
                        cmd.addResult(item, queue.toDict())

                if len(item_list) == 1 and len(cmd.getResults()) == 0:
                    raise Exception("queue '%s' not found!" % item_list[0])
            else:
                for name, queue in self.queue_list.items():
                    cmd.addResult(name, queue.toDict())
        else:
            raise Exception("command '%s' not supported!" % cmd.getName())

    def task(self):
        for queue in self.queue_list.values():
            queue.updatePriority()

            LOG.info("updated priority for queue '%s'" % queue.getName())

    def setup(self):
        LOG.info("%s setup invoked!" % (self.name))

        try:
            self.queue_list = {}

            database = CONF.MYSQL.db
            user = CONF.MYSQL.user
            passwd = CONF.MYSQL.password
            host = CONF.MYSQL.host
            poolSize = CONF.MYSQL.pool_size

            if poolSize < 1:
                poolSize = 10

            self.pool = None
            self.pool = PooledDB(
                mysql.connector,
                poolSize,
                database=database,
                user=user,
                passwd=passwd,
                host=host)
        except Exception as ex:
            LOG.error(ex)
            raise ex

    def destroy(self):
        for queue in self.queue_list.values():
            queue.close()

        if self.pool:
            self.pool.close()

    def createQueue(self, name):
        if name not in self.queue_list:
            queue = Queue(name, self.pool)
            self.queue_list[name] = queue
            return queue
        else:
            raise Exception("the queue '%s' already exists!" % name)

    def deleteQueue(self, name):
        if name not in self.queue_list:
            raise Exception("queue '%s' not found!" % name)

        del self.queue_list[name]

    def getQueue(self, name):
        if name not in self.queue_list:
            raise Exception("queue '%s' not found!" % name)

        return self.queue_list[name]
