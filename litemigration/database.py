#!/usr/bin/python3

import sqlite3
import datetime as dt
import logging
import MySQLdb
import psycopg2

class Database(object):
    "Create migration control"
    def __init__(self, db_type,details,logger = None):
        self.db_type = db_type
        self.details = details
        self.cursor = self.check_database_type()
        self.logger = logging.getLogger(__name__)

    def initialise(self):
        "Create new database on new start"
        try:
            self.connect.execute("CREATE TABLE migration("\
                             'id INTEGER PRIMARY KEY NOT NULL,'\
                             'version INTEGER UNIQUE NOT NULL,'\
                             'date TIMESTAMP NOT NULL)')
            self.cursor.execute("INSERT INTO migration(version,date) VALUES(0,?)", (dt.datetime.now(),))
            self.connect.commit()
            self.connect.close()
        except sqlite3.OperationalError as e:
            self.logger.exeception(e)
            exit()

    def check_database_type(self):
        database_dict = {'postgresql': self.postgres,
                         'mysql': self.mysql,
                         'sqlite': self.sqlite}
        try:
            choosen_db = database_dict[self.db_type]
            self.connect = choosen_db()
        except KeyError:
            logging.error("Unknown database")
            exit()
            

    def add_schema(self, change_list):
        "Add new schema changes"
        for (change_id, sql_statement) in change_list:
            self.cursor.execute('SELECT max(version) from migration')
            (max_id,) = self.cursor.fetchone()
            if max_id >= change_id:
                self.logger.info("schema change {} is smaller the lastest schema change {} or new change id has already being applied".format(change_id, max_id))
            else:
                try:
                    self.connect.execute(sql_statement)
                    self.connect.execute("INSERT INTO migration(version,date) VALUES(?,?)",
                                         (change_id, dt.datetime.now(),))
                    self.connect.commit()
                except sqlite3.OperationalError:
                    self.logger.error("Unable to add schema {}".format(change_id),
                                         exc_info=True)
                    exit()

    def postgresql(self):
        "create postgresql connections"
        
        try:
            connect = psycopg2.connect(self.details)
            return connect
        except psycopg2.Error as e:
            logging.error("Unable to connect to postgresql")
            logging.exeception(e)
            exit()
    def mysql(self):
        "create mysql connections"
        connect = MySQLdb.connect(self.details)
        return connect
    def sqlite(self):
        "create sqlite connections"
        connect = sqlite3.connect(self.details)
        return connect
        
