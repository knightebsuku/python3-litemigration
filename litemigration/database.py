#!/usr/bin/python3

import datetime as dt
import logging
from . import snapshots


class Database(object):
    "Create migration control"
    def __init__(self, db_type, host=None, port=None, user=None,
                 password=None, database=None):
        self.db_type = db_type
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.details = ""
        self.logger = logging.getLogger(__name__)
        self.connect = self._get_connector()
        self.cursor = self.connect.cursor()

    def _get_connector(self):
        supported_databases = {'postgresql': self._postgresql,
                         'sqlite': self._sqlite}
        try:
            connect = supported_databases[self.db_type]()
            return connect
        except KeyError:
            self.logger.critical("Unknown database or not supported")
            exit()

    def _get_sql(self):
        sqlite_create = ["CREATE TABLE migration("\
          'id INTEGER PRIMARY KEY NOT NULL,'\
          'version INTEGER UNIQUE NOT NULL,'\
          'date TIMESTAMP NOT NULL)',
          "INSERT INTO migration(version,date) VALUES(0,?)"]
          
        pg_create = ["CREATE TABLE migration("\
          'id SERIAL PRIMARY KEY,'\
          'version INTEGER NOT NULL,'\
          'date DATE NOT NULL)',
          "INSERT INTO migration(version,date) VALUES(0,%s)"]
          
        all_sql = {'postgresql': pg_create,
                   'sqlite': sqlite_create}

        return all_sql[self.db_type]

    def initialise(self):
        [create_table, initial_insert] = self._get_sql()
        try:
            self.cursor.execute(create_table)
            self.cursor.execute(initial_insert,
                                (dt.datetime.now(),))
            self.connect.commit()
            self.logger.info("Database has been created")
        except Exception as e:
            self.logger.error("Unable to add migration table")
            self.logger.error(e)
            exit()
            
    def add_schema(self, change_list):
        "Add new schema changes"
        if self.db_type == 'postgresql':
            insert_sql = "INSERT INTO migration(version,date) VALUES(%s,%s)"
        elif self.db_type == 'sqlite':
            insert_sql = "INSERT INTO migration(version,date) VALUES(?,?)"
            
        for (change_id, sql_statement) in change_list:
            self.cursor.execute('SELECT max(version) from migration')
            (max_id,) = self.cursor.fetchone()
            if max_id >= change_id:
                self.logger.info("schema change {} is smaller the lastest schema change {}"\
                                 " or new change id has already being applied".format(change_id, max_id))
            else:
                try:
                    self.cursor.execute(sql_statement)
                    self.cursor.execute(insert_sql,
                                         (change_id, dt.datetime.now(),))
                    self.connect.commit()
                    self.logger.info("new schema added")
                except Exception:
                    self.logger.error("Unable to add schema {}".format(change_id),
                                         exc_info=True)
                    exit()

    def _postgresql(self):
        "create postgresql connections"
        try:
            import psycopg2
            connect = psycopg2.connect(database=self.database,
                                       host=self.host,
                                       user=self.user,
                                       password=self.password,
                                       port=self.port)
            return connect
        except ImportError:
            self.logger.error("Unable to find python3 postgresql module")
            exit()
        except psycopg2.Error as e:
            self.logger.error("Unable to connect to postgresql")
            self.logger.exception(e)
            exit()
        except psycopg2.OperationalError as e:
            self.logger.exception(e)
            exit()

    def _sqlite(self):
        "create sqlite connections"
        import sqlite3
        try:
            connect = sqlite3.connect(self.database)
            return connect
        except sqlite3.OperationalError:
            self.logging.error("unable to connect to sqlite database",exc_info=True)
            exit()
