import logging
import sqlite3

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import List

from terminaltables import AsciiTable
from colorclass import Color

log = logging.getLogger(__name__)


@dataclass
class Migration:
    version: int
    up: str
    down: str


class MigrationException(Exception):
    pass


class Database(ABC):
    @abstractmethod
    def connection(self):
        pass

    @abstractmethod
    def initialize(self):
        pass

    @abstractmethod
    def add_migration(self, change: List[Migration]):
        pass

    @abstractmethod
    def show_migrations(self, change: List[Migration]):
        print("Showing the database migrations")


class SqliteDatabase(Database):
    def __init__(self, name):
        self.name = name
        self.connect = self.connection()

    def connection(self):
        connect = sqlite3.connect(self.name)
        return connect

    def initialize(self):
        query = (
            'CREATE TABLE migration('
            'id INTEGER PRIMARY KEY NOT NULL,'
            'version INTEGER UNIQUE NOT NULL,'
            'date TIMESTAMP NOT NULL)'
        )

        cur = self.connect.cursor()
        try:
            cur.execute(query)
            cur.execute("INSERT INTO migration(version,date) VALUES(1,?)", (datetime.now(),))
            self.connect.commit()
        except sqlite3.OperationalError:
            log.info('Database already exists')

    def add_migration(self, change: List[Migration]):
        cur = self.connect.cursor()
        cur.execute('SELECT max(version) from migration')
        (max_version,) = cur.fetchone()
        for migration in change:
            print(f'Current migration {migration} ')
            if max_version >= migration.version:
                log.info(f'migration {migration.version} already applied')
                continue

            if migration.version - max_version != 1:
                log.error(f'missing migration version before {migration.version}')
                raise MigrationException('missing migration version before {}'.format(migration.version))
            try:
                cur.execute(migration.up)
                cur.execute("INSERT INTO migration(version,date) VALUES(?,?)", (migration.version, datetime.now()))
                self.connect.commit()
                max_version = migration.version
            except sqlite3.OperationalError as error:
                log.error(f'unable to apply migration {migration.version}')
                raise MigrationException(error)

        self.connect.close()

    def reverse_migration(self, version: int, change: List[Migration]):
        """
        Migration version to revert to from max version.
        So if max version is 10 and version choosen is 5.
        Version 10 - 6 will be reverted
        """
        cur = self.connect.cursor()
        cur.execute('SELECT max(version) from migration')
        (max_id,) = cur.fetchone()
        if version > max_id:
            raise MigrationException('version greater than max version unbale to reverse')

        for migration in reversed(change):
            if migration.version == version:
                break
            elif migration.version > version:
                cur.execute(migration.down)
                cur.execute('DELETE FROM migration where version=?', (migration.version,))
                self.connect.commit()
        self.connect.close()

    def show_migrations(self, change: List[Migration]):
        version = 0
        data = []
        data.append(['Applied', 'Version', 'Date'])

        cur = self.connect.cursor()
        cur.execute('SELECT version, date FROM migration')
        applied = cur.fetchall()
        for m in applied:
            data.append([Color('{autogreen}Yes{/autogreen}'), m[0], m[1]])
            version = m[0]

        for m in change:
            if version < m.version:
                data.append([Color('{autored}No{/autored}'), m.version])

        table = AsciiTable(data)
        print(table.table)



# class PostgresqlDatabase(Database):
#     def __init__(self, host, port, user, password, name):
#         self.host = host
#         self.port = port
#         self.user = user
#         self.password = password
#         self.name = name
#         self.connect = self.connection()
#
#     def connection(self):
#         connect = psycopg2.connect(database=self.database,
#                                    host=self.host,
#                                    user=self.user,s
#                                    password=self.password,
#                                    port=self.port)
#         return connect
#
#     def initialize(self):
#         create_query = ("CREATE TABLE migration("
#                      'id SERIAL PRIMARY KEY,'
#                      'version INTEGER NOT NULL,'
#                      'date DATE NOT NULL)')
#         insert_query = "INSERT INTO migration(version,date) VALUES(1,%s)"
#         try:
#             cur = self.connect.cursor()
#             cur.execute(create_query)
#             cur.execute(insert_query, dt.datetime.now())
#         except psycopg2.OperationalError as error:
#             log.exception(error)
#             exit()

    # def add_migration(self, change: List[Migration]):
    #     """
    #     list of database changes
    #     [1, (insert into)]
    #     """
    #     cur = self.connect.cursor()
    #     cur.execute('SELECT max(version) from migration')
    #     (max_id, _) = cur.fetchone()
    #
    #     for migration in change:
    #         if max_id > migration.version:
    #             log.info(f'schema migration {migration.version} has already been applied')
    #             continue
    #         else:
    #             if max_id - migration != 1:
    #                 log.error(f'migration version {migration.version} not continouus')
    #                 raise MigrationException('migration version not continous')
    #             else:
    #                 try:
    #                     cur.execute(migration.up)
    #                     cur.execute()
    #                     self.connect.commit()
    #                     log.info(f'migration {migration.version} added')
    #                     max_id = migration.version
    #                 except Exception:
    #                     pass
    #
    #
    #     for change_id, sql_statement in change_list:
    #         self.cursor.execute('SELECT max(version) from migration')
    #         (max_id,) = self.cursor.fetchone()
    #         if max_id >= change_id:
    #             log.info("schema change id {} is smaller than the latest"
    #                      "change".format(change_id))
    #             log.info("or schema change id has already been applied ")
    #         else:
    #             try:
    #                 self.cursor.execute(sql_statement)
    #                 self.cursor.execute(insert_sql,
    #                                     (change_id, dt.datetime.now(),))
    #                 self.connect.commit()
    #                 log.info("new schema added")
    #             except Exception:
    #                 log.error("Unable to add schema {}".format(change_id),
    #                           exc_info=True)
    #                 sys.exit()








# class Database:
#     "Create migration control"
#     def __init__(self, db_type, host=None, port=None, user=None,
#                  password=None, database=None):
#         self.db_type = db_type
#         self.host = host
#         self.port = port
#         self.user = user
#         self.password = password
#         self.database = database
#         self.details = ""
#         self.connect = self._get_connector()
#         self.cursor = self.connect.cursor()
#
#     def _get_connector(self):
#         """'
#         Return database connection from specified database from user
#         """
#         supported_databases = {'postgresql': self._postgresql,
#                                'sqlite': self._sqlite}
#         try:
#             connect = supported_databases[self.db_type]()
#             return connect
#         except KeyError:
#             log.critical("Unknown database or not supported")
#             exit()
#
#     def _get_initail_sql_migration(self) -> Tuple[str, str]:
#         """
#         Return 2 sql commands:
#         1) Create migration
#         2) Insert into migration table
#         """
#         sqlite_create = ("CREATE TABLE migration("
#                          'id INTEGER PRIMARY KEY NOT NULL,'
#                          'version INTEGER UNIQUE NOT NULL,'
#                          'date TIMESTAMP NOT NULL)',
#                          "INSERT INTO migration(version,date) VALUES(0,?)")
#
#         pg_create = ("CREATE TABLE migration("
#                      'id SERIAL PRIMARY KEY,'
#                      'version INTEGER NOT NULL,'
#                      'date DATE NOT NULL)',
#                      "INSERT INTO migration(version,date) VALUES(0,%s)")
#
#         all_sql = {'postgresql': pg_create,
#                    'sqlite': sqlite_create}
#
#         return all_sql[self.db_type]
#
#     def initialise(self):
#         "Create new database and add initial migration"
#         create_table, initial_insert = self._get_initail_sql_migration()
#         try:
#             self.cursor.execute(create_table)
#             self.cursor.execute(initial_insert,
#                                 (dt.datetime.now(),))
#             self.connect.commit()
#             log.info("Database has been created")
#         except Exception as e:
#             log.error("Unable to add migration table")
#             log.exception(e)
#             sys.exit()
#
#     def add_schema(self, change_list: List[Tuple[int, str]]):
#         """
#         The first migration change should be version 1
#         """
#         if self.db_type == 'postgresql':
#             insert_sql = "INSERT INTO migration(version,date) VALUES(%s,%s)"
#         elif self.db_type == 'sqlite':
#             insert_sql = "INSERT INTO migration(version,date) VALUES(?,?)"
#
#         for change_id, sql_statement in change_list:
#             self.cursor.execute('SELECT max(version) from migration')
#             (max_id,) = self.cursor.fetchone()
#             if max_id >= change_id:
#                 log.info("schema change id {} is smaller than the latest"
#                          "change".format(change_id))
#                 log.info("or schema change id has already been applied ")
#             else:
#                 try:
#                     self.cursor.execute(sql_statement)
#                     self.cursor.execute(insert_sql,
#                                         (change_id, dt.datetime.now(),))
#                     self.connect.commit()
#                     log.info("new schema added")
#                 except Exception:
#                     log.error("Unable to add schema {}".format(change_id),
#                               exc_info=True)
#                     sys.exit()
#
#     def _postgresql(self):
#         "create postgresql connection and return the connection object"
#         try:
#             import psycopg2
#             connect = psycopg2.connect(database=self.database,
#                                        host=self.host,
#                                        user=self.user,
#                                        password=self.password,
#                                        port=self.port)
#             return connect
#         except ImportError:
#             log.error("Unable to find python3 postgresql module")
#             sys.exit()
#         except psycopg2.Error as e:
#             log.error("Unable to connect to postgresql")
#             log.exception(e)
#             sys.exit()
#         except psycopg2.OperationalError as e:
#             log.exception(e)
#             sys.exit()
#
#     def _sqlite(self):
#         """
#         Create an sqlite connection and return the connection object
#         """
#         import sqlite3
#         try:
#             connect = sqlite3.connect(self.database)
#             return connect
#         except sqlite3.OperationalError:
#             log.error("unable to connect to sqlite database",
#                       exc_info=True)
#             sys.exit()
