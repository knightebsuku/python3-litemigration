import logging
import sqlite3

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import List

from terminaltables import AsciiTable
from colorclass import Color

try:
    import psycopg2
except  ImportError:
    pass


log = logging.getLogger(__name__)


@dataclass
class Migration:
    version: int
    up: str
    down: str


class MigrationException(Exception):
    pass


class Database(ABC):
    def __init__(self):
        self.connect = self.connection()

    @abstractmethod
    def connection(self):
        pass

    @abstractmethod
    def initialize(self):
        pass

    @abstractmethod
    def add_migrations(self, change: List[Migration]):
        pass

    @abstractmethod
    def reverse_migrations(self, version: int, change: List[Migration]):
        pass

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
        return table

    def dry_run_reverse(self, version: int, change: List[Migration]):
        """
        Show which changes will be reversed (--dry)
        """
        data = []
        data.append(['Reversed (Dummy)', 'Version'])

        cur = self.connect.cursor()
        cur.execute('SELECT max(version) from migration')
        (max_id,) = cur.fetchone()
        if version > max_id:
            raise MigrationException('version greater than max version unable to reverse')

        for migration in reversed(change):
            if migration.version == version:
                break
            elif migration.version > version:
                data.append([Color('{autogreen}Yes{/autogreen}'), migration.version])

        table = AsciiTable(data)
        return table


class SqliteDatabase(Database):
    def __init__(self, name):
        self.name = name
        super().__init__()

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
        except sqlite3.OperationalError as error:
            log.info(f'Error creating migration table: {error}')
            raise MigrationException(error)

    def add_migrations(self, change: List[Migration]):
        cur = self.connect.cursor()
        cur.execute('SELECT max(version) from migration')
        (max_version,) = cur.fetchone()
        for migration in change:
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
                print(f'Migration {migration.version} applied....' + Color('{autogreen}Ok{/autogreen}'))
                max_version = migration.version
            except sqlite3.OperationalError as error:
                print(f'Migration {migration.version} applied....' + Color('{autored}Error{/autored}'))
                log.error(f'unable to apply migration {migration.version}')
                raise MigrationException(error)

        self.connect.close()

    def reverse_migrations(self, version: int, change: List[Migration]):
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


class Postgresql(Database):

    def __init__(self, name, user, password, host, port=5432):
        self.name = name
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        super().__init__()

    def connection(self):
        """
        Check if psycopg2 is installed if using postgres database
        """
        try:
            import psycopg2
        except ImportError:
            raise MigrationException('postgresql driver not installed')

        try:
            conn = psycopg2.connect(
                host=self.host,
                database=self.name,
                user=self.user,
                password=self.password,
                port=self.port
            )
        except psycopg2.OperationalError as error:
            log.error(f'Unable to connect to database: {error}')
            raise MigrationException(f'Unable to connect to database: {error}')
        else:
            return conn

    def initialize(self):
        query = (
            'CREATE TABLE migration('
            'id SERIAL PRIMARY KEY NOT NULL,'
            'version INTEGER UNIQUE NOT NULL,'
            'date TIMESTAMP NOT NULL)'
        )

        cur = self.connect.cursor()
        try:
            cur.execute(query)
            cur.execute("INSERT INTO migration(version,date) VALUES(1, %s)", (datetime.now(),))
            self.connect.commit()
        except (psycopg2.OperationalError, psycopg2.DatabaseError) as error:
            log.info(f'Error creating migration table: {error}')
            raise MigrationException(error)

    def add_migrations(self, change: List[Migration]):
        cur = self.connect.cursor()
        cur.execute('SELECT max(version) from migration')
        (max_version,) = cur.fetchone()
        for migration in change:
            if max_version >= migration.version:
                log.info(f'migration {migration.version} already applied')
                continue

            if migration.version - max_version != 1:
                log.error(f'missing migration version before {migration.version}')
                raise MigrationException('missing migration version before {}'.format(migration.version))
            try:
                cur.execute(migration.up)
                cur.execute("INSERT INTO migration(version,date) VALUES(%s,%s)", (migration.version, datetime.now()))
                self.connect.commit()
                print(f'Migration {migration.version} applied....' + Color('{autogreen}Ok{/autogreen}'))
                max_version = migration.version
            except (psycopg2.OperationalError, psycopg2.DatabaseError) as error:
                print(f'Migration {migration.version} applied....' + Color('{autored}Error{/autored}'))
                log.error(f'unable to apply migration {migration.version}')
                raise MigrationException(error)

        self.connect.close()

    def reverse_migrations(self, version: int, change: List[Migration]):
        """
        Migration version to revert to from max version.
        So if max version is 10 and version chosen is 5.
        Version 10 - 6 will be reverted
        """
        data = []
        data.append(['Reversed', 'Version'])

        cur = self.connect.cursor()
        cur.execute('SELECT max(version) from migration')
        (max_id,) = cur.fetchone()
        if version > max_id:
            raise MigrationException('version greater than max version unable to reverse')

        for migration in reversed(change):
            if migration.version == version:
                break
            elif migration.version > version:
                try:
                    cur.execute(migration.down)
                    cur.execute('DELETE FROM migration where version=%s', (migration.version,))
                    self.connect.commit()
                    data.append([Color('{autogreen}Yes{/autogreen}'), migration.version])
                except (psycopg2.DatabaseError, psycopg2.OperationalError):
                    data.append([Color('{autored}Error{/autored}'), migration.version])
        self.connect.close()
        table = AsciiTable(data)
        return table
