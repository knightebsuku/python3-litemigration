import unittest
import os
from datetime import datetime
from freezegun import freeze_time

from colorclass import Color
from litemigration.database import SqliteDatabase, Migration


@freeze_time('2022-01-01 00:00:00')
class TestMigration(unittest.TestCase):

    def setUp(self) -> None:
        self.db = SqliteDatabase('test.db')
        self.db.initialize()
        self.migration_changes = [
            Migration(
                version=2,
                up='CREATE TABLE player(name VARCHAR NOT NULL,score INTEGER)',
                down='DROP TABLE player'
            ),
            Migration(
                version=3,
                up='INSERT INTO player(name,score) VALUES("User", 10)',
                down='DELETE FROM PLAYER where name="User"'
            )
        ]

    def test_add_migration(self):
        self.db.add_migrations(self.migration_changes)

        cur = self.db.connection().cursor()
        cur.execute('SELECT max(version) FROM migration')
        (max_id,) = cur.fetchone()
        self.assertEqual(max_id, 3)

    def test_show_migrations(self):
        data = [
            ['Applied', 'Version', 'Date'],
            [Color('{autogreen}Yes{/autogreen}'), 1, datetime.now()],
            [Color('{autored}No{/autored}'), 2],
            [Color('{autored}No{/autored}'), 3]
        ]
        table = self.db.show_migrations(self.migration_changes)
        print(table.table_data)
        self.assertEqual(data, table.table_data)

    def test_reverse_migrations(self):
        pass

    def test_dry_reverse_migrations(self):
        pass

    def tearDown(self) -> None:
        os.remove('test.db')



if __name__ == '__main__':
    unittest.main()
