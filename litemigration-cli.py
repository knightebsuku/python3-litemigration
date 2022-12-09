import argparse
import importlib

def check_settings():
    """
    Looks for database.py file with the list of migrations
    List of migrations variable should be migration_changes
    Returns the module file
    """
    try:
        mod = importlib.import_module('database')
        return mod
        #print(type(mod))
        #print(dir(mod))
        #print(mod.migration_changes)

    except ModuleNotFoundError:
        print('Unable to find database file')
        exit()
    except AttributeError:
        print('Unable to find migration_changes')
        exit()



def show_migrations():
    """
    Get
    """
    module = check_settings()
    db = module.db
    db.show_migrations(module.migration_changes)

def migration(direction: str):
    print('Run forward or reverse migration')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Manage migrations')
    subparsers = parser.add_subparsers(help='sub-command help')

    show_migration = subparsers.add_parser('showmigrations', help='show current migrations')
    show_migration.set_defaults(func=show_migrations)

    migrate = subparsers.add_parser('migrate', help='Run forward or reverse migrations')
    migrate.set_defaults(func=migration)
    migrate.add_argument('direction', choices=['up', 'down'], help='forward or reverse migration')

    args = parser.parse_args()
    args.func()


# showmigrations - show all migrations, including unapplied
# migrate up - apply migrations
# migrate down - reverse migrations upto a particular version
#
# from litemigration.database import SqliteDatabase, Migration
#
#
# db = SqliteDatabase('example.db')
# db.initialize()
#
# change = [
#     Migration(
#         version=2,
#         up='CREATE TABLE player(name VARCHAR NOT NULL,score INTEGER)',
#         down='DROP TABLE player'
#     ),
#     Migration(
#         version=3,
#         up='INSERT INTO player(name,score) VALUES("Menzi", 10)',
#         down='DELETE FROM PLAYER where name="Menzi"'
#     )
# ]
#
#
# db.add_migration(change)
# #db.reverse_migration(2, change)

