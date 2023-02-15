#!/usr/bin/python3

import argparse
import importlib

from litemigration.database import Database

def check_settings() -> dict:
    """
    Looks for database.py file with the list of migrations
    List of migrations variable should be migration_changes
    Returns the module file
    """
    try:
        mod = importlib.import_module('database')
        return {
            'database': mod.db,
            'changes': mod.MIGRATION_CHANGES
        }

    except ModuleNotFoundError as error:
        print(f'Unable to find database file: {error}')
        exit()
    except AttributeError as error:
        print(f'Unable to find migration_changes: {error}')
        exit()


def show_migrations(params):
    """
    Show the status of the current migrations
    """
    settings = check_settings()
    db = settings['database']
    changes = settings['changes']
    table = db.show_migrations(changes)
    print(table.table)


def migration(params):
    """
    * Add new migrations
    * Reverse existing migrations
    """
    settings = check_settings()
    db: Database = settings['database']
    changes = settings['changes']
    if params.direction == 'up':
        db.add_migrations(changes)
    elif params.direction == 'down' and params.dry:
        if params.version == 0:
            print("migration version needed")
            exit()
        else:
            table = db.dry_run_reverse(params.version, changes)
            print(table.table)
    elif params.direction == 'down':
        if params.version == 0:
            print("migration version needed")
            exit()
        else:
            table = db.reverse_migrations(params.version, changes)
            print(table.table)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Manage migrations')
    subparsers = parser.add_subparsers(help='sub-command help')

    show_migration = subparsers.add_parser('showmigrations', help='show current migrations')
    show_migration.set_defaults(func=show_migrations)

    migrate = subparsers.add_parser('migrate', help='Run forward or reverse migrations')
    migrate.add_argument('direction', choices=['up', 'down'], help='forward [up] or reverse migration [down]')
    migrate.add_argument('version', help='Version number at which to stop the migration', nargs='?', type=int, default=0)
    migrate.add_argument('--dry', help='Show migrations to be reversed or applied', action='store_const', const=True)
    migrate.set_defaults(func=migration)

    args = parser.parse_args()
    args.func(args)



