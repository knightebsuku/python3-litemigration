# python3-litemigration 1.1.0

Simple module to keep database changes up to date in sql

While I was busy building small database based applications, I needed a way to 

1. modify the database without corrupting the current data
2. keeping track of all the changes that have been made

All my small applications are done in sql, so it made sense to make/track changes in sql as well.

Two of my Gtk applications use this module

[Letmenotifyu](https://github.com/stucomplex/letmenotifyu)

[ClientContacts](https://github.com/stucomplex/ClientContacts)

## Supported Databases

Sqlite3

PostgreSQL

## Example Usage
```python

from litemigration.database import Database

db = Database('sqlite', database="Test.sqlite")
db.initialise()
db.add_schema([
    [1, "CREATE TABLE cats(id INTEGER PRIMARY KEY,name TEXT)"],
    [2, "CREATE TABLE dogs(id INTEGER PRIMARY KEY)"],
    [3, "CREATE TABLE house(id INTEGER)"],
    [4, "INSERT INTO cats(name) VALUES('jow')"]
])
```

So if you want to add a new change all you would have to do is to add another entry to the add_schema method.

```python
[5,"INSERT INTO names(name) VALUES('me')"]
```
When your application starts up it will run through all the changes to make sure they have been applied and apply any new ones.
