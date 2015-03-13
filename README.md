# python3-litemigration 1.0.0

Simple module to keep database changes update to date with raw sql


## Supported Databases

Sqlite3
PostgreSQL

## Example Usage
```python

from litemigration.database import Database

postgres  = Database("postgresql",
		database="host=localhost user=postgres password=postgres dbname=test")
postgres.initialise() # create migration table and insert first change

postgres.add_schema([
    [1,"CREATE TABLE names(id SERIAL PRIMARY KEY,name TEXT)"],
    [2,"INSERT INTO names(name) VALUES('Tommy')"],
    [3,"INSERT INTO names(name) VALUES('Billy')"]
    ])
```

So if you want to add a new change all you would have to do is to add another entry to the add_schema method

```python
[4,"INSERT INTO names(name) VALUES('me')"]
```
When your application starts up it will run through all the changes to make sure they have been applied

