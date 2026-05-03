# Contents

- [Sql-Engine](#sql-engine)
  - [Features](#features)
  - [Purpose](#purpose)
  - [Installation](#installation)
  - [Env](#env)
  - [Usage](#usage)
- [Docs](#docs)
  - [Table Declaration](#table-declaration)
    - [Class Declaration](#class-declaration)
    - [Schema Declaration](#schema-declaration)
  - [Table Instantiation](#table-instantiation)
  - [Custom Types](#custom-types)
  - [Transactions](#transactions)
    - [Transaction per Table](#transaction-per-table)
    - [Shared Connection](#shared-connection)
  - [Syntax sugar](#syntax-sugar)
    - [Single Index](#single-index)
    - [Multi Index](#multi-index)
  - [More examples](#more-examples)
    - [Instantiate](#instantiate)
    - [Insert single row](#insert-single-row)
    - [Insert many rows](#insert-many-rows)
    - [Insert many rows in transaction](#insert-many-rows-in-transaction)
    - [Query select with specified columns](#query-select-with-specified-columns)
    - [Query select with multiple specified values](#query-select-with-multiple-specified-values)
    - [Delete rows](#delete-rows)
    - [Select all](#select-all)
    - [Create transaction rows batch generator](#create-transaction-rows-batch-generator)


# Sql-Engine

My Sql-Engine is a cute little wrapper for `sqlite3` table manipulations without any third party dependencies **(vibe-code free!)**


## Features

Abstracts SQL queries into tiny little methods like `select`, `insert`, `delete_rows`, `update`, `upsert`, etc. Sql-Engine also provides bulk insertion and transaction methods, like `insert_many` and `fetchall_iterator`. Methods can be executed in transaction mode thanks to `transaction` context manager.


## Purpose

It's a tiny little modern project that lets you prototype your databases locally with great flexibility. Also, it can be used in small production apps like chat bots to store data, but **beware! Security might be flawed, proof query execution beforehand.** In other cases, sure, use it as you like.


## Installation

To install `sqlengine`, you can use `pip`:

```sh
git clone --depth 1 https://github.com/suffermuffin/SQL-Engine.git
pip install -e .
```

## Env

You may set environment variable for logging. By default it's `WARNING`.

```console
SQL_ENGINE_LOG_LEVEL=INFO
```

## Usage

All you have to do to create your own cute little table is to [inherit](#class-declaration) `SqlTableMixin` class or to create your own [schema](#schema-declaration) and declare desired properties of your table's columns. They are:


Name of the table that will be used in queries. If omitted in inherited class declaration, then it will take the class name.
```py
__tablename__ : Optional[str]
```

Column names of the table
```py
__columns__ : list[str] 
```

Column types of the table
```py
__types__ : list[SqlType | str] 
```

List of primary keys
```py
__primary__ :list[str] 
```

# Docs

Here are some examples in code.

## Table Declaration

There are couple of ways to declare your table.


### Class Declaration

One way is to use inheritance from `SqlTableMixin` with sql-native types declaration:

```py
from sqlengine import SqlTableMixin

class EmployeesA(SqlTableMixin):

    __tablename__ = "Employees"
    __columns__   = ["ID", "name", "surname", "salary", "position"]
    __types__     = ["INTEGER", "TEXT", "TEXT", "REAL", "TEXT"]
    __primary__   = ["ID", "name"]

```

or with python types if more convenient:

```py
class EmployeesA(SqlTableMixin):

    __tablename__ = "Employees"
    __columns__   = ["ID", "name", "surname", "salary", "position"]
    __types__     = [int, str, str, float, str]
    __primary__   = ["ID", "name"]

```

### Schema Declaration

The other one is to use schemas:

```py
from sqlengine import schema, Schema

employees_schema : Schema = {
    "tablename": "Employees",
    "columns"  : ["ID", "name", "surname", "salary", "position"],
    "types"    : [int, str, str, float, str],
    "primary"  : ["ID", "name"],
}
```

## Table Instantiation

You may instantiate a table via your class or schema. You have to provide a path to the database where the data exists or you wish to create a new table/database. You can also use `:memory:` to complete operations [in memory](https://sqlite.org/inmemorydb.html). You may operate on `:memory:` databases only in [transactions](#transactions) because they don't commit to a file.

```py
table_a = EmployeesA("temp/data.db")
table_b = schema.table_from_schema(":memory:", employees_schema)

table_a.schema == table_b.schema # -> True
```

You also can instantiate all the tables from one database at once:

```py
tables = schema.table_from_database("temp/chinook.db")

for table in tables:
    print(f"{table.tablename}: {table.shape}")

# ->
# Album: (3, 347)
# Artist: (2, 275)
# Customer: (13, 59)
# Employee: (15, 8)
# ...

```

If you pass the table name in this function you will get a single table:

```py
album = schema.table_from_database("temp/chinook.db", "Album")

print(album)

# -> Album(database=temp/chinook.db, tablename=Album, columns=(AlbumId, Title, ArtistId), types=(INTEGER, NVARCHAR(160), INTEGER), primary=(AlbumId))
```

## Custom Types

You can define non-native sqlite datatypes. They have to implement `to_sql` and `from_sql`. Or there are other ways that `sqlite3` [documentation](https://docs.python.org/3/library/sqlite3.html#how-to-adapt-custom-python-types-to-sqlite-values) explains.


```py
class Point:
    def __init__(self, x : float, y : float) -> None:
        self.x = x
        self.y = y

    def to_sql(self) -> str:
        """ 
        Method that converts object implementation
        to native sqlite3 value 
        """
        return f"{self.x}, {self.y}"
    
    @classmethod
    def from_sql(cls, sql : bytes):
        """ 
        Method that accepts bytes 
        and returns own object implementation
        """
        x, y = list(map(float, sql.split(b",")))
        return cls(x, y)
    
```

Then to use this type in table - declare it in `__types__` attribute

```py
class Coordinates(SqlTableMixin):

    __columns__ = ["ID", "name", "coords", "temp"]
    __types__   = [int, str, Point, float]
    __primary__ = ["ID"]

    def insert(self, id : int, name : str, coords : Point, temp : float) -> None:
        return super().insert(id, name, coords, temp)
    
    def upsert(self, id : int, name : str, coords : Point, temp : float) -> None:
        return super().upsert(id, name, coords, temp)
```

Should work just right

```py
table = Coordinates("temp/data.db")

table.upsert(0, "Kazahstan", Point(43.2380, 76.8829), 14)
row = table.select_eq("ID", 0)[0]

isinstance(row[2], Point) # -> True
```

## Transactions

Transactions help you to not spam `execute()` -> `commit()` on a database for each of operations which is computationally heavy.

### Transaction per Table

```py
class Homies(SqlTableMixin):

    __columns__   = ["ID", "Name", "Age"]
    __types__     = [int, str, int]
    __primary__   = ["ID"]

table = Homies(":memory:")

data = [
    (0, 'John', 20), (1, 'Boris', 23), (2, 'George', 34),
    (3, 'Kate', 18), (4, 'Angela', 40), (5, 'Mark', 28),
    (6, 'Max', 21), (7, 'Maria', 33)
]
```

Transactions must be called with `with` statement. They allow to use `__iter__` and `fetchall_iterator` methods that otherwise are not available.

```py
from sqlengine import sqlgen as sql

with table.transaction():
    
    table.create_table()
    table.insert_many(data)
    
    for idx, name, age in table:
        where = sql.where_equals("ID", idx)
        assert(isinstance(age, int)) # typing is lacking in returning rows
        table.update(where, {"Age" : age + 1})
    
    older_homies = table.select()

older_homies

# ->
# [
#     (0, 'John', 21), (1, 'Boris', 24), (2, 'George', 35),
#     (3, 'Kate', 19), (4, 'Angela', 41), (5, 'Mark', 29),
#     (6, 'Max', 22), (7, 'Maria', 34)
# ]
```

### Shared Connection

To use multiple databases and tables in the same transaction (spanning multiple databases), you can use `shared_connection` util.

```py
CHINOOK_DB = "temp/chinook.db"

chinook_tables : list[SqlTableMixin] = schema.table_from_database(CHINOOK_DB)
biggest_table  : SqlTableMixin       = max(chinook_tables, key=lambda x: len(x) * len(x.columns))

biggest_table.tablename, biggest_table.shape # -> ('Track', (9, 3503))

```

With `shared_connection` you can also use `__iter__` and `fetchall_iterator` methods.

```py
from sqlengine.utils import shared_connection

copy_biggest_table = schema.table_from_schema(":memory:", biggest_table.schema)

with shared_connection(copy_biggest_table, biggest_table, **biggest_table.connection_params):
    copy_biggest_table.create_table()
    query = sql.select('Track')
    
    for batch in biggest_table.fetchall_iterator(query, 1000):
        copy_biggest_table.insert_many(batch)

    final_shape = copy_biggest_table.shape

final_shape
```

```sql
-- Debug output --
Track: Using in-memory database
Starting shared transaction across 2 databases
Track: CREATE TABLE IF NOT EXISTS Track (TrackId INTEGER, Name NVARCHAR(200), AlbumId INTEGER, MediaTypeId INTEGER, GenreId INTEGER, Composer NVARCHAR(220), Milliseconds INTEGER, Bytes INTEGER, UnitPrice NUMERIC(10,2), PRIMARY KEY (TrackId)); 
Track: INSERT INTO Track (TrackId, Name, AlbumId, MediaTypeId, GenreId, Composer, Milliseconds, Bytes, UnitPrice) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?); [(...), ...]
Track: INSERT INTO Track (TrackId, Name, AlbumId, MediaTypeId, GenreId, Composer, Milliseconds, Bytes, UnitPrice) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?); [(...), ...]
Track: INSERT INTO Track (TrackId, Name, AlbumId, MediaTypeId, GenreId, Composer, Milliseconds, Bytes, UnitPrice) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?); [(...), ...]
Track: INSERT INTO Track (TrackId, Name, AlbumId, MediaTypeId, GenreId, Composer, Milliseconds, Bytes, UnitPrice) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?); [(...), ...]
Track: SELECT COUNT(*) FROM Track;
Shared transaction finished
```

## Syntax sugar

### Single Index

`SqlTableMixin` has `__getitem__` implementation. Usage differs based on `__primary__`. Best case is a single `INTEGER` column:

```py
table = Coordinates("temp/data.db", force_drop=True)

coords_data = [
    (0,  "loc0",  Point(0,0),   0.0),
    (1,  "loc1",  Point(1,1),   1.1),
    (2,  "loc0",  Point(2,2),   2.2),
    (3,  "loc3",  Point(3,3),   3.3),
    (4,  "loc4",  Point(4,4),   4.4),
    (5,  "loc5",  Point(5,5),   5.5),
    (6,  "loc6",  Point(6,6),   6.6),
    (7,  "loc7",  Point(7,7),   7.7),
    (10, "loc10", Point(10,10), 10.10),
]

table.insert_many(coords_data)
```

You can implicitly call by `ID` column (which is primary in this case):

```py
table[4] # -> (4, 'loc4', Point(4.0, 4.0), 4.4)
```

```sql
-- Debug output --
Coordinates: SELECT * FROM Coordinates WHERE ID = 4;
```

You can even slice your cute little table

```py
table[:4:-1]

# -> 
# [(4, 'loc4', Point(4.0, 4.0), 4.4),
#  (3, 'loc3', Point(3.0, 3.0), 3.3),
#  (2, 'loc0', Point(2.0, 2.0), 2.2),
#  (1, 'loc1', Point(1.0, 1.0), 1.1),
#  (0, 'loc0', Point(0.0, 0.0), 0.0)]
```

```sql
-- Debug output --
Coordinates: SELECT * FROM Coordinates WHERE ID BETWEEN 0 AND 4 ORDER BY ID;
```

### Multi Index

In case of multiple values in `__primary__` you have to call with list key in order of declared `__primary__`:

```py
class Employees(SqlTableMixin):

    __columns__   = ["ID", "name", "surname", "salary", "position"]
    __types__     = [int, str, str, float, "TEXT NOT NULL"]
    __primary__   = ["ID", "name"]

table = Employees("temp/data.db", force_drop=True)

employees_data = [
    (0, "test0", "surname0", 100.0, "pos0"),
    (0, "test1", "surname0", 100.0, "pos0"),
    (1, "test0", "surname1", 111.1, "pos1"),
    (1, "test1", "surname1", 111.1, "pos1"),
    (2, "test2", "surname2", 122.2, "pos2"),
    (3, "test3", "surname3", 133.3, "pos3"),
    (4, "test4", "surname4", 144.4, "pos4"),
]

table.insert_many(employees_data)

```

```py
table[[3, "test3"]] # -> (3, 'test3', 'surname3', 133.3, 'pos3')
```

```sql
-- Debug output --
Employees: SELECT * FROM Employees WHERE ID = 3 AND name = "test3";
```

You can get number of rows in your table with `len`:

```py
len(table) # -> 7
```

```sql
-- Debug output --
Employees: SELECT COUNT(*) FROM Employees;
```


## More examples

**and under the hood queries**

```py
class Employees(SqlTableMixin):

    __columns__   = ["ID", "Name", "Occupation"]
    __types__     = ["INT", "TEXT NOT NULL", "TEXT"]
    __primary__   = ["ID"]
    __tablename__ = "EmployeesDB"

    # You may overwrite your insert methods for type consistency
    def insert(self, id : int, name : str, occupation : str) -> None:
        return super().insert(id, name, occupation)
    
    def upsert(self, id : int, name : str, occupation : str) -> None:
        return super().upsert(id, name, occupation)

```

---
### Instantiate

```py
# Force table overwrite with `force_drop=True`
table = Employees("MyBusiness.db", force_drop=True)
```

```sql
-- Debug output --
Employees: DROP TABLE IF EXISTS Employees 
Employees: CREATE TABLE IF NOT EXISTS Employees (ID INT, Name TEXT NOT NULL, Occupation TEXT, PRIMARY KEY (ID));
```

---
### Insert single row

```py
table.insert(0, 'John', 'CEO')
```

```sql
-- Debug output --
Employees: INSERT INTO Employees (ID, Name, Occupation) VALUES (?, ?, ?); (0, 'John', 'CEO')
```

---
### Insert many rows

```py
workers  = [(1, 'Boris', 'worker'), (2, 'George', 'worker'), (3, 'Kate', 'worker')]
table.insert_many(workers)
```

```sql
-- Debug output --
Employees: INSERT INTO Employees (ID, Name, Occupation) VALUES (?, ?, ?); [(1, 'Boris', 'worker'), (2, 'George', 'worker'), (3, 'Kate', 'worker')]
```

---
### Insert many rows in transaction

```py
batch_size = 2

sales = [
    (4, 'Angela', 'seller'), (5, 'Mark', 'seller'), 
    (6, 'Max', 'seller'), (7, 'Maria', 'seller')
]

with table.transaction():
    for i in range(0, len(sales), batch_size):
        batch = sales[i: i + batch_size]
        table.insert_many(batch)
```

```sql
-- Debug output --
Employees: Transaction started
Employees: INSERT INTO Employees (ID, Name, Occupation) VALUES (?, ?, ?); [(4, 'Angela', 'seller'), (5, 'Mark', 'seller')]
Employees: INSERT INTO Employees (ID, Name, Occupation) VALUES (?, ?, ?); [(6, 'Max', 'seller'), (7, 'Maria', 'seller')]
Employees: Transaction finished
```

---
### Query select with specified columns

```py
table.select_eq('Occupation', 'CEO', return_columns=['Name', 'ID'])

# Returns
[('John', 0)]
```

```sql
-- Debug output --
Employees: SELECT Name, ID FROM Employees WHERE Occupation = "CEO";
```

---
### Query select with multiple specified values

```py
table.select_eq('Occupation', equals=['worker', 'CEO'])

# Returns
[(0, 'John', 'CEO'),
 (1, 'Boris', 'worker'),
 (2, 'George', 'worker'),
 (3, 'Kate', 'worker')]
```

```sql
-- Debug output --
Employees: SELECT * FROM Employees WHERE Occupation in ("worker", "CEO");
```

---
### Delete rows

```py
table.delete_eq('ID', 1)
```

```sql
-- Debug output --
Employees: DELETE FROM Employees WHERE ID = 1; 
```

---
### Select all

```py
table.select()

# Returns
[(0, 'John', 'CEO'),
 (2, 'George', 'worker'),
 (3, 'Kate', 'worker'),
 (4, 'Angela', 'seller'),
 (5, 'Mark', 'seller'),
 (6, 'Max', 'seller'),
 (7, 'Maria', 'seller')]
```

```sql
-- Debug output --
Employees: SELECT * FROM Employees;
```
---
### Create transaction rows batch generator

```py
import logging
from sqlengine import sqlgen as sql

logger = logging.getLogger(__name__)

with table.transaction():
    
    query = sql.select(table.tablename, columns=['Name', 'ID'])
    batches = table.fetchall_iterator(query, batch_size=2)
    
    for i, batch in enumerate(batches):
        logger.debug(f"batch {i}: {batch}")
```

```sql
-- Debug output --
Employees: Transaction started
batch 0: [('John', 0), ('George', 2)]
batch 1: [('Kate', 3), ('Angela', 4)]
batch 2: [('Mark', 5), ('Max', 6)]
batch 3: [('Maria', 7)]
Employees: Transaction finished
```
