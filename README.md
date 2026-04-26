# Sql-Engine

My Sql-Engine is a cute little wrapper for `sqlite3` table manipulations without any third party dependencies **(vibe-code free!)**


## Features

Abstracts SQL queries into tiny little methods like `select`, `insert`, `delete_rows`, `update`, `upsert`, etc. Sql-Engine also provides bulk insertion and transaction methods, like `insert_many` and `fetchall_iterator`. Methods can be executed in transaction mode thanks to `transaction` context manager.


## Purpose

It's a tiny little modern projects that lets you prototype your databases locally with great flexability. Also, it can be used in small production apps like chat bots to store data, but **beware! Security might be flowed, proof query execution beforehand.** In other cases, sure, use it as you like.


## Installation

To install `sqlengine`, you can use `pip`:

```sh
git clone --depth 1 https://github.com/suffermuffin/SQL-Engine.git
pip install -e .
```

Or you may add this package as a `git submodule`:

```sh
git submodule add --depth 1 https://github.com/suffermuffin/SQL-Engine.git /path/to/submodules/sqlengine
pip install -e ./path/to/submodules/sqlengine
```

## Usage

All you have to do to create your own cute little table is to inherit `SqlTableMixin` class and declare desired properties of your table's columns. They are:

 - **\_\_columns\_\_** 
   - *(list[str])* Column names of the table
 - **\_\_types\_\_**
   - *(list[str])* Types of declared columns
 - **\_\_primary\_\_**
   - *(list[str])* List of one or more primary keys
 - **\_\_tablename\_\_**
   - *(str)* Name of the table


## Env

You may set environment variable for logging. By default it's `WARNING`

```console
SQL_ENGINE_LOG_LEVEL=INFO
```

### Example in code

**Declare your table**

```py
from sqlengine import SqlTableMixin

class Employees(SqlTableMixin):

    __columns__   : list[str] = ["ID", "Name", "Occupation"]
    __types__     : list[str] = ["INT", "TEXT NOT NULL", "TEXT"]
    __primary__   : list[str] = ["ID"]
    __tablename__ : str = "EmployeesDB"

    # You may overwrite your insert methods for type consistancy
    def insert(self, id : int, name : str, occupation : str) -> None:
        return super().insert(id, name, occupation)
    
    def upsert(self, id : int, name : str, occupation : str) -> None:
        return super().upsert(id, name, occupation)

```
---
**Instantiate**

```py
# Force table overwrite with `force_drop=True`
employees_table = Employees("MyBusiness.db", force_drop=True)
```

```sql
-- Debug output --
Employees: DROP TABLE IF EXISTS EmployeesDB 
Employees: CREATE TABLE IF NOT EXISTS EmployeesDB (ID INT, Name TEXT NOT NULL, Occupation TEXT, PRIMARY KEY (ID));
```

---
**Insert single row**

```py
employees_table.insert(0, 'John', 'CEO')
```

```sql
-- Debug output --
Employees: INSERT INTO EmployeesDB (ID, Name, Occupation) VALUES (?, ?, ?); (0, 'John', 'CEO')
```

---
**Insert many rows**

```py
workers  = [(1, 'Boris', 'worker'), (2, 'George', 'worker'), (3, 'Kate', 'worker')]
employees_table.insert_many(workers)
```

```sql
-- Debug output --
Employees: INSERT INTO EmployeesDB (ID, Name, Occupation) VALUES (?, ?, ?); [(1, 'Boris', 'worker'), (2, 'George', 'worker'), (3, 'Kate', 'worker')]
```

---
**Insert many rows in transaction**

```py
batch_size = 2

sales = [
    (4, 'Angela', 'seller'), (5, 'Mark', 'seller'), 
    (6, 'Max', 'seller'), (7, 'Maria', 'seller')
]

with employees_table.transaction():
    for i in range(0, len(sales), batch_size):
        batch = sales[i: i + batch_size]
        employees_table.insert_many(batch)
```

```sql
-- Debug output --
Employees: Transaction started
Employees: INSERT INTO EmployeesDB (ID, Name, Occupation) VALUES (?, ?, ?); [(4, 'Angela', 'seller'), (5, 'Mark', 'seller')]
Employees: INSERT INTO EmployeesDB (ID, Name, Occupation) VALUES (?, ?, ?); [(6, 'Max', 'seller'), (7, 'Maria', 'seller')]
Employees: Transaction finished
```

---
**Query select with specified columns**

```py
employees_table.select_eq('Occupation', 'CEO', return_columns=['Name', 'ID'])

# Returns
[('John', 0)]
```

```sql
-- Debug output --
Employees: SELECT Name, ID FROM EmployeesDB WHERE Occupation = "CEO";
```

---
**Query select with multiple specified values**

```py
employees_table.select_eq('Occupation', equals=['worker', 'CEO'])

# Returns
[(0, 'John', 'CEO'),
 (1, 'Boris', 'worker'),
 (2, 'George', 'worker'),
 (3, 'Kate', 'worker')]
```

```sql
-- Debug output --
Employees: SELECT * FROM EmployeesDB WHERE Occupation in ("worker", "CEO");
```

---
**Delete Rows**

```py
employees_table.delete_eq('ID', 1)
```

```sql
-- Debug output --
Employees: DELETE FROM EmployeesDB WHERE ID = 1; 
```

---
**Select all**

```py
employees_table.select()

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
Employees: SELECT * FROM EmployeesDB;
```

**Create transaction rows batch generator**

```py
import logging
from sqlengine import sqlgen as sql

logger = logging.getLogger(__name__)

with employees_table.transaction():
    
    query = sql.select(employees_table.tablename, columns=['Name', 'ID'])
    batches = employees_table.fetchall_iterator(query, batch_size=2)
    
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
