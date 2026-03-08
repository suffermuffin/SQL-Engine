# Sql-Engine

My Sql-Engine is a cute little wrapper for `sqlite3` table manipulations without any third party dependencies.


## Features

Abstracts SQL queries into tiny little methods like `select`, `insert`, `delete_rows`. Sql-Engine also provides some bulk insertions methods, which are `insert_many` and `insert_many_transaction`. 


## Installation

To install `sqlengine`, you can use `pip`:

```sh
pip install -e . sqlengine
```

## Usage

All you have to do to create your own cute little table is to inherit `SqlTableMixin` class and declare desired properties of your table's columns. They are:

 - \_\_columns\_\_ 
   - (list[str]) Column names of the table
 - \_\_types\_\_
   - (list[str]) Types of declared columns
 - \_\_primary\_\_
   - (list[str]) List of one or more primary keys
 - \_\_tablename\_\_
   - (str) Name of the table

### Example in code

```py
from sqlengine import SqlTableMixin

class Employees(SqlTableMixin):

    __columns__   : list[str] = ["ID", "Name", "Occupation"]
    __types__     : list[str] = ["INT", "TEXT NOT NULL", "TEXT"]
    __primary__   : list[str] = ["ID"]
    __tablename__ : str = "EmployeesDB"

    def __init__(self, db_filename: str, force_drop: bool = False) -> None:
        super().__init__(db_filename, force_drop)

    # overwrite your insert method for type consistancy
    def insert(self, id : int, name : str, occupation : str) -> None:
        return self._insert_args(id, name, occupation)

```
---
**[IN]**

```py
db_filename = "MyBuisness.db"
# Force table overwrite with `force_drop=True`
employees_table = Employees(db_filename, force_drop=True) 
```

**[Debug output]**
```
DROPPING EmployeesDB TABLE IN 3s
Employees: DROP TABLE IF EXISTS EmployeesDB 
Employees: CREATE TABLE IF NOT EXISTS EmployeesDB (ID INT, Name TEXT NOT NULL, Occupation TEXT, PRIMARY KEY (ID));
```

---
**[IN]**

```py
employees_table.insert(0, 'John', 'CEO')
```

**[Debug output]**
```
Employees: INSERT INTO EmployeesDB (ID, Name, Occupation) VALUES (?, ?, ?); (0, 'John', 'CEO')
```

---
**[IN]**

```py
workers  = [(1, 'Boris', 'worker'), (2, 'George', 'worker'), (3, 'Kate', 'worker')]
employees_table.insert_many(workers)
```

**[Debug output]**
```
Employees: INSERT INTO EmployeesDB (ID, Name, Occupation) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?); [1, 'Boris', 'worker', 2, 'George', 'worker', 3, 'Kate', 'worker']
```

---
**[IN]**

```py
sales = [(4, 'Angela', 'seller'), (5, 'Mark', 'seller')]
employees_table.insert_many_transaction(sales, batch_size=1)
```

**[Debug output]**
```
Employees: inserting 2 rows in 2 batches
```

---
**[IN]**

```py
employees_table.select_eq('Occupation', 'CEO', return_columns=['Name', 'ID'])
```

**[Debug output]**
```
Employees: SELECT Name, ID FROM EmployeesDB WHERE Occupation = "CEO";
```

**[OUT]**

```py
[('John', 0)]
```

---
**[IN]**

```py
employees_table.select_eq('Occupation', ['seller', 'CEO'])
```

**[Debug output]**
```
Employees: SELECT * FROM EmployeesDB WHERE Occupation in ("seller", "CEO");
```

**[OUT]**

```py
[(0, 'John', 'CEO'), (4, 'Angela', 'seller'), (5, 'Mark', 'seller')]
```

---
**[IN]**

```py
employees_table.delete_eq('ID', 1)
employees_table.select()
```

**[Debug output]**
```
Employees: DELETE FROM EmployeesDB WHERE ID = 1; 
Employees: SELECT * FROM EmployeesDB;
```

**[OUT]**

```py
[(0, 'John', 'CEO'),
 (2, 'George', 'worker'),
 (3, 'Kate', 'worker'),
 (4, 'Angela', 'seller'),
 (5, 'Mark', 'seller')]
```
