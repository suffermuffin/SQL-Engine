

# Examples

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
## Instantiate

```py
# Force table overwrite with `force_drop=True`
table = Employees("MyBusiness.db", force_drop=True)
```

```sql
-- Debug output --
EmployeesDB: DROP TABLE IF EXISTS EmployeesDB 
EmployeesDB: CREATE TABLE IF NOT EXISTS EmployeesDB (ID INT, Name TEXT NOT NULL, Occupation TEXT, PRIMARY KEY (ID));
```

---
## Insert single row

```py
table.insert(0, 'John', 'CEO')
```

```sql
-- Debug output --
EmployeesDB: INSERT INTO EmployeesDB (ID, Name, Occupation) VALUES (?, ?, ?); (0, 'John', 'CEO')
```

---
## Insert many rows

```py
workers  = [(1, 'Boris', 'worker'), (2, 'George', 'worker'), (3, 'Kate', 'worker')]
table.insert_many(workers)
```

```sql
-- Debug output --
EmployeesDB: INSERT INTO EmployeesDB (ID, Name, Occupation) VALUES (?, ?, ?); [(1, 'Boris', 'worker'), (2, 'George', 'worker'), (3, 'Kate', 'worker')]
```

---
## Insert many rows in transaction

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
EmployeesDB: Transaction started
EmployeesDB: INSERT INTO EmployeesDB (ID, Name, Occupation) VALUES (?, ?, ?); [(4, 'Angela', 'seller'), (5, 'Mark', 'seller')]
EmployeesDB: INSERT INTO EmployeesDB (ID, Name, Occupation) VALUES (?, ?, ?); [(6, 'Max', 'seller'), (7, 'Maria', 'seller')]
EmployeesDB: Transaction finished
```

---
## Query select with specified columns

```py
table.select('Name', 'ID').where.eq('Occupation', 'CEO').then.fetchone()

# Returns
('John', 0)
```

```sql
-- Debug output --
EmployeesDB: SELECT Name, ID FROM EmployeesDB WHERE Occupation = ?; ('CEO',)
```

---
## Query select with multiple where clauses

```py
table.select.where.eq('Occupation', 'worker').eq('Occupation', 'CEO').join("OR").then.fetchall()

# Returns
[(0, 'John', 'CEO'),
 (1, 'Boris', 'worker'),
 (2, 'George', 'worker'),
 (3, 'Kate', 'worker')]
```

```sql
-- Debug output --
EmployeesDB: SELECT * FROM EmployeesDB WHERE (Occupation = ? OR Occupation = ?); ('worker', 'CEO')

```
---

## Select, compare, order and limit
```py
table.select.where.in_('Occupation', ('seller', 'worker')).then.order_by("ID", ascending=False).limit(5).fetchall()

# Returns
[(7, 'Maria', 'seller'),
 (6, 'Max', 'seller'),
 (5, 'Mark', 'seller'),
 (4, 'Angela', 'seller'),
 (3, 'Kate', 'worker')]
```

```sql
-- Debug output --
EmployeesDB: SELECT * FROM EmployeesDB WHERE Occupation IN (?, ?) ORDER BY ID DESC LIMIT ?; ('seller', 'worker', 5)
```

---

## Aggregate

```py
table.select.where.eq("Occupation", "worker").then.aggregate("COUNT").fetchone()

# -> (3,)
```

```sql
-- Debug output --
EmployeesDB: SELECT COUNT(*) FROM EmployeesDB WHERE Occupation = ?; ('worker',)
```

---
## Delete rows

```py
table.delete.where.eq('ID', 1).then.execute()
```

```sql
-- Debug output --
EmployeesDB: DELETE FROM EmployeesDB WHERE ID = ?; (1,)
```

---
## Select all

```py
table.select.fetchall()

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
EmployeesDB: SELECT * FROM EmployeesDB; ()
```
---
## Create transaction rows batch generator

```py
import logging

logger = logging.getLogger(__name__)

with table.transaction():
    
    batches = table.select('Name', 'ID').fetchmany_iterator(batch_size=2)
    
    for i, batch in enumerate(batches):
        logger.debug(f"batch {i}: {batch}")
```

```sql
-- Debug output --
EmployeesDB: Transaction started
batch 0: [('John', 0), ('George', 2)]
batch 1: [('Kate', 3), ('Angela', 4)]
batch 2: [('Mark', 5), ('Max', 6)]
batch 3: [('Maria', 7)]
EmployeesDB: Transaction finished
```
---

## Iterate over multiple tables

```py

from sqlengine.utils import shared_connection
from sqlengine import SqlTableMixin


class Employees(SqlTableMixin):

    __columns__ = ["ID", "Salary"]
    __types__   = [int, float]
    __primary__ = ["ID"]


class Temp(SqlTableMixin):

    __columns__ = ["ID", "Temperature"]
    __types__   = [int, float]
    __primary__ = ["ID"]


table1 = Employees("temp/data1.db", True)
table2 = Temp("temp/data2.db", True)

table1.insert_many([(1, 25000), (2, 30000), (3, 45000)])
table2.insert_many([(1, 2.5), (2, 30), (3, 12.5)])


with shared_connection(table1, table2, **table1.connection_params):
    for (id1,), (id2, temp) in zip(table1.select("ID").limit(20), table2.select("ID", "Temperature").limit(20)):
        if id1 == id2:
            table1.update.where.eq("ID", id2).then.set("Salary", temp).execute()

table1.select.fetchall() # -> [(1, 2.5), (2, 30.0), (3, 12.5)]
```

```sql
-- Debug output --
Starting shared transaction across 2 databases
Employees: UPDATE Employees SET Salary = ? WHERE ID = ?; (2.5, 1)
Employees: UPDATE Employees SET Salary = ? WHERE ID = ?; (30.0, 2)
Employees: UPDATE Employees SET Salary = ? WHERE ID = ?; (12.5, 3)
Shared transaction finished
Employees: SELECT * FROM Employees; ()
```
