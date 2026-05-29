# Syntax Sugar

## Single Index

`SqlTableMixin` has `__getitem__` implementation. Usage differs based on `__primary__`. The simplest case uses a single `INTEGER` primary key:

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

You can implicitly retrieve rows by the `ID` column (which is primary in this case):

```py
table[4] # -> (4, 'loc4', Point(4.0, 4.0), 4.4)
```

```sql
-- Debug output --
Coordinates: SELECT * FROM Coordinates WHERE ID = ?; (4,);
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
Coordinates: SELECT * FROM Coordinates WHERE ID BETWEEN ? AND ? ORDER BY ID DESC; (0, 4)
```

## Multi Index

In case of multiple values in `__primary__` you have to call with tuple key in order of declared `__primary__`:

```py
class Employees(SqlTableMixin):

    __tablename__ = "Employees"

    ID       : Primary[int]
    name     : Primary[str]
    surname  : str | None
    salary   : float
    position : str

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
table[3, "test3"] # -> (3, 'test3', 'surname3', 133.3, 'pos3')
```

```sql
-- Debug output --
Employees: SELECT * FROM Employees WHERE ID = ? AND name = ?; (3, 'test3')
```

## Length and Shape

You can get number of rows in your table with `len`:

```py
len(table) # -> 7
```

```sql
-- Debug output --
Employees: SELECT COUNT(*) FROM Employees;
```

Or shape (n_cols, n_rows) in `x, y` fashion.

```py
table.shape # -> (5, 7)
```