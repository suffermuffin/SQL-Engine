# Transactions

Transactions help you to not spam `execute()` -> `commit()` on a database for each of operations which is computationally heavy.

## Transaction per Table

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

Transactions must be called with `with` statement. They allow to use `select.__iter__` and `select.fetchmany_iterator` methods that otherwise are not available.

```py
with table.transaction():
    
    table.create_table()
    table.insert_many(data)
    
    for idx, age in table.select("ID", "Age"):
        assert(isinstance(age, int)) # typing is lacking in returning rows
        table.update.where.eq("ID", idx).then.set("Age", age + 1).execute()
    
    older_homies = table.select.fetchall()

older_homies

# ->
# [
#     (0, 'John', 21), (1, 'Boris', 24), (2, 'George', 35),
#     (3, 'Kate', 19), (4, 'Angela', 41), (5, 'Mark', 29),
#     (6, 'Max', 22), (7, 'Maria', 34)
# ]
```

## Shared Connection

To use multiple databases and tables in the same transaction (spanning either one or multiple databases), you can use `shared_connection` util. `shared_connection` creates connection across one or more databases for multiple tables by manipulating their transaction attributes.

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
    
    for batch in biggest_table.select.fetchmany_iterator(1000):
        copy_biggest_table.insert_many(batch)

    final_shape = copy_biggest_table.shape

final_shape # -> (9, 3503)
```

## Manually Managed Connections

You can use manually manged connections with below syntax:

```py
table.open_connection()
table.insert_many(DATA)

for idx, temp in table.select("ID", "temp"):
    table.update.set("temp", temp*2).where.eq("ID", idx).then.execute()

table.commit()
table.close_connection()
```

Note that you have to use `table.commit()` or `table.rollback()` after the operations. And don't forget to `table.close_connection()` if necessary.

It works fine between multiple tables as well as `shared_connection`. Here an example of coping one table to the other while in transaction:

```py
table_original = schema.table_from_database(CHINOOK_DB, "Customer")
schema_        = table_original.schema

table_copy = schema.table_from_schema(":memory:", schema_)

table_original.open_connection()
table_copy.open_connection()

table_copy.create_table()

for rows in table_original.select.fetchmany_iterator(50):
    table.insert_many(rows)

table_copy.commit()

table_original.close_connection()

...

```

**Note:** This approach might be unpredictable if used within the same database file across multiple tables. In such case a `shared_connection` is suggested, as it uses optimal connection managment.
