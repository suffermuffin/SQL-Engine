# Sql-Engine

* [sqlengine.sqltable](#sqlengine.sqltable)
  * [SqlTableMixin](#sqlengine.sqltable.SqlTableMixin)
* [sqlengine.schema](#sqlengine.schema)
  * [get\_database\_tablenames](#sqlengine.schema.get_database_tablenames)
  * [get\_table\_schema](#sqlengine.schema.get_table_schema)
  * [get\_database\_schemas](#sqlengine.schema.get_database_schemas)
  * [table\_from\_schema](#sqlengine.schema.table_from_schema)
  * [table\_from\_database](#sqlengine.schema.table_from_database)
* [sqlengine.utils.connection](#sqlengine.utils.connection)
  * [shared\_connection](#sqlengine.utils.connection.shared_connection)
* [sqlengine.utils.convert](#sqlengine.utils.convert)
  * [to\_csv](#sqlengine.utils.convert.to_csv)
  * [to\_dicts](#sqlengine.utils.convert.to_dicts)
  * [to\_dicts\_stream](#sqlengine.utils.convert.to_dicts_stream)
* [sqlengine.\_internal.statements](#sqlengine._internal.statements)
  * [Where](#sqlengine._internal.statements.Where)
  * [Statement](#sqlengine._internal.statements.Statement)
  * [MutationalStatement](#sqlengine._internal.statements.MutationalStatement)
  * [Select](#sqlengine._internal.statements.Select)
  * [Update](#sqlengine._internal.statements.Update)
* [sqlengine.\_internal.connection\_manager](#sqlengine._internal.connection_manager)
  * [ConnectionManager](#sqlengine._internal.connection_manager.ConnectionManager)

<a id="sqlengine.sqltable"></a>

# sqlengine.sqltable

<a id="sqlengine.sqltable.SqlTableMixin"></a>

## SqlTableMixin Objects

```python
class SqlTableMixin()
```

Lightweight wrapper for SQLite3 tables

**Arguments**:

- `database` _str_ - database filename to connect to. If it does not exists - will create new one first.
  If `":memory:"` is passed, then database will be set in memory and you will have to
  create table manually with `create_table()` method inside `transaction()` block.
- `force_drop` _bool_ - If `True` - will drop existing table.
- `**connection_params` - Params to create connection with. Reference: https://docs.python.org/3/library/sqlite3.html#sqlite3.connect
  

**Attributes**:

- `__tablename__` _Optional[str]_ - Name of the table that will be used in queries.
  If omitted in inherited class declaration, then it will take the class name.
- `__columns__` _list[str]_ - Column names of the table
- `__types__` _list[ColumnType]_ - Column types of the table
- `__primary__` _list[str]_ - List of primary keys
  

**Example**:

```python
from sqlengine import SqlTableMixin, Primary

class Employees(SqlTableMixin):
    ID       : Primary[int]
    name     : Primary[str]
    surname  : str   | None
    salary   : float | None
    position : str

table = Employees("mydb.sqlite3")
```

<a id="sqlengine.sqltable.SqlTableMixin.create_table"></a>

#### create\_table

```python
def create_table() -> None
```

Create table if not exists

<a id="sqlengine.sqltable.SqlTableMixin.drop_table"></a>

#### drop\_table

```python
def drop_table(confirm: bool = False) -> None
```

Drops table if it exists.

<a id="sqlengine.sqltable.SqlTableMixin.transaction"></a>

#### transaction

```python
def transaction(autocommit: bool = True)
```

Creates context manager to use class methods in transaction

**Arguments**:

- `autocommit` _bool_ - If `True`, will commit changes at the end of transaction
  

**Example**:

  
```python
with table.transaction():
    for idx, age in table.select("ID", "Age"):
        table.update("Age", age + 1).where.eq("ID", idx).then.execute()
```

<a id="sqlengine.sqltable.SqlTableMixin.insert"></a>

#### insert

```python
def insert(*args, **kwargs) -> None
```

Insert single row

**Arguments**:

- `*args` _SqlValue_ - Arguments in order of declared __columns__
- `**kwargs` _SqlValue_ - Column to value mapping
  

**Example**:

  
```python
table = MyTable("mydb.db")
table.columns # -> ["ID", "Name", "Age"]
table.insert(0, "Daniel", 27)
table.insert(ID=1, name="Boris", age=26)
```

<a id="sqlengine.sqltable.SqlTableMixin.upsert"></a>

#### upsert

```python
def upsert(*args, **kwargs) -> None
```

Upsert (update or insert) single row, resolving conflicts
via the declared `primary` key

**Arguments**:

- `**args` _SqlValue_ - Arguments in order of declared __columns__
- `**kwargs` _SqlValue_ - Column to value mapping
  

**Example**:

  
```python
table = MyTable("mydb.db")
table.columns # -> ["ID", "Name", "Age"]
table.upsert(0, "Daniel", 27)
table.upsert(ID=0, age=21)
```

<a id="sqlengine.sqltable.SqlTableMixin.insert_many"></a>

#### insert\_many

```python
def insert_many(rows: Sequence[SqlRow]) -> None
```

Bulk insert multiple rows

**Arguments**:

- `rows` _list[SqlRow]_ - List of tuples, each tuple contains
  values for one row in the order of __columns__

<a id="sqlengine.sqltable.SqlTableMixin.head"></a>

#### head

```python
def head(n: int = 5) -> list[SqlRow]
```

Returns first `n` rows unordered

<a id="sqlengine.sqltable.SqlTableMixin.in_transaction"></a>

#### in\_transaction

```python
def in_transaction() -> bool
```

Returns True if instance is in transaction

<a id="sqlengine.sqltable.SqlTableMixin.open_connection"></a>

#### open\_connection

```python
def open_connection() -> None
```

Opens unmanaged transaction

<a id="sqlengine.sqltable.SqlTableMixin.close_connection"></a>

#### close\_connection

```python
def close_connection() -> None
```

Closes unmanaged transaction

<a id="sqlengine.sqltable.SqlTableMixin.commit"></a>

#### commit

```python
def commit() -> None
```

Commit current transaction

<a id="sqlengine.sqltable.SqlTableMixin.rollback"></a>

#### rollback

```python
def rollback() -> None
```

Rollback current transaction

<a id="sqlengine.sqltable.SqlTableMixin.__getitem__"></a>

#### \_\_getitem\_\_

```python
def __getitem__(
        key: tuple[SqlValue, ...] | SqlValue | slice) -> SqlRow | list[SqlRow]
```

Get row by primary key

<a id="sqlengine.sqltable.SqlTableMixin.update"></a>

#### update

```python
@property
def update() -> Update
```

UPDATE statement builder and executor

**Example**:

  
```python
table.update.set("City", "Karaganda").where.eq("Country", "Czech Republic").then.execute()
```

<a id="sqlengine.sqltable.SqlTableMixin.delete"></a>

#### delete

```python
@property
def delete() -> Delete
```

DELETE statement builder and executor

**Example**:

  
```python
table.delete.where.eq("ID", 0).then.execute()
```

<a id="sqlengine.sqltable.SqlTableMixin.select"></a>

#### select

```python
@property
def select() -> Select
```

SELECT statement builder and fetcher

**Example**:

  
```python
table.select("Email").where.eq("SupportRepId", 3).then.aggregate("COUNT").fetchone()
```

<a id="sqlengine.sqltable.SqlTableMixin.columns"></a>

#### columns

```python
@property
def columns() -> list[str]
```

List of table column names

<a id="sqlengine.sqltable.SqlTableMixin.types"></a>

#### types

```python
@property
def types() -> list[ColumnType]
```

List of table column dtypes as declared

<a id="sqlengine.sqltable.SqlTableMixin.types_sql"></a>

#### types\_sql

```python
@property
def types_sql() -> list[str]
```

List of table column dtypes converted to SQL native and registered types

<a id="sqlengine.sqltable.SqlTableMixin.primary"></a>

#### primary

```python
@property
def primary() -> list[str]
```

List of table column primary keys

<a id="sqlengine.sqltable.SqlTableMixin.tablename"></a>

#### tablename

```python
@property
def tablename() -> str
```

Name of the table

<a id="sqlengine.sqltable.SqlTableMixin.shape"></a>

#### shape

```python
@property
def shape() -> tuple[int, int]
```

Table shape (n_cols, n_rows)

<a id="sqlengine.sqltable.SqlTableMixin.schema"></a>

#### schema

```python
@property
def schema() -> Schema
```

Table schema

<a id="sqlengine.sqltable.SqlTableMixin.conn"></a>

#### conn

```python
@property
def conn() -> ConnectionManager
```

Access connection manager instance

<a id="sqlengine.sqltable.SqlTableMixin.tx_conn"></a>

#### tx\_conn

```python
@property
def tx_conn() -> sqlite3.Connection
```

Access connection while in transaction

<a id="sqlengine.sqltable.SqlTableMixin.tx_cursor"></a>

#### tx\_cursor

```python
@property
def tx_cursor() -> sqlite3.Cursor
```

Access cursor while in transaction

<a id="sqlengine.sqltable.SqlTableMixin.connection_params"></a>

#### connection\_params

```python
@property
def connection_params() -> dict[str, Any]
```

Connection parameters used in connection creation

<a id="sqlengine.schema"></a>

# sqlengine.schema

<a id="sqlengine.schema.get_database_tablenames"></a>

#### get\_database\_tablenames

```python
def get_database_tablenames(database: str,
                            cursor: sqlite3.Cursor | None = None) -> list[str]
```

Returns all table names of provided `database` path

<a id="sqlengine.schema.get_table_schema"></a>

#### get\_table\_schema

```python
def get_table_schema(database: str,
                     tablename: str,
                     cursor: sqlite3.Cursor | None = None) -> Schema | None
```

Constructs `Schema` from provided `database` and `tablename` if this table exists

<a id="sqlengine.schema.get_database_schemas"></a>

#### get\_database\_schemas

```python
def get_database_schemas(database: str) -> list[Schema]
```

Reads provided `database` path and outputs gathered schemas of tables inside it

<a id="sqlengine.schema.table_from_schema"></a>

#### table\_from\_schema

```python
def table_from_schema(database: str, schema: Schema,
                      **kwargs) -> SqlTableMixin
```

Dynamically builds class from provided schema

<a id="sqlengine.schema.table_from_database"></a>

#### table\_from\_database

```python
def table_from_database(database: str,
                        tablename: str | None = None,
                        **kwargs) -> list[SqlTableMixin] | SqlTableMixin
```

Dynamically builds table class(es) from provided database

<a id="sqlengine.utils.connection"></a>

# sqlengine.utils.connection

<a id="sqlengine.utils.connection.shared_connection"></a>

#### shared\_connection

```python
@contextmanager
def shared_connection(*args: SqlTableMixin,
                      autocommit: bool = True,
                      **connection_params)
```

Creates shared connection across one or more databases for multiple tables by
manipulating their transaction attributes

**Arguments**:

- `*args` _SqlTableMixin_ - tuple of instances of table classes inherited from `SqlTableMixin`
- `autocommit` _bool_ - If `True`, will commit changes at the end of transaction
- `**connection_params` _dict_ - Params to create connections with. This argument will be shared
  across different connections. Reference: https://docs.python.org/3/library/sqlite3.html#sqlite3.connect
  

**Example**:

  
```python
from sqlengine.utils import shared_connection
with shared_connection(table1, table2, **table1.connection_params):
    for (id1,), (id2, temp) in zip(table1.select("ID").limit(20), table2.select("ID", "Temperature").limit(20)):
        if id1 == id2:
            table.update.where.eq("ID", id2).then.set("Salary", temp).execute()
```

<a id="sqlengine.utils.convert"></a>

# sqlengine.utils.convert

<a id="sqlengine.utils.convert.to_csv"></a>

#### to\_csv

```python
def to_csv(builder: Select | Where[Select] | SqlTableMixin,
           filename: str,
           stream_batch_size: int | None = None) -> None
```

Writes query or whole table to a csv file

**Arguments**:

- `builder` _Select | Where[Select] | SqlTableMixin_ - Object to convert to csv
- `filename` _str_ - Path to write to
- `stream_batch_size` _int | None_ - If not None or 0, will stream all rows to csv in batches of provided size

<a id="sqlengine.utils.convert.to_dicts"></a>

#### to\_dicts

```python
def to_dicts(
    builder: Select | Where[Select] | SqlTableMixin
) -> list[dict[str, SqlValue]]
```

Converts query or whole table to pandas friendly format

**Arguments**:

- `builder` _Select | Where[Select] | SqlTableMixin_ - Object to convert to list of dicts
  

**Returns**:

- `out` _list[dict[str, SqlValue]]_ - list of rows mappings
  

**Example**:

  
```python
import pandas as pd

df = pd.DataFrame(to_dicts(table))
```

<a id="sqlengine.utils.convert.to_dicts_stream"></a>

#### to\_dicts\_stream

```python
def to_dicts_stream(
        builder: Select | Where[Select] | SqlTableMixin,
        batch_size: int) -> Generator[list[dict[str, SqlValue]], None, None]
```

Converts query or whole table to pandas friendly format and yields it in batches

**Arguments**:

- `builder` _Select | Where[Select] | SqlTableMixin_ - Object to convert to list of dicts
- `batch_size` _int_ - Size of each yielded batch
  

**Yields**:

- `batch` _list[dict[str, SqlValue]]_ - list of rows mappings
  

**Example**:

  
```python
import pandas as pd

df = pd.DataFrame(columns=table.columns)

with table.transaction():
    for batch in to_dicts_stream(table, 100):
        df = pd.concat([df, pd.DataFrame(batch)], axis=0)

df.set_index("ID", inplace=True)
```

<a id="sqlengine._internal.statements"></a>

# sqlengine.\_internal.statements

<a id="sqlengine._internal.statements.Where"></a>

## Where Objects

```python
class Where()
```

Where clause build helper

<a id="sqlengine._internal.statements.Where.then"></a>

#### then

```python
@property
def then() -> T
```

Returns upper statement object

<a id="sqlengine._internal.statements.Where.__call__"></a>

#### \_\_call\_\_

```python
def __call__(where_clasuse: str, *args: SqlValue) -> Self
```

Shortcut to custom where clause

<a id="sqlengine._internal.statements.Where.op"></a>

#### op

```python
def op(column: str, value: SqlValue, operator: str) -> Self
```

Adds operator to the where clause

<a id="sqlengine._internal.statements.Where.join"></a>

#### join

```python
def join(lop: str = "AND") -> Self
```

Join previous expression via logical operator `lop`

<a id="sqlengine._internal.statements.Where.eq"></a>

#### eq

```python
def eq(column: str, value: SqlValue) -> Self
```

Add `column = value` to the where clause

<a id="sqlengine._internal.statements.Where.neq"></a>

#### neq

```python
def neq(column: str, value: SqlValue) -> Self
```

Add `column != value` to the where clause

<a id="sqlengine._internal.statements.Where.gt"></a>

#### gt

```python
def gt(column: str, value: SqlValue) -> Self
```

Add `column > value` to the where clause

<a id="sqlengine._internal.statements.Where.gte"></a>

#### gte

```python
def gte(column: str, value: SqlValue) -> Self
```

Add `column >= value` to the where clause

<a id="sqlengine._internal.statements.Where.lt"></a>

#### lt

```python
def lt(column: str, value: SqlValue) -> Self
```

Add `column < value` to the where clause

<a id="sqlengine._internal.statements.Where.lte"></a>

#### lte

```python
def lte(column: str, value: SqlValue) -> Self
```

Add `column <= value` to the where clause

<a id="sqlengine._internal.statements.Where.like"></a>

#### like

```python
def like(column: str, pattern: str) -> Self
```

Like operator. Pattern is a SQL wildcard pattern 
(i.e. `%` for any string, `_` for one character).

<a id="sqlengine._internal.statements.Where.is_null"></a>

#### is\_null

```python
def is_null(column: str) -> Self
```

Add `column IS NULL` to the where clause

<a id="sqlengine._internal.statements.Where.inverted"></a>

#### inverted

```python
def inverted() -> Self
```

Invert previous where clauses with `NOT`

<a id="sqlengine._internal.statements.Where.custom"></a>

#### custom

```python
def custom(where_clause: str, *args: SqlValue) -> Self
```

Add custom where clause (e.g. `where.custom("Age > ? AND Age != ?", 10, 25)`)

<a id="sqlengine._internal.statements.Statement"></a>

## Statement Objects

```python
class Statement(ABC)
```

Statement object that helps you build queries and execute them

<a id="sqlengine._internal.statements.Statement.custom_query"></a>

#### custom\_query

```python
def custom_query(query: str, *args) -> Self
```

Custom query that completely replaces builder's expression

<a id="sqlengine._internal.statements.Statement.build"></a>

#### build

```python
def build() -> tuple[str, tuple[SqlValue, ...]]
```

Build complete expression with sorted arguments and operations

<a id="sqlengine._internal.statements.Statement.reset"></a>

#### reset

```python
def reset() -> None
```

Reset statement to reuse the object

<a id="sqlengine._internal.statements.Statement.where"></a>

#### where

```python
@property
def where() -> Where[Self]
```

Where clause builder

<a id="sqlengine._internal.statements.MutationalStatement"></a>

## MutationalStatement Objects

```python
class MutationalStatement(Statement, ABC)
```

<a id="sqlengine._internal.statements.MutationalStatement.execute"></a>

#### execute

```python
def execute() -> None
```

Execute built statement

<a id="sqlengine._internal.statements.Select"></a>

## Select Objects

```python
class Select(Statement)
```

<a id="sqlengine._internal.statements.Select.__call__"></a>

#### \_\_call\_\_

```python
def __call__(*columns: str) -> Self
```

Shortcut to columns selector

<a id="sqlengine._internal.statements.Select.columns"></a>

#### columns

```python
def columns(*columns: str) -> Self
```

Columns selector

<a id="sqlengine._internal.statements.Select.aggregate"></a>

#### aggregate

```python
def aggregate(by: Literal['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']) -> Self
```

Aggregate by provided method

<a id="sqlengine._internal.statements.Select.order_by"></a>

#### order\_by

```python
def order_by(column: str, ascending: bool = True) -> Self
```

Orders returned rows by provided column

<a id="sqlengine._internal.statements.Select.limit"></a>

#### limit

```python
def limit(n: int) -> Self
```

Limit number of returned rows

<a id="sqlengine._internal.statements.Select.fetchone"></a>

#### fetchone

```python
def fetchone() -> SqlRow
```

Fetch first row

<a id="sqlengine._internal.statements.Select.fetchmany"></a>

#### fetchmany

```python
def fetchmany(size: int = 1) -> list[SqlRow]
```

Fetch first `size` rows

<a id="sqlengine._internal.statements.Select.fetchall"></a>

#### fetchall

```python
def fetchall() -> list[SqlRow]
```

Fetch all rows

<a id="sqlengine._internal.statements.Select.fetchmany_iterator"></a>

#### fetchmany\_iterator

```python
def fetchmany_iterator(batch_size: int) -> Generator[list[SqlRow], None, None]
```

Yields all rows in batches within a single transaction.

**Arguments**:

- `batch_size` _int_ - Size of each batch
  

**Example**:

  
```python
with table.transaction():
    for batch in table.select.where.gt("Age", 30).then.fetchmany_iterator(1000):
        process_batch(batch)
```

<a id="sqlengine._internal.statements.Select.__iter__"></a>

#### \_\_iter\_\_

```python
def __iter__() -> Generator[SqlRow, None, None]
```

Select statement rows iterator

**Example**:

  
```python
with table.transaction():
    # here `then` is used to link back to the `select` instance from `where` object
    for row in table.select.where.gt("Age", 30).then: 
        process_row(row)
```

<a id="sqlengine._internal.statements.Update"></a>

## Update Objects

```python
class Update(MutationalStatement)
```

<a id="sqlengine._internal.statements.Update.__call__"></a>

#### \_\_call\_\_

```python
def __call__(column: str, value: SqlValue) -> Self
```

Shortcut to set value to a column

<a id="sqlengine._internal.statements.Update.set"></a>

#### set

```python
def set(column: str, value: SqlValue) -> Self
```

Set value to a column

<a id="sqlengine._internal.connection_manager"></a>

# sqlengine.\_internal.connection\_manager

<a id="sqlengine._internal.connection_manager.ConnectionManager"></a>

## ConnectionManager Objects

```python
class ConnectionManager()
```

Connection manager for sqlite3

**Arguments**:

  
- `database` _str_ - database filename to connect to. If `":memory:"` is passed, then database will be set in memory.
- `**connection_params` - Params to create connection with. Reference: https://docs.python.org/3/library/sqlite3.html#sqlite3.connect

<a id="sqlengine._internal.connection_manager.ConnectionManager.execute"></a>

#### execute

```python
def execute(query: str, *args: SqlValue) -> None
```

Shortcut to connect() -> execute() -> commit() for single operations.
Can be used in transaction using `transaction()` manager.

**Arguments**:

- `query` _str_ - SQL query to execute on SQLite3 DB
- `*args` _tuple[SqlValue, ...]_ - Arguments to the execution

<a id="sqlengine._internal.connection_manager.ConnectionManager.executemany"></a>

#### executemany

```python
def executemany(query: str, args: Sequence[SqlRow]) -> None
```

Shortcut to connect() -> executemany() -> commit() for single operations.
Can be used in transaction using `transaction()` manager.

**Arguments**:

- `query` _str_ - SQL query to execute on SQLite3 DB
- `args` _list[tuple[SqlValue, ...]]_ - Arguments to the execution

<a id="sqlengine._internal.connection_manager.ConnectionManager.fetchone"></a>

#### fetchone

```python
def fetchone(query: str, *args: SqlValue) -> SqlRow
```

Fetch first row based on `query`

**Arguments**:

- `query` _str_ - SQL query
- `*args` _tuple[SqlValue, ...]_ - Arguments to the execution
  

**Returns**:

- `row` _SqlRow_ - Single row

<a id="sqlengine._internal.connection_manager.ConnectionManager.fetchmany"></a>

#### fetchmany

```python
def fetchmany(query: str, *args: SqlValue, size: int = 1) -> list[SqlRow]
```

Fetch first `size` rows based on `query`

**Arguments**:

- `query` _str_ - SQL query
- `*args` _tuple[SqlValue, ...]_ - Arguments to the execution
- `size` _int_ - Number of rows to return
  

**Returns**:

- `rows` _list[SqlRow]_ - list of `size` rows

<a id="sqlengine._internal.connection_manager.ConnectionManager.fetchall"></a>

#### fetchall

```python
def fetchall(query: str, *args: SqlValue) -> list[SqlRow]
```

Fetch all rows based on `query`

**Arguments**:

- `query` _str_ - SQL query
- `*args` _tuple[SqlValue, ...]_ - Arguments to the execution
  

**Returns**:

- `rows` _list[SqlRow]_ - list of rows

<a id="sqlengine._internal.connection_manager.ConnectionManager.in_transaction"></a>

#### in\_transaction

```python
def in_transaction() -> bool
```

Returns True if instance is in transaction

<a id="sqlengine._internal.connection_manager.ConnectionManager.connect"></a>

#### connect

```python
def connect() -> sqlite3.Connection
```

Shortcut to sqlite3 connection context manager

<a id="sqlengine._internal.connection_manager.ConnectionManager.open"></a>

#### open

```python
def open() -> None
```

Opens unmanaged transaction

<a id="sqlengine._internal.connection_manager.ConnectionManager.close"></a>

#### close

```python
def close() -> None
```

Closes unmanaged transaction

<a id="sqlengine._internal.connection_manager.ConnectionManager.transaction"></a>

#### transaction

```python
@contextmanager
def transaction(autocommit: bool = True)
```

Creates context manager to use class methods in transaction

**Arguments**:

- `autocommit` _bool_ - If `True`, will commit changes at the end of transaction

<a id="sqlengine._internal.connection_manager.ConnectionManager.tx_conn"></a>

#### tx\_conn

```python
@property
def tx_conn() -> sqlite3.Connection
```

Gives access to connection while in transaction

<a id="sqlengine._internal.connection_manager.ConnectionManager.tx_cursor"></a>

#### tx\_cursor

```python
@property
def tx_cursor() -> sqlite3.Cursor
```

Gives access to connection cursor while in transaction

