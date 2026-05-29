# Table Declaration

All you have to do to create your own cute little table is to [inherit](#class-declaration) `SqlTableMixin` class or to create your own [schema](#schema-declaration) and declare desired properties of your table's columns. They are:


_Name of the table that will be used in queries. If omitted in inherited class declaration, then it will take the class name._
```py
__tablename__ : Optional[str]
```

_Column names of the table_
```py
__columns__ : list[str] 
```

_Column types of the table_
```py
__types__ : list[SqlType | str] 
```

_List of primary keys_
```py
__primary__ : list[str] 

```

## Class Declaration

There are a couple of ways to declare your table. One way is to use the inheritance from `SqlTableMixin` with sql-native types declaration:

```py
from sqlengine import SqlTableMixin

class Employees(SqlTableMixin):

    __tablename__ = "Employees"
    __columns__   = ["ID", "name", "surname", "salary", "position"]
    __types__     = ["INTEGER", "NVARCHAR(160)", "TEXT", "REAL", "TEXT"]
    __primary__   = ["ID", "name"]

```

or with python types. It's also possible to combine with sql-native types strings:

```py
class Employees(SqlTableMixin):

    __tablename__ = "Employees"
    __columns__   = ["ID", "name", "surname", "salary", "position"]
    __types__     = [int, "NVARCHAR(160)", str, float, str]
    __primary__   = ["ID", "name"]

```

## Class Declaration with Annotation

The most convenient way is to use annotations to declare all the columns, primary keys and types.

```py
from sqlengine import SqlTableMixin, Primary

class Employees(SqlTableMixin):

    __tablename__ = "Employees"

    ID       : Primary[int]
    name     : Primary[str]
    surname  : str | None
    salary   : float
    position : str

```

You may combine both of the above ways in a flexible manner, but there are some rules:

 - Dunder attributes are declared first and annotated ones are appended to them. It's important as you will have to insert arguments in the correct order.
 - If you declare same columns and/or primary keys both via annotations and dunders, an AttributeError will be raised.
 - Table must have at least 1 primary column
 - All declared primary keys must be present among the columns in either way

Here are some quick examples of possible combinations:

```py
# If you don't feel like marking each primary column
# with `Primary[T]` annotation, you may still use
# __primary__ dunder attribute

from sqlengine import SqlTableMixin

class Employees(SqlTableMixin):

    __primary__ = ["ID", "name"]

    ID       : int
    name     : str
    surname  : str | None
    salary   : float
    position : str

```

```py
# Here, the "name" column will be used first in
# insert methods unlike the above examples
# where the first one was `ID`

from sqlengine import SqlTableMixin, Primary

class Employees(SqlTableMixin):

    __columns__ = ["name"]
    __types__   = ["NVARCHAR(160)"]
    __primary__ = ["name"]

    ID       : Primary[int]
    surname  : str | None
    salary   : float
    position : str

```

Note that IDE will suggest you your own annotations when using class object, but in reality they do not exist.

## Schema Declaration

The other way to declare a table is to use schemas:

```py
from sqlengine import Schema

employees_schema : Schema = {
    "tablename": "Employees",
    "columns"  : ["ID", "name", "surname", "salary", "position"],
    "types"    : [int, str, str, float, str],
    "primary"  : ["ID", "name"],
}
```


# Table Instantiation

You may instantiate a table via your class or schema. You have to provide a path to the database where the data exists or you wish to create a new table/database. You can also use `:memory:` to complete operations [in memory](https://sqlite.org/inmemorydb.html). You may operate on `:memory:` databases only in [transactions](transactions.md#transactions) because they don't commit to a file.

```py
from sqlengine import schema


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