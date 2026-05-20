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

There are a couple of ways to declare your table.

## Class Declaration

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

## Schema Declaration

The other one is to use schemas:

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

You may instantiate a table via your class or schema. You have to provide a path to the database where the data exists or you wish to create a new table/database. You can also use `:memory:` to complete operations [in memory](https://sqlite.org/inmemorydb.html). You may operate on `:memory:` databases only in [transactions](#transactions) because they don't commit to a file.

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