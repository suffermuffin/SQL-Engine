- [1. Sql-Engine](#1-sql-engine)
  - [1.1. Features](#11-features)
  - [1.2. Purpose](#12-purpose)
  - [1.3. Installation](#13-installation)
  - [1.4. Env](#14-env)
- [2. Usage](#2-usage)
- [3. Table Declaration](#3-table-declaration)
  - [3.1. Class Declaration](#31-class-declaration)
  - [3.2. Schema Declaration](#32-schema-declaration)
- [4. Table Instantiation](#4-table-instantiation)
- [5. Statements](#5-statements)
  - [5.1. Where](#51-where)
    - [5.1.1. `join(lop : str)` operator](#511-joinlop--str-operator)
    - [5.1.2. `build()` method](#512-build-method)
    - [5.1.3. `then` property](#513-then-property)
  - [5.2. Select](#52-select)
    - [5.2.1. `limit(n : int)` op](#521-limitn--int-op)
    - [5.2.2. `order_by(column : str, ascending : bool = True)` op](#522-order_bycolumn--str-ascending--bool--true-op)
    - [5.2.3. `aggregate(by : Literal['COUNT', 'SUM', 'AVG', 'MIN', 'MAX'])` op](#523-aggregateby--literalcount-sum-avg-min-max-op)
    - [5.2.4. `fetch()` methods](#524-fetch-methods)
    - [5.2.5. `__iter__` methods](#525-__iter__-methods)
  - [5.3. Update](#53-update)
    - [5.3.1. `set(column : str, value : SqlValue)` op](#531-setcolumn--str-value--sqlvalue-op)
    - [5.3.2. `execute()` method](#532-execute-method)
  - [5.4. Delete](#54-delete)
    - [5.4.1. `execute()` method](#541-execute-method)
- [6. Custom Types](#6-custom-types)
- [7. Transactions](#7-transactions)
  - [7.1. Transaction per Table](#71-transaction-per-table)
  - [7.2. Shared Connection](#72-shared-connection)
- [8. Syntax Sugar](#8-syntax-sugar)
  - [8.1. Single Index](#81-single-index)
  - [8.2. Multi Index](#82-multi-index)
  - [8.3. Length and Shape](#83-length-and-shape)
- [9. More Examples](#9-more-examples)
  - [9.1. Instantiate](#91-instantiate)
  - [9.2. Insert single row](#92-insert-single-row)
  - [9.3. Insert many rows](#93-insert-many-rows)
  - [9.4. Insert many rows in transaction](#94-insert-many-rows-in-transaction)
  - [9.5. Query select with specified columns](#95-query-select-with-specified-columns)
  - [9.6. Query select with multiple where clauses](#96-query-select-with-multiple-where-clauses)
  - [9.7. Select, compare, order and limit](#97-select-compare-order-and-limit)
  - [9.8. Aggregate](#98-aggregate)
  - [9.9. Delete rows](#99-delete-rows)
  - [9.10. Select all](#910-select-all)
  - [9.11. Create transaction rows batch generator](#911-create-transaction-rows-batch-generator)
  - [9.12. Iterate over multiple tables](#912-iterate-over-multiple-tables)


# 1. Sql-Engine

My Sql-Engine is a cute little wrapper for `sqlite3` table manipulations without any third party dependencies **(vibe-code free!)**


## 1.1. Features

Abstracts SQL queries into tiny little methods like, `insert`, `insert_many`, `upsert`, and not so little and tiny query builders a-la `select`, `delete`, `update`, etc. Sql-Engine also provides bulk insertion and transaction methods, like `insert_many` and `select.fetchmany_iterator`. Methods can be executed in transaction mode thanks to `transaction` context manager.


Sql-Engine implements Jupyter integration and dynamic schema building. You can easily instantiate existing database table and view it in cute little html representation.

```py
from sqlengine import schema

table = schema.table_from_database("temp/chinook.db", "Invoice")
table
```

<table style="border-collapse: collapse; font-size: 14px;"><caption style="font-size: 18px; font-weight: bold;">Invoice</caption><thead><tr><td style="border: 1px solid #555; text-align: center;">InvoiceId</td><td style="border: 1px solid #555; text-align: center;">CustomerId</td><td style="border: 1px solid #555; text-align: center;">InvoiceDate</td><td style="border: 1px solid #555; text-align: center;">BillingAddress</td><td style="border: 1px solid #555; text-align: center;">BillingCity</td><td style="border: 1px solid #555; text-align: center;">BillingState</td><td style="border: 1px solid #555; text-align: center;">BillingCountry</td><td style="border: 1px solid #555; text-align: center;">BillingPostalCode</td><td style="border: 1px solid #555; text-align: center;">Total</td></tr></thead><tbody><tr><td style="border: 1px solid #000; text-align: center;">1</td><td style="border: 1px solid #000; text-align: center;">2</td><td style="border: 1px solid #000; text-align: center;">2021-01-01 00:00:00</td><td style="border: 1px solid #000; text-align: center;">Theodor-Heuss-Straße 34</td><td style="border: 1px solid #000; text-align: center;">Stuttgart</td><td style="border: 1px solid #000; text-align: center;">None</td><td style="border: 1px solid #000; text-align: center;">Germany</td><td style="border: 1px solid #000; text-align: center;">70174</td><td style="border: 1px solid #000; text-align: center;">1.98</td></tr><tr><td style="border: 1px solid #000; text-align: center;">2</td><td style="border: 1px solid #000; text-align: center;">4</td><td style="border: 1px solid #000; text-align: center;">2021-01-02 00:00:00</td><td style="border: 1px solid #000; text-align: center;">Ullevålsveien 14</td><td style="border: 1px solid #000; text-align: center;">Oslo</td><td style="border: 1px solid #000; text-align: center;">None</td><td style="border: 1px solid #000; text-align: center;">Norway</td><td style="border: 1px solid #000; text-align: center;">0171</td><td style="border: 1px solid #000; text-align: center;">3.96</td></tr><tr><td style="border: 1px solid #000; text-align: center;">3</td><td style="border: 1px solid #000; text-align: center;">8</td><td style="border: 1px solid #000; text-align: center;">2021-01-03 00:00:00</td><td style="border: 1px solid #000; text-align: center;">Grétrystraat 63</td><td style="border: 1px solid #000; text-align: center;">Brussels</td><td style="border: 1px solid #000; text-align: center;">None</td><td style="border: 1px solid #000; text-align: center;">Belgium</td><td style="border: 1px solid #000; text-align: center;">1000</td><td style="border: 1px solid #000; text-align: center;">5.94</td></tr><tr><td style="border: 1px solid #000; text-align: center;">4</td><td style="border: 1px solid #000; text-align: center;">14</td><td style="border: 1px solid #000; text-align: center;">2021-01-06 00:00:00</td><td style="border: 1px solid #000; text-align: center;">8210 111 ST NW</td><td style="border: 1px solid #000; text-align: center;">Edmonton</td><td style="border: 1px solid #000; text-align: center;">AB</td><td style="border: 1px solid #000; text-align: center;">Canada</td><td style="border: 1px solid #000; text-align: center;">T6G 2C7</td><td style="border: 1px solid #000; text-align: center;">8.91</td></tr><tr><td style="border: 1px solid #000; text-align: center;">5</td><td style="border: 1px solid #000; text-align: center;">23</td><td style="border: 1px solid #000; text-align: center;">2021-01-11 00:00:00</td><td style="border: 1px solid #000; text-align: center;">69 Salem Street</td><td style="border: 1px solid #000; text-align: center;">Boston</td><td style="border: 1px solid #000; text-align: center;">MA</td><td style="border: 1px solid #000; text-align: center;">USA</td><td style="border: 1px solid #000; text-align: center;">2113</td><td style="border: 1px solid #000; text-align: center;">13.86</td></tr><tr><td style="border: 1px solid #000; text-align: center;">6</td><td style="border: 1px solid #000; text-align: center;">37</td><td style="border: 1px solid #000; text-align: center;">2021-01-19 00:00:00</td><td style="border: 1px solid #000; text-align: center;">Berger Straße 10</td><td style="border: 1px solid #000; text-align: center;">Frankfurt</td><td style="border: 1px solid #000; text-align: center;">None</td><td style="border: 1px solid #000; text-align: center;">Germany</td><td style="border: 1px solid #000; text-align: center;">60316</td><td style="border: 1px solid #000; text-align: center;">0.99</td></tr><tr><td style="border: 1px solid #000; text-align: center;">7</td><td style="border: 1px solid #000; text-align: center;">38</td><td style="border: 1px solid #000; text-align: center;">2021-02-01 00:00:00</td><td style="border: 1px solid #000; text-align: center;">Barbarossastraße 19</td><td style="border: 1px solid #000; text-align: center;">Berlin</td><td style="border: 1px solid #000; text-align: center;">None</td><td style="border: 1px solid #000; text-align: center;">Germany</td><td style="border: 1px solid #000; text-align: center;">10779</td><td style="border: 1px solid #000; text-align: center;">1.98</td></tr><tr><td style="border: 1px solid #000; text-align: center;">8</td><td style="border: 1px solid #000; text-align: center;">40</td><td style="border: 1px solid #000; text-align: center;">2021-02-01 00:00:00</td><td style="border: 1px solid #000; text-align: center;">8, Rue Hanovre</td><td style="border: 1px solid #000; text-align: center;">Paris</td><td style="border: 1px solid #000; text-align: center;">None</td><td style="border: 1px solid #000; text-align: center;">France</td><td style="border: 1px solid #000; text-align: center;">75002</td><td style="border: 1px solid #000; text-align: center;">1.98</td></tr><tr><td style="border: 1px solid #000; text-align: center;">9</td><td style="border: 1px solid #000; text-align: center;">42</td><td style="border: 1px solid #000; text-align: center;">2021-02-02 00:00:00</td><td style="border: 1px solid #000; text-align: center;">9, Place Louis Barthou</td><td style="border: 1px solid #000; text-align: center;">Bordeaux</td><td style="border: 1px solid #000; text-align: center;">None</td><td style="border: 1px solid #000; text-align: center;">France</td><td style="border: 1px solid #000; text-align: center;">33000</td><td style="border: 1px solid #000; text-align: center;">3.96</td></tr><tr><td style="border: 1px solid #000; text-align: center;">10</td><td style="border: 1px solid #000; text-align: center;">46</td><td style="border: 1px solid #000; text-align: center;">2021-02-03 00:00:00</td><td style="border: 1px solid #000; text-align: center;">3 Chatham Street</td><td style="border: 1px solid #000; text-align: center;">Dublin</td><td style="border: 1px solid #000; text-align: center;">Dublin</td><td style="border: 1px solid #000; text-align: center;">Ireland</td><td style="border: 1px solid #000; text-align: center;">None</td><td style="border: 1px solid #000; text-align: center;">5.94</td></tr><tr><td colspan="9" style="text-align:center;color:#888;font-style:italic;padding:8px;">... more rows ...</td></tr></tbody></table>

---

You can preview select statements before fetching data to your variables.

```py
table.select("InvoiceId", "CustomerId", "BillingAddress", "BillingCountry", "Total").where.gte("Total", 2.0).then.order_by("CustomerId").limit(10)
```

<table style="border-collapse: collapse; font-size: 14px;"><caption style="font-size: 18px; font-weight: bold;">Invoice</caption><thead><tr><td style="border: 1px solid #555; text-align: center;">InvoiceId</td><td style="border: 1px solid #555; text-align: center;">CustomerId</td><td style="border: 1px solid #555; text-align: center;">BillingAddress</td><td style="border: 1px solid #555; text-align: center;">BillingCountry</td><td style="border: 1px solid #555; text-align: center;">Total</td></tr></thead><tbody><tr><td style="border: 1px solid #000; text-align: center;">98</td><td style="border: 1px solid #000; text-align: center;">1</td><td style="border: 1px solid #000; text-align: center;">Av. Brigadeiro Faria Lima, 2170</td><td style="border: 1px solid #000; text-align: center;">Brazil</td><td style="border: 1px solid #000; text-align: center;">3.98</td></tr><tr><td style="border: 1px solid #000; text-align: center;">121</td><td style="border: 1px solid #000; text-align: center;">1</td><td style="border: 1px solid #000; text-align: center;">Av. Brigadeiro Faria Lima, 2170</td><td style="border: 1px solid #000; text-align: center;">Brazil</td><td style="border: 1px solid #000; text-align: center;">3.96</td></tr><tr><td style="border: 1px solid #000; text-align: center;">143</td><td style="border: 1px solid #000; text-align: center;">1</td><td style="border: 1px solid #000; text-align: center;">Av. Brigadeiro Faria Lima, 2170</td><td style="border: 1px solid #000; text-align: center;">Brazil</td><td style="border: 1px solid #000; text-align: center;">5.94</td></tr><tr><td style="border: 1px solid #000; text-align: center;">327</td><td style="border: 1px solid #000; text-align: center;">1</td><td style="border: 1px solid #000; text-align: center;">Av. Brigadeiro Faria Lima, 2170</td><td style="border: 1px solid #000; text-align: center;">Brazil</td><td style="border: 1px solid #000; text-align: center;">13.86</td></tr><tr><td style="border: 1px solid #000; text-align: center;">382</td><td style="border: 1px solid #000; text-align: center;">1</td><td style="border: 1px solid #000; text-align: center;">Av. Brigadeiro Faria Lima, 2170</td><td style="border: 1px solid #000; text-align: center;">Brazil</td><td style="border: 1px solid #000; text-align: center;">8.91</td></tr><tr><td style="border: 1px solid #000; text-align: center;">12</td><td style="border: 1px solid #000; text-align: center;">2</td><td style="border: 1px solid #000; text-align: center;">Theodor-Heuss-Straße 34</td><td style="border: 1px solid #000; text-align: center;">Germany</td><td style="border: 1px solid #000; text-align: center;">13.86</td></tr><tr><td style="border: 1px solid #000; text-align: center;">67</td><td style="border: 1px solid #000; text-align: center;">2</td><td style="border: 1px solid #000; text-align: center;">Theodor-Heuss-Straße 34</td><td style="border: 1px solid #000; text-align: center;">Germany</td><td style="border: 1px solid #000; text-align: center;">8.91</td></tr><tr><td style="border: 1px solid #000; text-align: center;">219</td><td style="border: 1px solid #000; text-align: center;">2</td><td style="border: 1px solid #000; text-align: center;">Theodor-Heuss-Straße 34</td><td style="border: 1px solid #000; text-align: center;">Germany</td><td style="border: 1px solid #000; text-align: center;">3.96</td></tr><tr><td style="border: 1px solid #000; text-align: center;">241</td><td style="border: 1px solid #000; text-align: center;">2</td><td style="border: 1px solid #000; text-align: center;">Theodor-Heuss-Straße 34</td><td style="border: 1px solid #000; text-align: center;">Germany</td><td style="border: 1px solid #000; text-align: center;">5.94</td></tr><tr><td style="border: 1px solid #000; text-align: center;">99</td><td style="border: 1px solid #000; text-align: center;">3</td><td style="border: 1px solid #000; text-align: center;">1498 rue Bélanger</td><td style="border: 1px solid #000; text-align: center;">Canada</td><td style="border: 1px solid #000; text-align: center;">3.98</td></tr></tbody></table>



```py
table.select("InvoiceId", "CustomerId", "BillingAddress", "BillingCountry", "Total").where.gte("Total", 2.0).then.order_by("CustomerId").limit(10).fetchall()

# Returns
[(98, 1, 'Av. Brigadeiro Faria Lima, 2170', 'Brazil', 3.98),
 (121, 1, 'Av. Brigadeiro Faria Lima, 2170', 'Brazil', 3.96),
 (143, 1, 'Av. Brigadeiro Faria Lima, 2170', 'Brazil', 5.94),
 (327, 1, 'Av. Brigadeiro Faria Lima, 2170', 'Brazil', 13.86),
 (382, 1, 'Av. Brigadeiro Faria Lima, 2170', 'Brazil', 8.91),
 (12, 2, 'Theodor-Heuss-Straße 34', 'Germany', 13.86),
 (67, 2, 'Theodor-Heuss-Straße 34', 'Germany', 8.91),
 (219, 2, 'Theodor-Heuss-Straße 34', 'Germany', 3.96),
 (241, 2, 'Theodor-Heuss-Straße 34', 'Germany', 5.94),
 (99, 3, '1498 rue Bélanger', 'Canada', 3.98)]
```

## 1.2. Purpose

It's a tiny little modern ORM that lets you prototype your databases locally with great flexibility. Also, it can be used in production apps to store and retrieve data, because all select, update, delete queries are parametrized. But it does not restrict you from using your own queries which might not be paramerized with methods like `select.custom()` and `where.custom()`.

And last (but not least) is data inspection. If you need to quickly inspect existing .db file but don't want to install yet another heavy ORM with a lot of unused dependencies, you might look into Sql-Engine, as it uses only native python modules.


## 1.3. Installation

To install `sqlengine`, you can use `pip`:

```sh
git clone --depth 1 https://github.com/suffermuffin/SQL-Engine.git
cd SQL-Engine
pip install -e .
```

## 1.4. Env

You may set environment variable for logging. By default it's `WARNING`.

```console
SQL_ENGINE_LOG_LEVEL=INFO
```

# 2. Usage

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
__primary__ : list[str] 
```


# 3. Table Declaration

There are a couple of ways to declare your table.


## 3.1. Class Declaration

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

## 3.2. Schema Declaration

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

# 4. Table Instantiation

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

# 5. Statements

Sql-Engine uses chained statements creation via object mutation. Unlike most other ORMs, Sql-Engine abstracts connection creation for single operations and lets you use statements to execute queries on the spot.

```py
table = schema.table_from_database("temp/data.db", "Employees")

table.delete.where.eq("ID", 0).eq("ID", 3).join("OR").then.execute()
table
```

<table style="border-collapse: collapse; font-size: 14px;"><caption style="font-size: 18px; font-weight: bold;">Employees</caption><thead><tr><td style="border: 1px solid #555; text-align: center;">ID</td><td style="border: 1px solid #555; text-align: center;">name</td><td style="border: 1px solid #555; text-align: center;">surname</td><td style="border: 1px solid #555; text-align: center;">salary</td><td style="border: 1px solid #555; text-align: center;">position</td></tr></thead><tbody><tr><td style="border: 1px solid #000; text-align: center;">1</td><td style="border: 1px solid #000; text-align: center;">test0</td><td style="border: 1px solid #000; text-align: center;">surname1</td><td style="border: 1px solid #000; text-align: center;">111.1</td><td style="border: 1px solid #000; text-align: center;">pos1</td></tr><tr><td style="border: 1px solid #000; text-align: center;">1</td><td style="border: 1px solid #000; text-align: center;">test1</td><td style="border: 1px solid #000; text-align: center;">surname1</td><td style="border: 1px solid #000; text-align: center;">111.1</td><td style="border: 1px solid #000; text-align: center;">pos1</td></tr><tr><td style="border: 1px solid #000; text-align: center;">2</td><td style="border: 1px solid #000; text-align: center;">test2</td><td style="border: 1px solid #000; text-align: center;">surname2</td><td style="border: 1px solid #000; text-align: center;">122.2</td><td style="border: 1px solid #000; text-align: center;">pos2</td></tr><tr><td style="border: 1px solid #000; text-align: center;">4</td><td style="border: 1px solid #000; text-align: center;">test4</td><td style="border: 1px solid #000; text-align: center;">surname4</td><td style="border: 1px solid #000; text-align: center;">144.4</td><td style="border: 1px solid #000; text-align: center;">pos4</td></tr></tbody></table>


You can preview your statements as SQL queries before execution.


```py
print(table.delete.where.eq("ID", 0).eq("ID", 3).join("OR"))
```

```sql
DELETE FROM Employees WHERE (ID = ? OR ID = ?);
```

Or repr them to see passed arguments

```py
repr(table.delete.where.eq("ID", 0).eq("ID", 3).join("OR"))
```

```sql
DELETE FROM Employees WHERE (ID = ? OR ID = ?); (0, 3)
```

## 5.1. Where

Statements have where clause builder. It's a separate object that you can access via `statement.where` property. It has basic methods like `eq()`, `gt()`, `lt()`, `neq()` and so on.

### 5.1.1. `join(lop : str)` operator

Where statements join clauses with `AND` logical operator by default, but it can be overwritten by `join()` method as shown in the example above. `join` combines all previous where clauses with provided logical operator and lets you continue the chain. This creates the ability to build complex where clauses.

```py
table = schema.table_from_database("temp/chinook.db", "Customer")

repr(table.select.where.eq("Country", "Brazil").eq("Country", "USA").join("OR").neq("State", "SP").join("AND").eq("City", "Vienne").join("OR"))
```

```sql
SELECT * FROM Customer WHERE (((Country = ? OR Country = ?) AND State != ?) OR City = ?); ('Brazil', 'USA', 'SP', 'Vienne')
```

<table style="border-collapse: collapse; font-size: 14px;"><caption style="font-size: 18px; font-weight: bold;">Customer</caption><thead><tr><td style="border: 1px solid #555; text-align: center;">CustomerId</td><td style="border: 1px solid #555; text-align: center;">FirstName</td><td style="border: 1px solid #555; text-align: center;">LastName</td><td style="border: 1px solid #555; text-align: center;">Company</td><td style="border: 1px solid #555; text-align: center;">Address</td><td style="border: 1px solid #555; text-align: center;">City</td><td style="border: 1px solid #555; text-align: center;">State</td><td style="border: 1px solid #555; text-align: center;">Country</td><td style="border: 1px solid #555; text-align: center;">PostalCode</td><td style="border: 1px solid #555; text-align: center;">Phone</td><td style="border: 1px solid #555; text-align: center;">Fax</td><td style="border: 1px solid #555; text-align: center;">Email</td><td style="border: 1px solid #555; text-align: center;">SupportRepId</td></tr></thead><tbody><tr><td style="border: 1px solid #000; text-align: center;">7</td><td style="border: 1px solid #000; text-align: center;">Astrid</td><td style="border: 1px solid #000; text-align: center;">Gruber</td><td style="border: 1px solid #000; text-align: center;">None</td><td style="border: 1px solid #000; text-align: center;">Rotenturmstraße 4, 1010 Innere Stadt</td><td style="border: 1px solid #000; text-align: center;">Vienne</td><td style="border: 1px solid #000; text-align: center;">None</td><td style="border: 1px solid #000; text-align: center;">Austria</td><td style="border: 1px solid #000; text-align: center;">1010</td><td style="border: 1px solid #000; text-align: center;">+43 01 5134505</td><td style="border: 1px solid #000; text-align: center;">None</td><td style="border: 1px solid #000; text-align: center;">astrid.gruber@apple.at</td><td style="border: 1px solid #000; text-align: center;">5</td></tr><tr><td style="border: 1px solid #000; text-align: center;">12</td><td style="border: 1px solid #000; text-align: center;">Roberto</td><td style="border: 1px solid #000; text-align: center;">Almeida</td><td style="border: 1px solid #000; text-align: center;">Riotur</td><td style="border: 1px solid #000; text-align: center;">Praça Pio X, 119</td><td style="border: 1px solid #000; text-align: center;">Rio de Janeiro</td><td style="border: 1px solid #000; text-align: center;">RJ</td><td style="border: 1px solid #000; text-align: center;">Brazil</td><td style="border: 1px solid #000; text-align: center;">20040-020</td><td style="border: 1px solid #000; text-align: center;">+55 (21) 2271-7000</td><td style="border: 1px solid #000; text-align: center;">+55 (21) 2271-7070</td><td style="border: 1px solid #000; text-align: center;">roberto.almeida@riotur.gov.br</td><td style="border: 1px solid #000; text-align: center;">3</td></tr><tr><td style="border: 1px solid #000; text-align: center;">13</td><td style="border: 1px solid #000; text-align: center;">Fernanda</td><td style="border: 1px solid #000; text-align: center;">Ramos</td><td style="border: 1px solid #000; text-align: center;">None</td><td style="border: 1px solid #000; text-align: center;">Qe 7 Bloco G</td><td style="border: 1px solid #000; text-align: center;">Brasília</td><td style="border: 1px solid #000; text-align: center;">DF</td><td style="border: 1px solid #000; text-align: center;">Brazil</td><td style="border: 1px solid #000; text-align: center;">71020-677</td><td style="border: 1px solid #000; text-align: center;">+55 (61) 3363-5547</td><td style="border: 1px solid #000; text-align: center;">+55 (61) 3363-7855</td><td style="border: 1px solid #000; text-align: center;">fernadaramos4@uol.com.br</td><td style="border: 1px solid #000; text-align: center;">4</td></tr><tr><td style="border: 1px solid #000; text-align: center;">16</td><td style="border: 1px solid #000; text-align: center;">Frank</td><td style="border: 1px solid #000; text-align: center;">Harris</td><td style="border: 1px solid #000; text-align: center;">Google Inc.</td><td style="border: 1px solid #000; text-align: center;">1600 Amphitheatre Parkway</td><td style="border: 1px solid #000; text-align: center;">Mountain View</td><td style="border: 1px solid #000; text-align: center;">CA</td><td style="border: 1px solid #000; text-align: center;">USA</td><td style="border: 1px solid #000; text-align: center;">94043-1351</td><td style="border: 1px solid #000; text-align: center;">+1 (650) 253-0000</td><td style="border: 1px solid #000; text-align: center;">+1 (650) 253-0000</td><td style="border: 1px solid #000; text-align: center;">fharris@google.com</td><td style="border: 1px solid #000; text-align: center;">4</td></tr><tr><td style="border: 1px solid #000; text-align: center;">17</td><td style="border: 1px solid #000; text-align: center;">Jack</td><td style="border: 1px solid #000; text-align: center;">Smith</td><td style="border: 1px solid #000; text-align: center;">Microsoft Corporation</td><td style="border: 1px solid #000; text-align: center;">1 Microsoft Way</td><td style="border: 1px solid #000; text-align: center;">Redmond</td><td style="border: 1px solid #000; text-align: center;">WA</td><td style="border: 1px solid #000; text-align: center;">USA</td><td style="border: 1px solid #000; text-align: center;">98052-8300</td><td style="border: 1px solid #000; text-align: center;">+1 (425) 882-8080</td><td style="border: 1px solid #000; text-align: center;">+1 (425) 882-8081</td><td style="border: 1px solid #000; text-align: center;">jacksmith@microsoft.com</td><td style="border: 1px solid #000; text-align: center;">5</td></tr><tr><td style="border: 1px solid #000; text-align: center;">18</td><td style="border: 1px solid #000; text-align: center;">Michelle</td><td style="border: 1px solid #000; text-align: center;">Brooks</td><td style="border: 1px solid #000; text-align: center;">None</td><td style="border: 1px solid #000; text-align: center;">627 Broadway</td><td style="border: 1px solid #000; text-align: center;">New York</td><td style="border: 1px solid #000; text-align: center;">NY</td><td style="border: 1px solid #000; text-align: center;">USA</td><td style="border: 1px solid #000; text-align: center;">10012-2612</td><td style="border: 1px solid #000; text-align: center;">+1 (212) 221-3546</td><td style="border: 1px solid #000; text-align: center;">+1 (212) 221-4679</td><td style="border: 1px solid #000; text-align: center;">michelleb@aol.com</td><td style="border: 1px solid #000; text-align: center;">3</td></tr><tr><td style="border: 1px solid #000; text-align: center;">19</td><td style="border: 1px solid #000; text-align: center;">Tim</td><td style="border: 1px solid #000; text-align: center;">Goyer</td><td style="border: 1px solid #000; text-align: center;">Apple Inc.</td><td style="border: 1px solid #000; text-align: center;">1 Infinite Loop</td><td style="border: 1px solid #000; text-align: center;">Cupertino</td><td style="border: 1px solid #000; text-align: center;">CA</td><td style="border: 1px solid #000; text-align: center;">USA</td><td style="border: 1px solid #000; text-align: center;">95014</td><td style="border: 1px solid #000; text-align: center;">+1 (408) 996-1010</td><td style="border: 1px solid #000; text-align: center;">+1 (408) 996-1011</td><td style="border: 1px solid #000; text-align: center;">tgoyer@apple.com</td><td style="border: 1px solid #000; text-align: center;">3</td></tr><tr><td style="border: 1px solid #000; text-align: center;">20</td><td style="border: 1px solid #000; text-align: center;">Dan</td><td style="border: 1px solid #000; text-align: center;">Miller</td><td style="border: 1px solid #000; text-align: center;">None</td><td style="border: 1px solid #000; text-align: center;">541 Del Medio Avenue</td><td style="border: 1px solid #000; text-align: center;">Mountain View</td><td style="border: 1px solid #000; text-align: center;">CA</td><td style="border: 1px solid #000; text-align: center;">USA</td><td style="border: 1px solid #000; text-align: center;">94040-111</td><td style="border: 1px solid #000; text-align: center;">+1 (650) 644-3358</td><td style="border: 1px solid #000; text-align: center;">None</td><td style="border: 1px solid #000; text-align: center;">dmiller@comcast.com</td><td style="border: 1px solid #000; text-align: center;">4</td></tr></tbody></table>


### 5.1.2. `build()` method

As name suggests - `build` method builds query. In context of table API it is not used by the developer directly, but it is nice to have. It returns parametrized args in right order to pass to `sqlite3` execute or fetch methods. `build` output from above would look like this:

```py
...join("OR").build()

# ->
('(((Country = ? OR Country = ?) AND State != ?) OR City = ?)',
 ('Brazil', 'USA', 'SP', 'Vienne'))
```


### 5.1.3. `then` property

`then` is a link to the above statement that can execute, aggregate, fetch, etc.

```py
table.select.where.eq("Country", "USA").then.fetchone()
```

## 5.2. Select

Select is non-mutational statement (i.e. it does not mutate table contents). Syntax is quite simple. In broad terms it can be described as: 

`select("Column1", "Column2", ...).order_by("Column").limit(n).fetchall()`

It will return list of rows, and each row will be in order of provided columns.

### 5.2.1. `limit(n : int)` op

`limit` *limits* number of returned rows to `n`

```py
table.select("SupportRepId","Email").limit(5)
```

<table style="border-collapse: collapse; font-size: 14px;"><caption style="font-size: 18px; font-weight: bold;">Customer</caption><thead><tr><td style="border: 1px solid #555; text-align: center;">SupportRepId</td><td style="border: 1px solid #555; text-align: center;">Email</td></tr></thead><tbody><tr><td style="border: 1px solid #000; text-align: center;">3</td><td style="border: 1px solid #000; text-align: center;">luisg@embraer.com.br</td></tr><tr><td style="border: 1px solid #000; text-align: center;">5</td><td style="border: 1px solid #000; text-align: center;">leonekohler@surfeu.de</td></tr><tr><td style="border: 1px solid #000; text-align: center;">3</td><td style="border: 1px solid #000; text-align: center;">ftremblay@gmail.com</td></tr><tr><td style="border: 1px solid #000; text-align: center;">4</td><td style="border: 1px solid #000; text-align: center;">bjorn.hansen@yahoo.no</td></tr><tr><td style="border: 1px solid #000; text-align: center;">4</td><td style="border: 1px solid #000; text-align: center;">frantisekw@jetbrains.com</td></tr></tbody></table>

### 5.2.2. `order_by(column : str, ascending : bool = True)` op

`order_by` will return rows in `ascending` or `descending` order of provided column.

```py
table.select("SupportRepId","Email").order_by("SupportRepId").limit(5)
```

<table style="border-collapse: collapse; font-size: 14px;"><caption style="font-size: 18px; font-weight: bold;">Customer</caption><thead><tr><td style="border: 1px solid #555; text-align: center;">SupportRepId</td><td style="border: 1px solid #555; text-align: center;">Email</td></tr></thead><tbody><tr><td style="border: 1px solid #000; text-align: center;">3</td><td style="border: 1px solid #000; text-align: center;">luisg@embraer.com.br</td></tr><tr><td style="border: 1px solid #000; text-align: center;">3</td><td style="border: 1px solid #000; text-align: center;">ftremblay@gmail.com</td></tr><tr><td style="border: 1px solid #000; text-align: center;">3</td><td style="border: 1px solid #000; text-align: center;">roberto.almeida@riotur.gov.br</td></tr><tr><td style="border: 1px solid #000; text-align: center;">3</td><td style="border: 1px solid #000; text-align: center;">jenniferp@rogers.ca</td></tr><tr><td style="border: 1px solid #000; text-align: center;">3</td><td style="border: 1px solid #000; text-align: center;">michelleb@aol.com</td></tr></tbody></table>


### 5.2.3. `aggregate(by : Literal['COUNT', 'SUM', 'AVG', 'MIN', 'MAX'])` op

`aggregate` *aggregates* by provided method. Note: `_repr_html_` does not work if `aggregate` was used and will fallback to `__repr__`.

```py
table.select("Email").where.eq("SupportRepId", 3).then.aggregate("COUNT").fetchone() # -> (21,)
```

### 5.2.4. `fetch()` methods

There are 3 fetch methods: `fetchone() -> SqlRow`, `fetchmany(size : int = 1) -> list[SqlRow]` and `fetchall() -> list[SqlRow]`. They are used at the end of the statement to finally execute built expression

```py
len(table.select("SupportRepId","Email").fetchone())    # -> 2 (len of values)
len(table.select("SupportRepId","Email").fetchmany(10)) # -> 10 (len of rows)
len(table.select("SupportRepId","Email").fetchall())    # -> 59 (len of rows)
```

### 5.2.5. `__iter__` methods

There are 2 iter methods: classical `__iter__` that lets you iterate over rows and `fetchmany_iterator(batch_size : int)`. Both of them are available if table is in [transaction](#4-table-instantiation) mode. Otherwise `RuntimeError` will be raised.


```py
with table.transaction():
    for batch in table.select.where.gt("Age", 30).then.fetchmany_iterator(1000):
        process_batch(batch)
```

```py
with table.transaction():
    for row in table.select.where.gt("Age", 30).then: # here `then` is used to link back to the `select` instance from `where` object
        process_row(row)
```

## 5.3. Update

Update is used to change values inside the table, therefore it is a mutational statement.

### 5.3.1. `set(column : str, value : SqlValue)` op

Set's `column` to provided `value`. You can use `where` builder to specify criteria for which rows this value must be changed.

```py
# This will move all residences of Czech Republic to Karaganda
table.update.set("City", "Karaganda").where.eq("Country", "Czech Republic").then.execute()
```

### 5.3.2. `execute()` method

This will build and execute the statement

## 5.4. Delete

Delete statement deletes rows based on where clause. You must specify at least one `where` operation.

```py
table.delete.where.eq("ID", 0).then.execute()
```

### 5.4.1. `execute()` method

This will (once again) build and execute the statement


# 6. Custom Types

You can define non-native sqlite datatypes. They have to implement `to_sql` and `from_sql`. There are other ways that `sqlite3` [documentation](https://docs.python.org/3/library/sqlite3.html#how-to-adapt-custom-python-types-to-sqlite-values) explains.


```py
class Point:
    def __init__(self, x : float, y : float) -> None:
        self.x = x
        self.y = y

    def to_sql(self) -> str:
        """ Method that converts object instance to native sqlite3 value """
        return f"{self.x}, {self.y}"
    
    @classmethod
    def from_sql(cls, sql : bytes):
        """ Method that accepts bytes and returns object instance """
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
point = table.select("coords").where.eq("ID", 0).then.fetchone()[0]

isinstance(point, Point) # -> True
```

# 7. Transactions

Transactions help you to not spam `execute()` -> `commit()` on a database for each of operations which is computationally heavy.

## 7.1. Transaction per Table

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

## 7.2. Shared Connection

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
    
    for batch in biggest_table.select.fetchmany_iterator(1000):
        copy_biggest_table.insert_many(batch)

    final_shape = copy_biggest_table.shape

final_shape # -> (9, 3503)
```

# 8. Syntax Sugar

## 8.1. Single Index

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

## 8.2. Multi Index

In case of multiple values in `__primary__` you have to call with tuple key in order of declared `__primary__`:

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
table[3, "test3"] # -> (3, 'test3', 'surname3', 133.3, 'pos3')
```

```sql
-- Debug output --
Employees: SELECT * FROM Employees WHERE ID = ? AND name = ?; (3, 'test3')
```

## 8.3. Length and Shape

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


# 9. More Examples

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
## 9.1. Instantiate

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
## 9.2. Insert single row

```py
table.insert(0, 'John', 'CEO')
```

```sql
-- Debug output --
EmployeesDB: INSERT INTO EmployeesDB (ID, Name, Occupation) VALUES (?, ?, ?); (0, 'John', 'CEO')
```

---
## 9.3. Insert many rows

```py
workers  = [(1, 'Boris', 'worker'), (2, 'George', 'worker'), (3, 'Kate', 'worker')]
table.insert_many(workers)
```

```sql
-- Debug output --
EmployeesDB: INSERT INTO EmployeesDB (ID, Name, Occupation) VALUES (?, ?, ?); [(1, 'Boris', 'worker'), (2, 'George', 'worker'), (3, 'Kate', 'worker')]
```

---
## 9.4. Insert many rows in transaction

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
## 9.5. Query select with specified columns

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
## 9.6. Query select with multiple where clauses

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

## 9.7. Select, compare, order and limit
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

## 9.8. Aggregate

```py
table.select.where.eq("Occupation", "worker").then.aggregate("COUNT").fetchone()

# -> (3,)
```

```sql
-- Debug output --
EmployeesDB: SELECT COUNT(*) FROM EmployeesDB WHERE Occupation = ?; ('worker',)
```

---
## 9.9. Delete rows

```py
table.delete.where.eq('ID', 1).then.execute()
```

```sql
-- Debug output --
EmployeesDB: DELETE FROM EmployeesDB WHERE ID = ?; (1,)
```

---
## 9.10. Select all

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
## 9.11. Create transaction rows batch generator

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

## 9.12. Iterate over multiple tables

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