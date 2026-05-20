- [Sql-Engine](#sql-engine)
  - [Features](#features)
  - [Purpose](#purpose)
  - [Installation](#installation)
  - [Env](#env)
- [Quick Start](#quick-start)
  - [Table Declaration](#table-declaration)
  - [Instantiation](#instantiation)
  - [Row insertion](#row-insertion)
  - [Jupyter view](#jupyter-view)
  - [Select Query](#select-query)
  - [Update Query](#update-query)
  - [Delete Query](#delete-query)
  - [Transaction](#transaction)
  - [Get Item](#get-item)
  - [Csv Converter](#csv-converter)
- [Full Documentation](#full-documentation)


# Sql-Engine

My Sql-Engine is a cute little wrapper for `sqlite3` table manipulations without any third party dependencies.


## Features

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
table.select("InvoiceId", "CustomerId", "BillingAddress", "BillingCountry", "Total")\
    .where\
        .gte("Total", 2.0)\
    .then\
        .order_by("CustomerId")\
        .limit(10)
```

<table style="border-collapse: collapse; font-size: 14px;"><caption style="font-size: 18px; font-weight: bold;">Invoice</caption><thead><tr><td style="border: 1px solid #555; text-align: center;">InvoiceId</td><td style="border: 1px solid #555; text-align: center;">CustomerId</td><td style="border: 1px solid #555; text-align: center;">BillingAddress</td><td style="border: 1px solid #555; text-align: center;">BillingCountry</td><td style="border: 1px solid #555; text-align: center;">Total</td></tr></thead><tbody><tr><td style="border: 1px solid #000; text-align: center;">98</td><td style="border: 1px solid #000; text-align: center;">1</td><td style="border: 1px solid #000; text-align: center;">Av. Brigadeiro Faria Lima, 2170</td><td style="border: 1px solid #000; text-align: center;">Brazil</td><td style="border: 1px solid #000; text-align: center;">3.98</td></tr><tr><td style="border: 1px solid #000; text-align: center;">121</td><td style="border: 1px solid #000; text-align: center;">1</td><td style="border: 1px solid #000; text-align: center;">Av. Brigadeiro Faria Lima, 2170</td><td style="border: 1px solid #000; text-align: center;">Brazil</td><td style="border: 1px solid #000; text-align: center;">3.96</td></tr><tr><td style="border: 1px solid #000; text-align: center;">143</td><td style="border: 1px solid #000; text-align: center;">1</td><td style="border: 1px solid #000; text-align: center;">Av. Brigadeiro Faria Lima, 2170</td><td style="border: 1px solid #000; text-align: center;">Brazil</td><td style="border: 1px solid #000; text-align: center;">5.94</td></tr><tr><td style="border: 1px solid #000; text-align: center;">327</td><td style="border: 1px solid #000; text-align: center;">1</td><td style="border: 1px solid #000; text-align: center;">Av. Brigadeiro Faria Lima, 2170</td><td style="border: 1px solid #000; text-align: center;">Brazil</td><td style="border: 1px solid #000; text-align: center;">13.86</td></tr><tr><td style="border: 1px solid #000; text-align: center;">382</td><td style="border: 1px solid #000; text-align: center;">1</td><td style="border: 1px solid #000; text-align: center;">Av. Brigadeiro Faria Lima, 2170</td><td style="border: 1px solid #000; text-align: center;">Brazil</td><td style="border: 1px solid #000; text-align: center;">8.91</td></tr><tr><td style="border: 1px solid #000; text-align: center;">12</td><td style="border: 1px solid #000; text-align: center;">2</td><td style="border: 1px solid #000; text-align: center;">Theodor-Heuss-Straße 34</td><td style="border: 1px solid #000; text-align: center;">Germany</td><td style="border: 1px solid #000; text-align: center;">13.86</td></tr><tr><td style="border: 1px solid #000; text-align: center;">67</td><td style="border: 1px solid #000; text-align: center;">2</td><td style="border: 1px solid #000; text-align: center;">Theodor-Heuss-Straße 34</td><td style="border: 1px solid #000; text-align: center;">Germany</td><td style="border: 1px solid #000; text-align: center;">8.91</td></tr><tr><td style="border: 1px solid #000; text-align: center;">219</td><td style="border: 1px solid #000; text-align: center;">2</td><td style="border: 1px solid #000; text-align: center;">Theodor-Heuss-Straße 34</td><td style="border: 1px solid #000; text-align: center;">Germany</td><td style="border: 1px solid #000; text-align: center;">3.96</td></tr><tr><td style="border: 1px solid #000; text-align: center;">241</td><td style="border: 1px solid #000; text-align: center;">2</td><td style="border: 1px solid #000; text-align: center;">Theodor-Heuss-Straße 34</td><td style="border: 1px solid #000; text-align: center;">Germany</td><td style="border: 1px solid #000; text-align: center;">5.94</td></tr><tr><td style="border: 1px solid #000; text-align: center;">99</td><td style="border: 1px solid #000; text-align: center;">3</td><td style="border: 1px solid #000; text-align: center;">1498 rue Bélanger</td><td style="border: 1px solid #000; text-align: center;">Canada</td><td style="border: 1px solid #000; text-align: center;">3.98</td></tr></tbody></table>


## Purpose

It's a tiny little modern ORM that lets you prototype your databases locally with great flexibility. Also, it can be used in production apps to store and retrieve data, because all select, update, delete queries are parametrized. But it does not restrict you from using your own queries which might not be paramerized with methods like `select.custom()` and `where.custom()`.

And last (but not least) is data inspection. If you need to quickly inspect existing .db file but don't want to install yet another heavy ORM with a lot of unused dependencies, you might look into Sql-Engine, as it uses only native python modules.


## Installation

To install `sqlengine`, you can use `pip`:

```sh
git clone --depth 1 https://github.com/suffermuffin/SQL-Engine.git
cd SQL-Engine
pip install -e .
```

## Env

You may set environment variable for logging. By default it's `WARNING`.

```console
SQL_ENGINE_LOG_LEVEL=INFO
```

# Quick Start

All you have to do to create your own cute little table is to [inherit](docs/table_declaration.md#class-declaration) `SqlTableMixin` class or to create your own [schema](docs/table_declaration.md#schema-declaration) and declare desired properties of your table's columns. They are:


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

## Table Declaration

More details at [Declaration](docs/table_declaration.md#table-declaration).

```py
from sqlengine import SqlTableMixin

# Helper constants for column names
ID = "ID"
Name = "Name"
Occupation = "Occupation"
Salary = "Salary"


class Employees(SqlTableMixin):

    __columns__   = [ID, Name, Occupation, Salary]
    __types__     = [int, str, str, float]
    __primary__   = [ID]

    # You may overwrite your insert methods for type consistency
    def insert(self, id : int, name : str, occupation : str, salary : float) -> None:
        return super().insert(id, name, occupation, salary)
    
    def upsert(self, id : int, name : str, occupation : str, salary : float) -> None:
        return super().upsert(id, name, occupation, salary)
```

## Instantiation

More details at [Instantiation](docs/table_declaration.md#table-instantiation).

```py
# Create an instance of the table class 
# with provided path to create or connect to

table = Employees("temp/data.db")
```

## Row insertion

```py
# Insert one row

table.insert(1, "John Doe", "Software Engineer", 75000.0)
```

```py
# Bulk insert multiple rows

employees_data = [
    (2, "Jane Smith", "Data Scientist", 80000.0),
    (3, "Alice Johnson", "Product Manager", 90000.0),
    (4, "Bob Brown", "Project Manager", 78000.0),
    (5, "Charlie Davis", "UI/UX Designer", 65000.0),
    (6, "David Wilson", "DevOps Engineer", 82000.0),
    (7, "Eve Taylor", "Customer Support", 45000.0),
    (8, "Frank White", "Quality Assurance", 53000.0),
    (9, "Grace Hall", "Marketing Manager", 68000.0),
    (10, "Henry Lee", "Technical Writer", 52000.0)
]

table.insert_many(employees_data)
```


```py
# Upsert one row

table.upsert(1, "Jane Doe", "Data Scientist", 80000.0)
```

## Jupyter view

```py
# Inspect tables in Jupyter Notebook

table
```

<table style="border-collapse: collapse; font-size: 14px;"><caption style="font-size: 18px; font-weight: bold;">Employees</caption><thead><tr><td style="border: 1px solid #555; text-align: center;">ID</td><td style="border: 1px solid #555; text-align: center;">Name</td><td style="border: 1px solid #555; text-align: center;">Occupation</td><td style="border: 1px solid #555; text-align: center;">Salary</td></tr></thead><tbody><tr><td style="border: 1px solid #000; text-align: center;">1</td><td style="border: 1px solid #000; text-align: center;">Jane Doe</td><td style="border: 1px solid #000; text-align: center;">Data Scientist</td><td style="border: 1px solid #000; text-align: center;">80000.0</td></tr><tr><td style="border: 1px solid #000; text-align: center;">2</td><td style="border: 1px solid #000; text-align: center;">Jane Smith</td><td style="border: 1px solid #000; text-align: center;">Data Scientist</td><td style="border: 1px solid #000; text-align: center;">80000.0</td></tr><tr><td style="border: 1px solid #000; text-align: center;">3</td><td style="border: 1px solid #000; text-align: center;">Alice Johnson</td><td style="border: 1px solid #000; text-align: center;">Product Manager</td><td style="border: 1px solid #000; text-align: center;">90000.0</td></tr><tr><td style="border: 1px solid #000; text-align: center;">4</td><td style="border: 1px solid #000; text-align: center;">Bob Brown</td><td style="border: 1px solid #000; text-align: center;">Project Manager</td><td style="border: 1px solid #000; text-align: center;">78000.0</td></tr><tr><td style="border: 1px solid #000; text-align: center;">5</td><td style="border: 1px solid #000; text-align: center;">Charlie Davis</td><td style="border: 1px solid #000; text-align: center;">UI/UX Designer</td><td style="border: 1px solid #000; text-align: center;">65000.0</td></tr><tr><td style="border: 1px solid #000; text-align: center;">6</td><td style="border: 1px solid #000; text-align: center;">David Wilson</td><td style="border: 1px solid #000; text-align: center;">DevOps Engineer</td><td style="border: 1px solid #000; text-align: center;">82000.0</td></tr><tr><td style="border: 1px solid #000; text-align: center;">7</td><td style="border: 1px solid #000; text-align: center;">Eve Taylor</td><td style="border: 1px solid #000; text-align: center;">Customer Support</td><td style="border: 1px solid #000; text-align: center;">45000.0</td></tr><tr><td style="border: 1px solid #000; text-align: center;">8</td><td style="border: 1px solid #000; text-align: center;">Frank White</td><td style="border: 1px solid #000; text-align: center;">Quality Assurance</td><td style="border: 1px solid #000; text-align: center;">53000.0</td></tr><tr><td style="border: 1px solid #000; text-align: center;">9</td><td style="border: 1px solid #000; text-align: center;">Grace Hall</td><td style="border: 1px solid #000; text-align: center;">Marketing Manager</td><td style="border: 1px solid #000; text-align: center;">68000.0</td></tr><tr><td style="border: 1px solid #000; text-align: center;">10</td><td style="border: 1px solid #000; text-align: center;">Henry Lee</td><td style="border: 1px solid #000; text-align: center;">Technical Writer</td><td style="border: 1px solid #000; text-align: center;">52000.0</td></tr></tbody></table>

## Select Query

More details at [Statements](docs/statements.md).

```py
# Query select and fetch

table.select.where.between(ID, 3, 5).then.fetchall()

# ->
# [(3, 'Alice Johnson', 'Product Manager', 90000.0),
#  (4, 'Bob Brown', 'Project Manager', 78000.0),
#  (5, 'Charlie Davis', 'UI/UX Designer', 65000.0)]
```

```py
# Inspect query select in Jupyter

table.select(Name, Salary).where.lt(Salary, 70_000)
```

<table style="border-collapse: collapse; font-size: 14px;"><caption style="font-size: 18px; font-weight: bold;">Employees</caption><thead><tr><td style="border: 1px solid #555; text-align: center;">Name</td><td style="border: 1px solid #555; text-align: center;">Salary</td></tr></thead><tbody><tr><td style="border: 1px solid #000; text-align: center;">Charlie Davis</td><td style="border: 1px solid #000; text-align: center;">65000.0</td></tr><tr><td style="border: 1px solid #000; text-align: center;">Eve Taylor</td><td style="border: 1px solid #000; text-align: center;">45000.0</td></tr><tr><td style="border: 1px solid #000; text-align: center;">Frank White</td><td style="border: 1px solid #000; text-align: center;">53000.0</td></tr><tr><td style="border: 1px solid #000; text-align: center;">Grace Hall</td><td style="border: 1px solid #000; text-align: center;">68000.0</td></tr><tr><td style="border: 1px solid #000; text-align: center;">Henry Lee</td><td style="border: 1px solid #000; text-align: center;">52000.0</td></tr></tbody></table>

## Update Query

```py
# equal to ...update.set(Salary, 50_000)...
table.update(Salary, 50_000).where.eq(Name, "Eve Taylor").then.execute()
```

## Delete Query

```py
table.delete.where.eq(ID, 5).then.execute()
```

## Transaction

More details at [Transaction](docs/transactions.md).

```py
# Operate within a transaction

with table.transaction():
    for row in employees_data:
        table.upsert(*row)
```

## Get Item

More details at [Syntax Sugar](docs/syntax_sugar.md).

```py
# Fetch row by primary key

table[9]

# -> (9, 'Grace Hall', 'Marketing Manager', 68000.0)
```

```py
# Fetch slice by integer primary key

table[4:10:2]

# -> 
# [(4, 'Bob Brown', 'Project Manager', 78000.0),
#  (6, 'David Wilson', 'DevOps Engineer', 82000.0),
#  (8, 'Frank White', 'Quality Assurance', 53000.0)]
```

## Csv Converter

```py
# Save table to csv
from sqlengine.utils import to_csv

to_csv(table, "temp/table.csv")
```

```py
# Save query result to csv

to_csv(table.select.where.gt(Salary, 70_000), "temp/query.csv")
```

# Full Documentation

For detailed usage, API reference, and advanced examples, see the [full documentation](docs/index.md).
