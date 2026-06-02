<p align="center">
<a href="https://github.com/suffermuffin/SQL-Engine/actions/workflows/test.yml?query=event%3Apush">
    <img src="https://github.com/suffermuffin/SQL-Engine/actions/workflows/test.yml/badge.svg?event=push&branch=main" alt="Tests">
</a>
<a href="https://github.com/suffermuffin/SQL-Engine/actions?query=workflow%3APublish">
    <img src="https://github.com/suffermuffin/SQL-Engine/actions/workflows/publish.yml/badge.svg" alt="Publishing">
</a>
<a href="https://pypi.org/project/sqlengine-lite/">
    <img alt="PyPI" src="https://img.shields.io/pypi/v/sqlengine-lite?logoSize=amd&labelColor=black&color=royalblue">
</a>
</p>


# SqlEngine

My SqlEngine is a cute little wrapper for `sqlite3` table manipulations without any third party dependencies.


[**Home Page**](https://github.com/suffermuffin/SQL-Engine) | [**Installation**](#installation) | [**Quick Start**](#quick-start) | [**Documentation**](https://github.com/suffermuffin/SQL-Engine/blob/main/docs/index.md)


## Features

SqlEngine abstracts SQL queries into tiny little methods like `insert`, `insert_many`, `upsert`, and not so tiny (but still cute and little) query builders like `select`, `delete` and `update`. SqlEngine also provides bulk insertion with `insert_many` and transaction operations like `select.fetchmany_iterator`. Methods can be executed either in transaction mode (thanks to `transaction` context manager) or right on the spot.


SqlEngine implements Jupyter integration and dynamic schema building. You can easily instantiate existing database table and view it in a cute little html representation.

```py
from sqlengine import schema

table = schema.table_from_database("temp/chinook.db", "Album")
table
```

<table style="border-collapse: collapse; font-size: 14px;"><caption style="font-size: 18px; font-weight: bold;">Album</caption><thead><tr><td style="border: 1px solid #555; text-align: center;">AlbumId</td><td style="border: 1px solid #555; text-align: center;">Title</td><td style="border: 1px solid #555; text-align: center;">ArtistId</td></tr></thead><tbody><tr><td style="border: 1px solid #000; text-align: center;">1</td><td style="border: 1px solid #000; text-align: center;">For Those About To Rock We Salute You</td><td style="border: 1px solid #000; text-align: center;">1</td></tr><tr><td style="border: 1px solid #000; text-align: center;">2</td><td style="border: 1px solid #000; text-align: center;">Balls to the Wall</td><td style="border: 1px solid #000; text-align: center;">2</td></tr><tr><td style="border: 1px solid #000; text-align: center;">3</td><td style="border: 1px solid #000; text-align: center;">Restless and Wild</td><td style="border: 1px solid #000; text-align: center;">2</td></tr><tr><td style="border: 1px solid #000; text-align: center;">4</td><td style="border: 1px solid #000; text-align: center;">Let There Be Rock</td><td style="border: 1px solid #000; text-align: center;">1</td></tr><tr><td style="border: 1px solid #000; text-align: center;">5</td><td style="border: 1px solid #000; text-align: center;">Big Ones</td><td style="border: 1px solid #000; text-align: center;">3</td></tr><tr><td style="border: 1px solid #000; text-align: center;">6</td><td style="border: 1px solid #000; text-align: center;">Jagged Little Pill</td><td style="border: 1px solid #000; text-align: center;">4</td></tr><tr><td style="border: 1px solid #000; text-align: center;">7</td><td style="border: 1px solid #000; text-align: center;">Facelift</td><td style="border: 1px solid #000; text-align: center;">5</td></tr><tr><td style="border: 1px solid #000; text-align: center;">8</td><td style="border: 1px solid #000; text-align: center;">Warner 25 Anos</td><td style="border: 1px solid #000; text-align: center;">6</td></tr><tr><td style="border: 1px solid #000; text-align: center;">9</td><td style="border: 1px solid #000; text-align: center;">Plays Metallica By Four Cellos</td><td style="border: 1px solid #000; text-align: center;">7</td></tr><tr><td style="border: 1px solid #000; text-align: center;">10</td><td style="border: 1px solid #000; text-align: center;">Audioslave</td><td style="border: 1px solid #000; text-align: center;">8</td></tr><tr><td colspan="3" style="text-align:center;color:#888;font-style:italic;padding:8px;">... more rows ...</td></tr></tbody></table>

Note that GitHub's html rendering is simplified.

---

You can preview select statements before fetching data to your variables.

```py
table = schema.table_from_database("temp/chinook.db", "Invoice")

table.select("InvoiceId", "CustomerId", "BillingAddress", "BillingCountry", "Total")\
    .where\
        .gte("Total", 2.0)\
    .then\
        .order_by("CustomerId")\
        .limit(10)
```

<table style="border-collapse: collapse; font-size: 14px;"><caption style="font-size: 18px; font-weight: bold;">Invoice</caption><thead><tr><td style="border: 1px solid #555; text-align: center;">InvoiceId</td><td style="border: 1px solid #555; text-align: center;">CustomerId</td><td style="border: 1px solid #555; text-align: center;">BillingAddress</td><td style="border: 1px solid #555; text-align: center;">BillingCountry</td><td style="border: 1px solid #555; text-align: center;">Total</td></tr></thead><tbody><tr><td style="border: 1px solid #000; text-align: center;">98</td><td style="border: 1px solid #000; text-align: center;">1</td><td style="border: 1px solid #000; text-align: center;">Av. Brigadeiro Faria Lima, 2170</td><td style="border: 1px solid #000; text-align: center;">Brazil</td><td style="border: 1px solid #000; text-align: center;">3.98</td></tr><tr><td style="border: 1px solid #000; text-align: center;">121</td><td style="border: 1px solid #000; text-align: center;">1</td><td style="border: 1px solid #000; text-align: center;">Av. Brigadeiro Faria Lima, 2170</td><td style="border: 1px solid #000; text-align: center;">Brazil</td><td style="border: 1px solid #000; text-align: center;">3.96</td></tr><tr><td style="border: 1px solid #000; text-align: center;">143</td><td style="border: 1px solid #000; text-align: center;">1</td><td style="border: 1px solid #000; text-align: center;">Av. Brigadeiro Faria Lima, 2170</td><td style="border: 1px solid #000; text-align: center;">Brazil</td><td style="border: 1px solid #000; text-align: center;">5.94</td></tr><tr><td style="border: 1px solid #000; text-align: center;">327</td><td style="border: 1px solid #000; text-align: center;">1</td><td style="border: 1px solid #000; text-align: center;">Av. Brigadeiro Faria Lima, 2170</td><td style="border: 1px solid #000; text-align: center;">Brazil</td><td style="border: 1px solid #000; text-align: center;">13.86</td></tr><tr><td style="border: 1px solid #000; text-align: center;">382</td><td style="border: 1px solid #000; text-align: center;">1</td><td style="border: 1px solid #000; text-align: center;">Av. Brigadeiro Faria Lima, 2170</td><td style="border: 1px solid #000; text-align: center;">Brazil</td><td style="border: 1px solid #000; text-align: center;">8.91</td></tr><tr><td style="border: 1px solid #000; text-align: center;">12</td><td style="border: 1px solid #000; text-align: center;">2</td><td style="border: 1px solid #000; text-align: center;">Theodor-Heuss-Straße 34</td><td style="border: 1px solid #000; text-align: center;">Germany</td><td style="border: 1px solid #000; text-align: center;">13.86</td></tr><tr><td style="border: 1px solid #000; text-align: center;">67</td><td style="border: 1px solid #000; text-align: center;">2</td><td style="border: 1px solid #000; text-align: center;">Theodor-Heuss-Straße 34</td><td style="border: 1px solid #000; text-align: center;">Germany</td><td style="border: 1px solid #000; text-align: center;">8.91</td></tr><tr><td style="border: 1px solid #000; text-align: center;">219</td><td style="border: 1px solid #000; text-align: center;">2</td><td style="border: 1px solid #000; text-align: center;">Theodor-Heuss-Straße 34</td><td style="border: 1px solid #000; text-align: center;">Germany</td><td style="border: 1px solid #000; text-align: center;">3.96</td></tr><tr><td style="border: 1px solid #000; text-align: center;">241</td><td style="border: 1px solid #000; text-align: center;">2</td><td style="border: 1px solid #000; text-align: center;">Theodor-Heuss-Straße 34</td><td style="border: 1px solid #000; text-align: center;">Germany</td><td style="border: 1px solid #000; text-align: center;">5.94</td></tr><tr><td style="border: 1px solid #000; text-align: center;">99</td><td style="border: 1px solid #000; text-align: center;">3</td><td style="border: 1px solid #000; text-align: center;">1498 rue Bélanger</td><td style="border: 1px solid #000; text-align: center;">Canada</td><td style="border: 1px solid #000; text-align: center;">3.98</td></tr></tbody></table>


## Purpose

It's a tiny little modern ORM-like that lets you prototype your databases locally with great flexibility. Also, it can be used in production apps to store and retrieve data, because all select, update, delete queries are parametrized. But it does not restrict you from using your own queries which might not be parameterized with methods like `select.custom()` and `where.custom()`. Flexibility is a go to for this library.

And last (but not least) is data inspection. If you need to quickly inspect existing .db file but don't want to install yet another heavy ORM with a lot of unused dependencies and features, you might look into SqlEngine, as it uses only native python modules, implements dynamic schema builder and has a good synergy with Jupyter Notebook.


## Installation

To install SqlEngine, you can use `pip`:

```bash
pip install sqlengine-lite
```

## Env

You can set environment variable for logging. By default it's `WARNING`.

```console
SQL_ENGINE_LOG_LEVEL=INFO
```


# Quick Start

Here lays everything you need to know to start working with SqlEngine. For detailed usage, API reference, and advanced examples, see the [full documentation](https://github.com/suffermuffin/SQL-Engine/blob/main/docs/index.md).

## Table Declaration

All you have to do to create your own cute little table is to [inherit](https://github.com/suffermuffin/SQL-Engine/blob/main/docs/table_declaration.md#class-declaration) `SqlTableMixin` class or to create your own [schema](https://github.com/suffermuffin/SQL-Engine/blob/main/docs/table_declaration.md#schema-declaration) and declare desired types of your table's columns.


```py
from sqlengine import SqlTableMixin, Primary

class Employees(SqlTableMixin):

    ID         : Primary[int]
    Name       : str | None
    Occupation : str
    Salary     : float

```

More details at [Declaration](https://github.com/suffermuffin/SQL-Engine/blob/main/docs/table_declaration.md#table-declaration).

## Instantiation

Create an instance of the table class with provided path to create or connect to. `force_drop=True` to overwrite existing table if it exists.

```py
table = Employees("temp/data.db", force_drop=True)
```

More details at [Instantiation](https://github.com/suffermuffin/SQL-Engine/blob/main/docs/table_declaration.md#table-instantiation).

## Row insertion

Insert one row in a `*args` style.

```py
table.insert(1, "John Doe", "Software Engineer", 75000.0)
```

Use **kwargs mapping** to insert/upsert one row

```py
table.insert(2, salary=80000.0, name="Jane Smith", occupation="Data Scientist")
```

Bulk insert multiple rows.

```py
employees_data = [
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


Upsert one row.

```py
table.upsert(1, "Jane Doe", "Data Scientist", 80000.0)
```

## Jupyter view

Inspect table in Jupyter Notebook.

```py
table
```

<table style="border-collapse: collapse; font-size: 14px;"><caption style="font-size: 18px; font-weight: bold;">Employees</caption><thead><tr><td style="border: 1px solid #555; text-align: center;">ID</td><td style="border: 1px solid #555; text-align: center;">Name</td><td style="border: 1px solid #555; text-align: center;">Occupation</td><td style="border: 1px solid #555; text-align: center;">Salary</td></tr></thead><tbody><tr><td style="border: 1px solid #000; text-align: center;">1</td><td style="border: 1px solid #000; text-align: center;">Jane Doe</td><td style="border: 1px solid #000; text-align: center;">Data Scientist</td><td style="border: 1px solid #000; text-align: center;">80000.0</td></tr><tr><td style="border: 1px solid #000; text-align: center;">2</td><td style="border: 1px solid #000; text-align: center;">Jane Smith</td><td style="border: 1px solid #000; text-align: center;">Data Scientist</td><td style="border: 1px solid #000; text-align: center;">80000.0</td></tr><tr><td style="border: 1px solid #000; text-align: center;">3</td><td style="border: 1px solid #000; text-align: center;">Alice Johnson</td><td style="border: 1px solid #000; text-align: center;">Product Manager</td><td style="border: 1px solid #000; text-align: center;">90000.0</td></tr><tr><td style="border: 1px solid #000; text-align: center;">4</td><td style="border: 1px solid #000; text-align: center;">Bob Brown</td><td style="border: 1px solid #000; text-align: center;">Project Manager</td><td style="border: 1px solid #000; text-align: center;">78000.0</td></tr><tr><td style="border: 1px solid #000; text-align: center;">5</td><td style="border: 1px solid #000; text-align: center;">Charlie Davis</td><td style="border: 1px solid #000; text-align: center;">UI/UX Designer</td><td style="border: 1px solid #000; text-align: center;">65000.0</td></tr><tr><td style="border: 1px solid #000; text-align: center;">6</td><td style="border: 1px solid #000; text-align: center;">David Wilson</td><td style="border: 1px solid #000; text-align: center;">DevOps Engineer</td><td style="border: 1px solid #000; text-align: center;">82000.0</td></tr><tr><td style="border: 1px solid #000; text-align: center;">7</td><td style="border: 1px solid #000; text-align: center;">Eve Taylor</td><td style="border: 1px solid #000; text-align: center;">Customer Support</td><td style="border: 1px solid #000; text-align: center;">45000.0</td></tr><tr><td style="border: 1px solid #000; text-align: center;">8</td><td style="border: 1px solid #000; text-align: center;">Frank White</td><td style="border: 1px solid #000; text-align: center;">Quality Assurance</td><td style="border: 1px solid #000; text-align: center;">53000.0</td></tr><tr><td style="border: 1px solid #000; text-align: center;">9</td><td style="border: 1px solid #000; text-align: center;">Grace Hall</td><td style="border: 1px solid #000; text-align: center;">Marketing Manager</td><td style="border: 1px solid #000; text-align: center;">68000.0</td></tr><tr><td style="border: 1px solid #000; text-align: center;">10</td><td style="border: 1px solid #000; text-align: center;">Henry Lee</td><td style="border: 1px solid #000; text-align: center;">Technical Writer</td><td style="border: 1px solid #000; text-align: center;">52000.0</td></tr></tbody></table>

## Select Query

Helper constants for column names.

```py
ID         = "ID"
Name       = "Name"
Occupation = "Occupation"
Salary     = "Salary"
```

Query **select** and **fetch** in one go.

```py
table.select.where.between(ID, 3, 5).then.fetchall()

# ->
# [(3, 'Alice Johnson', 'Product Manager', 90000.0),
#  (4, 'Bob Brown', 'Project Manager', 78000.0),
#  (5, 'Charlie Davis', 'UI/UX Designer', 65000.0)]
```

**Iterate** over select statements.

```py
with table.transaction():
    for id, occupation in table.select(ID, Occupation).where.gt(Salary, 70_000):
        print(id, occupation)
```

```console
1 Data Scientist
3 Product Manager
4 Project Manager
6 DevOps Engineer
```

Inspect select query in Jupyter.

```py
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

More details at [Statements](https://github.com/suffermuffin/SQL-Engine/blob/main/docs/statements.md).

## Transaction

Operate within a transaction

```py

with table.transaction():
    for row in employees_data:
        table.upsert(*row)
```

More details at [Transaction](https://github.com/suffermuffin/SQL-Engine/blob/main/docs/transactions.md).

## Get Item

Fetch row by primary key.

```py
table[9] # -> (9, 'Grace Hall', 'Marketing Manager', 68000.0)
```

Fetch slice by integer primary key.

```py
table[4:10:2]

# -> 
# [(4, 'Bob Brown', 'Project Manager', 78000.0),
#  (6, 'David Wilson', 'DevOps Engineer', 82000.0),
#  (8, 'Frank White', 'Quality Assurance', 53000.0)]
```

More details at [Syntax Sugar](https://github.com/suffermuffin/SQL-Engine/blob/main/docs/syntax_sugar.md).

## Custom Types

Declare your own non-native SQL type to be compatible with sqlite3.

```py
from datetime import datetime
from sqlengine import SqlTableMixin, Primary

class DateTime(datetime):
    
    def to_sql(self) -> str:
        return self.strftime("%Y-%m-%d %H:%M")

    @classmethod
    def from_sql(cls, sql : bytes):
        return cls.fromisoformat(sql.decode('utf-8'))
    
    def __repr__(self):
        return f"DateTime({self.time})"


class ReservationIndex(SqlTableMixin):

    user_id : Primary[int]
    room_id : Primary[str]
    time_at : Primary[DateTime]
    user_name : str | None


table = ReservationIndex("temp/data.db")

table.insert(
    user_id=1, 
    room_id="loft_1", 
    time_at=DateTime.now()
)
table
```

<table style="border-collapse: collapse; font-size: 14px;"><caption style="font-size: 18px; font-weight: bold;">ReservationIndex</caption><thead><tr><td style="border: 1px solid #555; text-align: center;">user_id</td><td style="border: 1px solid #555; text-align: center;">room_id</td><td style="border: 1px solid #555; text-align: center;">time_at</td><td style="border: 1px solid #555; text-align: center;">user_name</td></tr></thead><tbody><tr><td style="border: 1px solid #000; text-align: center;">1</td><td style="border: 1px solid #000; text-align: center;">loft_1</td><td style="border: 1px solid #000; text-align: center;">2026-05-29 21:29:00</td><td style="border: 1px solid #000; text-align: center;">None</td></tr></tbody></table>

More details at [Custom Types](https://github.com/suffermuffin/SQL-Engine/blob/main/docs/custom_types.md).

## Pure SQL

Use SQL queries directly.

```py
with table.transaction(autocommit=False):
    table.conn.execute("DELETE FROM ReservationIndex WHERE user_id = 1;")
    table.commit()
```

Same as:

```py
table.conn.execute("DELETE FROM ReservationIndex WHERE user_id = 1;")
```

```py
table
```

<table style="border-collapse: collapse; font-size: 14px;"><caption style="font-size: 18px; font-weight: bold;">ReservationIndex</caption><thead><tr><td style="border: 1px solid #555; text-align: center;">user_id</td><td style="border: 1px solid #555; text-align: center;">room_id</td><td style="border: 1px solid #555; text-align: center;">time_at</td><td style="border: 1px solid #555; text-align: center;">user_name</td></tr></thead><tbody></tbody></table>

## Csv Converter

Save table to csv.

```py
from sqlengine.utils import to_csv

to_csv(table, "temp/table.csv")
```

Save query result to csv.

```py
to_csv(table.select.where.gt(Salary, 70_000), "temp/query.csv")
```

Stream big tables or query results to csv.

```py
with table.transaction():
    to_csv(table, "temp/query.csv", stream_batch_size=1000)
```

## Pandas-like Converter

Convert tables or query results to pandas **DataFrame** in one shot.

```py
import pandas as pd
from sqlengine.utils import to_dicts

df = pd.DataFrame(to_dicts(table))
df.set_index("ID", inplace=True)
```

Or stream them via generator.

```py
import pandas as pd
from sqlengine.utils import to_dicts_stream

df = pd.DataFrame(columns=table.columns)

with table.transaction():
    for batch in to_dicts_stream(table, batch_size=1000):
        df = pd.concat([df, pd.DataFrame(batch)], axis=0)

df.set_index("ID", inplace=True)
```

# Contributions

Your impact is welcome. Install module from source if you want to contribute:

```bash
git clone https://github.com/suffermuffin/SQL-Engine.git
cd SQL-Engine
```

Use `uv` to sync dependencies and checkout to your new branch:

```bash
uv sync
git checkout -b "<your_feature_or_fix_name>"
```

Don't forget to run tests after the implementation:

```bash
uv run python -m unittest discover -s tests
```

And update api documentation with your docstrings:

```bash
pydoc-markdown
```

# License

This project is licensed under the terms of the [MIT license](https://github.com/suffermuffin/SQL-Engine/blob/main/LICENSE).