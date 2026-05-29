# 1. Statements

Sql-Engine uses chained statements creation via object mutation. Unlike most other ORMs, Sql-Engine abstracts connection creation for single operations and lets you use statements to execute queries on the spot.

```py
table = schema.table_from_database("temp/data.db", "Employees")

table.delete.where.eq("ID", 0).eq("ID", 3).join("OR").then.execute()
table
```

<table style="border-collapse: collapse; font-size: 14px;"><caption style="font-size: 18px; font-weight: bold;">Employees</caption><thead><tr><td style="border: 1px solid #555; text-align: center;">ID</td><td style="border: 1px solid #555; text-align: center;">name</td><td style="border: 1px solid #555; text-align: center;">surname</td><td style="border: 1px solid #555; text-align: center;">salary</td><td style="border: 1px solid #555; text-align: center;">position</td></tr></thead><tbody><tr><td style="border: 1px solid #000; text-align: center;">1</td><td style="border: 1px solid #000; text-align: center;">test0</td><td style="border: 1px solid #000; text-align: center;">surname1</td><td style="border: 1px solid #000; text-align: center;">111.1</td><td style="border: 1px solid #000; text-align: center;">pos1</td></tr><tr><td style="border: 1px solid #000; text-align: center;">1</td><td style="border: 1px solid #000; text-align: center;">test1</td><td style="border: 1px solid #000; text-align: center;">surname1</td><td style="border: 1px solid #000; text-align: center;">111.1</td><td style="border: 1px solid #000; text-align: center;">pos1</td></tr><tr><td style="border: 1px solid #000; text-align: center;">2</td><td style="border: 1px solid #000; text-align: center;">test2</td><td style="border: 1px solid #000; text-align: center;">surname2</td><td style="border: 1px solid #000; text-align: center;">122.2</td><td style="border: 1px solid #000; text-align: center;">pos2</td></tr><tr><td style="border: 1px solid #000; text-align: center;">4</td><td style="border: 1px solid #000; text-align: center;">test4</td><td style="border: 1px solid #000; text-align: center;">surname4</td><td style="border: 1px solid #000; text-align: center;">144.4</td><td style="border: 1px solid #000; text-align: center;">pos4</td></tr></tbody></table>


You can preview your statements as SQL queries before execution:


```py
print(table.delete.where.eq("ID", 0).eq("ID", 3).join("OR"))
```

```sql
DELETE FROM Employees WHERE (ID = ? OR ID = ?);
```

Repr them to see passed arguments:

```py
repr(table.delete.where.eq("ID", 0).eq("ID", 3).join("OR"))
```

```sql
DELETE FROM Employees WHERE (ID = ? OR ID = ?); (0, 3)
```

Or you can just build the statement:

```py
query, args = table.delete.where.eq("ID", 0).eq("ID", 3).join("OR").then.build()
```

## 1.1. Where

Statements have where clause builder. It's a separate object that you can access via `statement.where` property. It has basic methods like `eq()`, `gt()`, `lt()`, `neq()` and so on.

### 1.1.1. `join(lop : str)` op

Where statements join clauses with `AND` logical operator by default, but it can be overwritten by `join()` method as shown in the example above. `join` combines all previous where clauses with provided logical operator and lets you continue the chain. This creates the ability to build complex where clauses.

```py
table = schema.table_from_database("temp/chinook.db", "Customer")

repr(
    table.select("CustomerId", "City", "State", "Country")\
        .where\
            .eq("Country", "Brazil")\
            .eq("Country", "USA")\
                .join("OR")\
            .neq("State", "SP")\
                .join("AND")\
            .eq("City", "Vienne")\
                .join("OR")
)
```

```sql
SELECT CustomerId, City, State, Country FROM Customer WHERE (((Country = ? OR Country = ?) AND State != ?) OR City = ?); ('Brazil', 'USA', 'SP', 'Vienne')
```

<table style="border-collapse: collapse; font-size: 14px;"><caption style="font-size: 18px; font-weight: bold;">Customer</caption><thead><tr><td style="border: 1px solid #555; text-align: center;">CustomerId</td><td style="border: 1px solid #555; text-align: center;">City</td><td style="border: 1px solid #555; text-align: center;">State</td><td style="border: 1px solid #555; text-align: center;">Country</td></tr></thead><tbody><tr><td style="border: 1px solid #000; text-align: center;">7</td><td style="border: 1px solid #000; text-align: center;">Vienne</td><td style="border: 1px solid #000; text-align: center;">None</td><td style="border: 1px solid #000; text-align: center;">Austria</td></tr><tr><td style="border: 1px solid #000; text-align: center;">12</td><td style="border: 1px solid #000; text-align: center;">Rio de Janeiro</td><td style="border: 1px solid #000; text-align: center;">RJ</td><td style="border: 1px solid #000; text-align: center;">Brazil</td></tr><tr><td style="border: 1px solid #000; text-align: center;">13</td><td style="border: 1px solid #000; text-align: center;">Brasília</td><td style="border: 1px solid #000; text-align: center;">DF</td><td style="border: 1px solid #000; text-align: center;">Brazil</td></tr><tr><td style="border: 1px solid #000; text-align: center;">16</td><td style="border: 1px solid #000; text-align: center;">Mountain View</td><td style="border: 1px solid #000; text-align: center;">CA</td><td style="border: 1px solid #000; text-align: center;">USA</td></tr><tr><td style="border: 1px solid #000; text-align: center;">17</td><td style="border: 1px solid #000; text-align: center;">Redmond</td><td style="border: 1px solid #000; text-align: center;">WA</td><td style="border: 1px solid #000; text-align: center;">USA</td></tr><tr><td style="border: 1px solid #000; text-align: center;">18</td><td style="border: 1px solid #000; text-align: center;">New York</td><td style="border: 1px solid #000; text-align: center;">NY</td><td style="border: 1px solid #000; text-align: center;">USA</td></tr><tr><td style="border: 1px solid #000; text-align: center;">19</td><td style="border: 1px solid #000; text-align: center;">Cupertino</td><td style="border: 1px solid #000; text-align: center;">CA</td><td style="border: 1px solid #000; text-align: center;">USA</td></tr><tr><td style="border: 1px solid #000; text-align: center;">20</td><td style="border: 1px solid #000; text-align: center;">Mountain View</td><td style="border: 1px solid #000; text-align: center;">CA</td><td style="border: 1px solid #000; text-align: center;">USA</td></tr><tr><td style="border: 1px solid #000; text-align: center;">21</td><td style="border: 1px solid #000; text-align: center;">Reno</td><td style="border: 1px solid #000; text-align: center;">NV</td><td style="border: 1px solid #000; text-align: center;">USA</td></tr><tr><td style="border: 1px solid #000; text-align: center;">22</td><td style="border: 1px solid #000; text-align: center;">Orlando</td><td style="border: 1px solid #000; text-align: center;">FL</td><td style="border: 1px solid #000; text-align: center;">USA</td></tr><tr><td style="border: 1px solid #000; text-align: center;">23</td><td style="border: 1px solid #000; text-align: center;">Boston</td><td style="border: 1px solid #000; text-align: center;">MA</td><td style="border: 1px solid #000; text-align: center;">USA</td></tr><tr><td style="border: 1px solid #000; text-align: center;">24</td><td style="border: 1px solid #000; text-align: center;">Chicago</td><td style="border: 1px solid #000; text-align: center;">IL</td><td style="border: 1px solid #000; text-align: center;">USA</td></tr><tr><td style="border: 1px solid #000; text-align: center;">25</td><td style="border: 1px solid #000; text-align: center;">Madison</td><td style="border: 1px solid #000; text-align: center;">WI</td><td style="border: 1px solid #000; text-align: center;">USA</td></tr><tr><td style="border: 1px solid #000; text-align: center;">26</td><td style="border: 1px solid #000; text-align: center;">Fort Worth</td><td style="border: 1px solid #000; text-align: center;">TX</td><td style="border: 1px solid #000; text-align: center;">USA</td></tr><tr><td style="border: 1px solid #000; text-align: center;">27</td><td style="border: 1px solid #000; text-align: center;">Tucson</td><td style="border: 1px solid #000; text-align: center;">AZ</td><td style="border: 1px solid #000; text-align: center;">USA</td></tr><tr><td style="border: 1px solid #000; text-align: center;">28</td><td style="border: 1px solid #000; text-align: center;">Salt Lake City</td><td style="border: 1px solid #000; text-align: center;">UT</td><td style="border: 1px solid #000; text-align: center;">USA</td></tr></tbody></table>


### 1.1.2. `build()` method

As name suggests - `build` method builds query. In context of table API it is not used by the developer directly, but it is nice to have. It returns parametrized args in right order to pass to `sqlite3` execute or fetch methods. `build` output from above would look like this:

```py
...join("OR").build()

# ->
('(((Country = ? OR Country = ?) AND State != ?) OR City = ?)',
 ('Brazil', 'USA', 'SP', 'Vienne'))
```


### 1.1.3. `then` property

`then` is a link to the above statement that can execute, aggregate, fetch, etc.

```py
table.select.where.eq("Country", "USA").then.fetchone()
```

## 1.2. Select

Select is non-mutational statement (i.e. it does not mutate table contents). Syntax is quite simple. In broad terms it can be described as: 

`select("Column1", "Column2", ...).order_by("Column").limit(n).fetchall()`

It will return list of rows, and each row will be in order of provided columns.

### 1.2.1. `limit(n : int)` op

`limit` *limits* number of returned rows to `n`

```py
table.select("SupportRepId","Email").limit(5)
```

<table style="border-collapse: collapse; font-size: 14px;"><caption style="font-size: 18px; font-weight: bold;">Customer</caption><thead><tr><td style="border: 1px solid #555; text-align: center;">SupportRepId</td><td style="border: 1px solid #555; text-align: center;">Email</td></tr></thead><tbody><tr><td style="border: 1px solid #000; text-align: center;">3</td><td style="border: 1px solid #000; text-align: center;">luisg@embraer.com.br</td></tr><tr><td style="border: 1px solid #000; text-align: center;">5</td><td style="border: 1px solid #000; text-align: center;">leonekohler@surfeu.de</td></tr><tr><td style="border: 1px solid #000; text-align: center;">3</td><td style="border: 1px solid #000; text-align: center;">ftremblay@gmail.com</td></tr><tr><td style="border: 1px solid #000; text-align: center;">4</td><td style="border: 1px solid #000; text-align: center;">bjorn.hansen@yahoo.no</td></tr><tr><td style="border: 1px solid #000; text-align: center;">4</td><td style="border: 1px solid #000; text-align: center;">frantisekw@jetbrains.com</td></tr></tbody></table>

### 1.2.2. `order_by(column : str, ascending : bool = True)` op

`order_by` will return rows in `ascending` or `descending` order of provided column.

```py
table.select("SupportRepId","Email").order_by("SupportRepId").limit(5)
```

<table style="border-collapse: collapse; font-size: 14px;"><caption style="font-size: 18px; font-weight: bold;">Customer</caption><thead><tr><td style="border: 1px solid #555; text-align: center;">SupportRepId</td><td style="border: 1px solid #555; text-align: center;">Email</td></tr></thead><tbody><tr><td style="border: 1px solid #000; text-align: center;">3</td><td style="border: 1px solid #000; text-align: center;">luisg@embraer.com.br</td></tr><tr><td style="border: 1px solid #000; text-align: center;">3</td><td style="border: 1px solid #000; text-align: center;">ftremblay@gmail.com</td></tr><tr><td style="border: 1px solid #000; text-align: center;">3</td><td style="border: 1px solid #000; text-align: center;">roberto.almeida@riotur.gov.br</td></tr><tr><td style="border: 1px solid #000; text-align: center;">3</td><td style="border: 1px solid #000; text-align: center;">jenniferp@rogers.ca</td></tr><tr><td style="border: 1px solid #000; text-align: center;">3</td><td style="border: 1px solid #000; text-align: center;">michelleb@aol.com</td></tr></tbody></table>


### 1.2.3. `aggregate(by : Literal['COUNT', 'SUM', 'AVG', 'MIN', 'MAX'])` op

`aggregate` *aggregates* by provided method. Note: `_repr_html_` does not work if `aggregate` was used and will fallback to `__repr__`.

```py
table.select("Email").where.eq("SupportRepId", 3).then.aggregate("COUNT").fetchone() # -> (21,)
```

### 1.2.4. `fetch()` methods

There are 3 fetch methods: `fetchone() -> SqlRow`, `fetchmany(size : int = 1) -> list[SqlRow]` and `fetchall() -> list[SqlRow]`. They are used at the end of the statement to finally execute built expression

```py
len(table.select("SupportRepId","Email").fetchone())    # -> 2 (len of values)
len(table.select("SupportRepId","Email").fetchmany(10)) # -> 10 (len of rows)
len(table.select("SupportRepId","Email").fetchall())    # -> 59 (len of rows)
```

### 1.2.5. `__iter__` methods

There are 2 iter methods: classical `__iter__` that lets you iterate over rows and `fetchmany_iterator(batch_size : int)`. Both of them are available if table is in [transaction](https://github.com/suffermuffin/SQL-Engine/blob/main/docs/transactions.md) mode. Otherwise `RuntimeError` will be raised.


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

## 1.3. Update

Update is used to change values inside the table, therefore it is a mutational statement.

### 1.3.1. `set(column : str, value : SqlValue)` op

Set's `column` to provided `value`. You can use `where` builder to specify criteria for which rows this value must be changed.

```py
# This will move all residences of Czech Republic to Karaganda
table.update.set("City", "Karaganda").where.eq("Country", "Czech Republic").then.execute()
```

### 1.3.2. `execute()` method

This will build and execute the statement

## 1.4. Delete

Delete statement deletes rows based on where clause. You must specify at least one `where` operation.

```py
table.delete.where.eq("ID", 0).then.execute()
```

### 1.4.1. `execute()` method

This will (once again) build and execute the statement