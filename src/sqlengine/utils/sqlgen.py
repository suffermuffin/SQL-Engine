from typing import Sequence, Literal, Sequence
from .types import SqlValue


def format_list(items : Sequence | set, brakets : bool = True) -> str:
    """ Formats list into `(item1, item2, ...)` format """
    items_str = ', '.join([str(i) for i in items])
    if not brakets: 
        return items_str
    return f'({items_str})'


def create_table(
        tablename: str,
        columns  : list[str],
        types    : list[str],
        primary  : list[str]
    ) -> str:

    columns_types = format_list(
        [
            f'{col} {dtype}' for col, dtype 
            in zip(columns, types)
        ], 
        False
    )
    
    primary_keys = format_list(primary)
    
    return (
        f"CREATE TABLE IF NOT EXISTS {tablename} "
        f"({columns_types}, PRIMARY KEY {primary_keys});"
    )


def drop_table(tablename : str) -> str:
    return f"DROP TABLE IF EXISTS {tablename}"


def values_placeholder(n_values : int) -> str:
    """ Creates placeholder `(?, ?, ?, ...)` with "?" `n_values` times """
    return format_list(['?']*n_values)


def bulk_placeholder(n_values : int, n_rows : int) -> str:
    """ Creates placeholders `(?, ?, ..), (?, ?, ..), ...` for each row """
    place_holder = values_placeholder(n_values)
    return f"{format_list([place_holder]*n_rows, False)}"


def insert(tablename : str, columns : list[str], values : str):
    """ Creates insert query """
    return f"INSERT INTO {tablename} {format_list(columns)} VALUES {values};"


def insert_row(tablename : str, columns : list[str]) -> str:
    """ Creates insert query for 1 row """
    n_values = len(columns)
    placeholder = values_placeholder(n_values)
    return insert(tablename, columns, placeholder)


def insert_many(tablename : str, columns : list[str], n_rows : int) -> str:
    """ Creates insert query for multiple rows """
    n_values = len(columns)
    placeholder = bulk_placeholder(n_values, n_rows)
    return insert(tablename, columns, placeholder)


def delete_rows(tablename : str, where_clause : str) -> str:
    return f"DELETE FROM {tablename} WHERE {where_clause};"


def where(
        column   : str, 
        operator : Literal["=", "!=", "<", ">", "<=", ">=", "IN"],
        values   : SqlValue | Sequence[SqlValue], 
    ) -> str:
    
    fmt = lambda x: x if not isinstance(x, str) else f"\"{x}\""
    
    if not isinstance(values, (list, tuple)):
        return f'{column} {operator} {fmt(values)}'
    
    val_list = [fmt(val) for val in values]

    return f'{column} {operator} {format_list(val_list)}'


def where_equals(column : str, equals : SqlValue | Sequence[SqlValue]) -> str:
    """ Generates where clause for provided `equals` values to look for equal values in `column` """
    
    if not isinstance(equals, (list, tuple)):
        return where(column, "=", equals)
    
    return where(column, "IN", equals)


def select(
      tablename   : str,
      columns     : str | list[str] = "*",
      where_clause: str | None      = None,
      order_by    : str | None      = None
    ) -> str: 
    """ Creates select query """
    
    _columns = columns if isinstance(columns, str) else format_list(columns, False)
    
    query  = f"SELECT {_columns} FROM {tablename}"
    query += f" WHERE {where_clause}" if where_clause else ""
    query += f" ORDER BY {order_by}" if order_by else ""
    query += ";"

    return query


def count(tablename : str, columns : str | list[str] = "*", where_clause : str | None = None) -> str:
    _columns = columns if isinstance(columns, str) else format_list(columns, False)
    return select(tablename, f"COUNT({_columns})", where_clause)


def update(tablename : str, where_clause : str, set_values : dict[str, SqlValue]) -> str:
    """ 
    Creates query to update columns based on `where_clause` 
    
    Args:
        tablename (str): name of the table in db
        where_clause (str): describes search filter with SQL condition query
        set_values (dict[str, SqlValue]): dict where keys are column names and
            values are corresponding new values to set
        
    """
    new_sets = [
        where(column, "=", new_value)
        for column, new_value in set_values.items()
    ]

    set_to = format_list(new_sets, False)

    return f"UPDATE {tablename} SET {set_to} WHERE {where_clause};"


def upsert(tablename : str, columns : list[str], primary_key : list[str]) -> str:
    """
    Creates query to upsert (update or insert) row based on `primary_key`

    Args:
        tablename (str): name of the table in db
        columns (list[str]): list of table columns
        primary_key (list[str]): list of primary keys
    """
    placeholder  = values_placeholder(len(columns))
    non_primary  = set(columns) - set(primary_key)
    updated_list = [f"{col}=excluded.{col}" for col in non_primary]
    
    columns_str = format_list(columns)
    updated_str = format_list(updated_list, False)
    primary_str = format_list(primary_key)

    query = (
        f"INSERT INTO {tablename} {columns_str} VALUES {placeholder} "
        f"ON CONFLICT {primary_str} "
        f"DO UPDATE SET {updated_str};"
    )
    return query


def max_value(tablename : str, column : str, where_clause : str | None = None) -> str:
    query  = f"SELECT MAX({column}) FROM {tablename}"
    query += f" WHERE {where_clause}" if where_clause else ""
    query += ";"
    return query


def min_value(tablename : str, column : str, where_clause : str | None = None) -> str:
    query  = f"SELECT MIN({column}) FROM {tablename}"
    query += f" WHERE {where_clause}" if where_clause else ""
    query += ";"
    return query


