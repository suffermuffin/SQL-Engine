import csv
from typing import Generator

from ..sqltable import SqlTableMixin
from .._internal.statements import Where, Select
from .._internal.types import SqlValue
from ..exceptions import OutsideTransactionError, SqlEngineError


def _get_select(builder : Select | Where[Select] | SqlTableMixin) -> Select:
    
    match builder:
        case Where():
            return builder.then
        case SqlTableMixin():
            return builder.select
        case Select():
            return builder
        case _:
            raise TypeError(f"Unexpected builder type {type(builder)}")


def to_csv(
        builder : Select | Where[Select] | SqlTableMixin, 
        filename : str, 
        stream_batch_size : int | None = None
    ) -> None:
    """ 
    Writes query or whole table to a csv file
    
    Args:
        builder (Select | Where[Select] | SqlTableMixin): Object to convert to csv
        filename (str): Path to write to
        stream_batch_size (int | None): If not None or 0, will stream all rows to csv in batches of provided size
    """
    
    builder = _get_select(builder)

    if builder._aggregate:
        raise SqlEngineError("Aggregated queries are not supported")
    
    if stream_batch_size and not builder._connection.in_transaction():
        raise OutsideTransactionError("To stream to csv you have to keep open the `transaction`")
    
    columns = builder._resolve_columns()
    
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        
        if not stream_batch_size:
            repr_rows = builder.fetchall()
            writer.writerows(repr_rows)
            return
        
        for rows in builder.fetchmany_iterator(stream_batch_size):
            writer.writerows(rows)


def to_dicts(builder : Select | Where[Select] | SqlTableMixin) -> list[dict[str, SqlValue]]:
    """ 
    Converts query or whole table to pandas friendly format

    Args:
        builder (Select | Where[Select] | SqlTableMixin): Object to convert to list of dicts

    Returns:
        out (list[dict[str, SqlValue]]): list of rows mappings
    
    Example:

    ```python
    import pandas as pd
    
    df = pd.DataFrame(to_dicts(table))
    ```
    """
    
    builder = _get_select(builder)

    if builder._aggregate:
        raise SqlEngineError("Aggregated queries are not supported")
    
    columns = builder._resolve_columns()
    rows = builder.fetchall()

    return [dict(zip(columns, row)) for row in rows]


def to_dicts_stream(
        builder : Select | Where[Select] | SqlTableMixin, 
        batch_size : int
    ) -> Generator[list[dict[str, SqlValue]], None, None]:
    """
    Converts query or whole table to pandas friendly format and yields it in batches

    Args:
        builder (Select | Where[Select] | SqlTableMixin): Object to convert to list of dicts
        batch_size (int): Size of each yielded batch

    Yields:
        batch (list[dict[str, SqlValue]]): list of rows mappings
    
    Example:

    ```python
    import pandas as pd
    
    df = pd.DataFrame(columns=table.columns)
    
    with table.transaction():
        for batch in to_dicts_stream(table, 100):
            df = pd.concat([df, pd.DataFrame(batch)], axis=0)
    
    df.set_index("ID", inplace=True)
    ```
    """
    
    builder = _get_select(builder)

    if builder._aggregate:
        raise SqlEngineError("Aggregated queries are not supported")
    
    if not builder._connection.in_transaction():
        raise OutsideTransactionError("To stream to csv you have to keep open the `transaction`")
    
    columns = builder._resolve_columns()

    for rows in builder.fetchmany_iterator(batch_size):
        yield [dict(zip(columns, row)) for row in rows]