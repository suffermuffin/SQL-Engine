import csv

from ..sqltable import SqlTableMixin
from ..core.statements import Where, Select


def to_csv(builder : Select | Where[Select] | SqlTableMixin, path : str) -> None:
    
    match builder:
        case Where():
            builder = builder.then
        case SqlTableMixin():
            builder = builder.select

    if builder._aggregate:
        raise AssertionError("Aggregated queries are not supported")
    
    columns   = builder._resolve_columns()
    repr_rows = builder.fetchall()

    with open(path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        writer.writerows(repr_rows)