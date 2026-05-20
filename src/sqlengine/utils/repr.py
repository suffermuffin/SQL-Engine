from __future__ import annotations
from html import escape
from typing import Sequence, TYPE_CHECKING

import csv

if TYPE_CHECKING:
    from .statements import Select, Where
    from ..sqltable  import SqlTableMixin

from .types import SqlRow


def to_html(tablename : str, columns : list[str], repr_rows : Sequence[SqlRow], limit : int = 25) -> str:

    tablestyle     = "<table style=\"border-collapse: collapse; font-size: 14px;\">"
    tablenamestyle = "<caption style=\"font-size: 18px; font-weight: bold;\">{}</caption>"
    colstyle       = "<td style=\"border: 1px solid #555; text-align: center;\">{}</td>"
    cellstyle      = "<td style=\"border: 1px solid #000; text-align: center;\">{}</td>"
    morestyle      = "<td colspan=\"{}\" style=\"text-align:center;color:#888;font-style:italic;padding:8px;\">... more rows ...</td>"
    
    cols      = [colstyle.format(escape(col)) for col in columns]
    tablename = tablenamestyle.format(escape(tablename))
    more      = morestyle.format(len(columns))

    html = [
        tablestyle,
        tablename,
            "<thead>",
                "<tr>",
                    *cols,
                "</tr>",
            "</thead>",
        "<tbody>",
    ]

    for row in repr_rows[:limit]:
        values = [cellstyle.format(escape(str(val))) for val in row]
        
        html.extend([
            "<tr>",
                *values,
            "</tr>"
        ])
    
    if len(repr_rows) > limit:
        html.extend([
        "<tr>",
            more,
        "</tr>"
      ])
    
    html.extend([
        "</tbody>",
        "</table>",
    ])

    return "".join(html)


def to_csv(builder : Select | Where[Select] | SqlTableMixin, path : str) -> None:
    
    from ..sqltable  import SqlTableMixin
    from .statements import Where

    match builder:
        case Where():
            builder = builder.then
        case SqlTableMixin():
            builder = builder.select

    if builder._aggregate:
        raise AssertionError("Aggregated queries are not supported")
    
    columns   = builder._table.columns if len(builder._columns) == 0 or "*" in builder._columns else builder._columns
    repr_rows = builder.fetchall()

    with open(path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        writer.writerows(repr_rows)