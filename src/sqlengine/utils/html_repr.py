from html import escape
from typing import Sequence

from .types import SqlRow


def repr_html(tablename : str, columns : list[str], repr_rows : Sequence[SqlRow]):

    tablestyle     = "<table style=\"border-collapse: collapse; font-size: 14px;\">"
    tablenamestyle = "<caption style=\"font-size: 18px; font-weight: bold;\">{}</caption>"
    colstyle       = "<td style=\"border: 1px solid #555; text-align: center;\">{}</td>"
    cellstyle      = "<td style=\"border: 1px solid #000; text-align: center;\">{}</td>"
    
    cols      = [colstyle.format(escape(col)) for col in columns]
    tablename = tablenamestyle.format(escape(tablename))

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

    for row in repr_rows:
        values = [cellstyle.format(escape(str(val))) for val in row]
        html.extend([
            "<tr>",
              *values,
            "</tr>"
        ])

    html.extend([
        "</tbody>",
        "</table>",
    ])

    return "".join(html)