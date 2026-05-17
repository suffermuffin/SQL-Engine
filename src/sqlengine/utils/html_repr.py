from html import escape
from typing import Sequence

from .types import SqlRow


def repr_html(tablename : str, columns : list[str], repr_rows : Sequence[SqlRow], limit : int = 25):

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