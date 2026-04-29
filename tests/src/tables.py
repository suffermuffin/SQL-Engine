import sqlite3
from typing import Literal

from sqlengine import SqlTableMixin, Schema


class Point:
    def __init__(self, x : float, y : float) -> None:
        self.x = x
        self.y = y

    def to_sql(self) -> str:
        return f"{self.x}, {self.y}"
    
    @classmethod
    def from_sql(cls, sql : bytes):
        x, y = list(map(float, sql.split(b",")))
        return cls(x, y)
    
    def __repr__(self) -> str:
        return f"Point({self.to_sql()})"


class Employees(SqlTableMixin):

    __tablename__ : str       = "MyDB"
    __columns__   : list[str] = ["ID", "name", "surname", "salary", "position"]
    __types__     : list[str] = ["INT", "TEXT", "TEXT", "FLOAT", "TEXT"]
    __primary__   : list[str] = ["ID", "name"]


class Coordinates(SqlTableMixin):

    __columns__   : list[str] = ["ID", "name", "coords", "temp"]
    __types__     : list[str] = ["INT", "TEXT", "POINT", "FLOAT"]
    __primary__   : list[str] = ["ID"]


    def __init__(self, database: str | Literal[':memory:'], force_drop: bool = False, **connection_params) -> None:
        sqlite3.register_adapter(Point, lambda x: x.to_sql())
        sqlite3.register_converter("POINT", Point.from_sql)
        super().__init__(database, force_drop, detect_types=sqlite3.PARSE_DECLTYPES, **connection_params)


coord_schema : Schema = {
    "tablename": "Coordinates",
    "columns"  : ["ID", "name", "coords", "temp"],
    "types"    : ["INT", "TEXT", "POINT", "FLOAT"],
    "primary"  : ["ID"],
}