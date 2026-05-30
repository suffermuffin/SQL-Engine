from sqlengine import SqlTableMixin, Schema, Primary


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
    
    def __eq__(self, value: object) -> bool:
        if not isinstance(value, Point):
            raise ValueError(f"Can't compare Point to {type(value)}")
        return (self.x == value.x) and (self.y == value.y)


class Employees(SqlTableMixin):

    __tablename__ = "MyDB"

    ID       : Primary[int]
    name     : Primary[str]
    surname  : str | None
    salary   : float
    position : str


class Coordinates(SqlTableMixin):

    __primary__   = ["ID"]

    ID     : int
    name   : str | None
    coords : Point
    temp   : float

    _my_local_params : dict


coord_schema : Schema = {
    "tablename": "Coordinates",
    "columns"  : ["ID", "name", "coords", "temp"],
    "types"    : [int, str, Point, float],
    "primary"  : ["ID"],
}


EMPLOYEES_DATA = [
    (0, "test0", "surname0", 100.0, "pos0"),
    (0, "test1", "surname0", 100.0, "pos0"),
    (1, "test0", "surname1", 111.1, "pos1"),
    (1, "test1", "surname1", 111.1, "pos1"),
    (2, "test2", "surname2", 122.2, "pos2"),
    (3, "test3", "surname3", 133.3, "pos3"),
    (4, "test4", "surname4", 144.4, "pos4"),
]


COORDS_DATA = [
    (0,  "loc0",  Point(0,0),   0.0),
    (1,  "loc1",  Point(1,1),   1.1),
    (2,  "loc0",  Point(2,2),   2.2),
    (3,  "loc3",  Point(3,3),   3.3),
    (4,  "loc4",  Point(4,4),   4.4),
    (5,  "loc5",  Point(5,5),   5.5),
    (6,  "loc6",  Point(6,6),   6.6),
    (7,  "loc7",  Point(7,7),   7.7),
    (10, "loc10", Point(10,10), 10.10),
]
