# Custom Types

You can define non-native sqlite datatypes. They have to implement `to_sql` and `from_sql`. There are other ways that `sqlite3` [documentation](https://docs.python.org/3/library/sqlite3.html#how-to-adapt-custom-python-types-to-sqlite-values) explains.


```py
class Point:
    def __init__(self, x : float, y : float) -> None:
        self.x = x
        self.y = y

    def to_sql(self) -> str:
        """ Method that converts object instance to native sqlite3 value """
        return f"{self.x}, {self.y}"
    
    @classmethod
    def from_sql(cls, sql : bytes):
        """ Method that accepts bytes and returns object instance """
        x, y = list(map(float, sql.split(b",")))
        return cls(x, y)
    
```

Then to use this type in table - declare it in `__types__` attribute or via annotation

```py
class Coordinates(SqlTableMixin):

    __columns__ = ["ID", "name", "coords", "temp"]
    __types__   = [int, str, Point, float]
    __primary__ = ["ID"]

    def insert(self, id : int, name : str, coords : Point, temp : float) -> None:
        return super().insert(id, name, coords, temp)
    
    def upsert(self, id : int, name : str, coords : Point, temp : float) -> None:
        return super().upsert(id, name, coords, temp)
```

Should work just right

```py
table = Coordinates("temp/data.db")

table.upsert(0, "Kazahstan", Point(43.2380, 76.8829), 14)
point = table.select("coords").where.eq("ID", 0).then.fetchone()[0]

isinstance(point, Point) # -> True
```

Another example uses inheritance to modify existing type to be compatible with the protocol:

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

# uses `datetime` classmethod to
# create a `DateTime` object that
# is compatible with sqlite3
time = DateTime.now()
table.insert(1, "loft_1", time, None)
```