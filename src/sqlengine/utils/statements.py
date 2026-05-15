from typing import Sequence, Literal
from abc import ABC, abstractmethod

from . import sqlgen as sql
from .types import SqlValue


class Where:

    def __init__(self): 
        self.clause   : list[str] = []
        self.args     : list[SqlValue] = []
    
    def op(self, column : str, value : SqlValue, operator : str):
        self.clause.append(f"{column} {operator} ?")
        self.args.append(value)
        return self

    def join(self, lop : str = "AND"):
        joined = f" {lop} ".join(self.clause)
        self.clause = [f"({joined})"]
        return self

    def eq(self, column : str, value : SqlValue):
        return self.op(column, value, "=")

    def neq(self, column : str, value : SqlValue):
        return self.op(column, value, "!=")
    
    def gt(self, column : str, value : SqlValue):
        return self.op(column, value, ">")
    
    def gte(self, column : str, value : SqlValue):
        return self.op(column, value, ">=")
    
    def lt(self, column : str, value : SqlValue):
        return self.op(column, value, "<")
    
    def lte(self, column : str, value : SqlValue):
        return self.op(column, value, "<=")
    
    def inside(self, column : str, values : Sequence[SqlValue]):
        placeholder = sql.values_placeholder(len(values))
        self.clause.append(f"{column} IN {placeholder}")
        self.args.extend(values)
        return self
    
    def between(self, column : str, start : SqlValue, stop : SqlValue):
        self.clause.append(f"{column} BETWEEN ? AND ?")
        self.args.extend((start, stop))
        return self
    
    def custom(self, where_clause : str, *args : SqlValue):
        self.clause.append(where_clause)
        self.args.extend(args)
        return self

    def build(self, lop : str = "AND"):
        where_clause = f" {lop} ".join(self.clause).strip()
        args = tuple(self.args)
        return where_clause, args
    
    def reset(self):
        self.args = []
        self.clause = []
    
    def __len__(self):
        return len(self.args)
    

class Statement(ABC):

    __command__ : Literal["SELECT", "INSERT", "UPDATE", "DELETE"]

    def __init__(self, tablename : str) -> None:
        super().__init__()

        self.tablename = tablename
        self._where = Where()

        self._custom_query : str | None = None
        self._custom_args  : tuple[SqlValue, ...] = ()
    
    
    def custom_query(self, query : str, *args):
        self._custom_query = query
        self._custom_args  = args
        return None
    

    def build(self) -> tuple[str, tuple[SqlValue, ...]]:
        if self._custom_query:
            return self._custom_query, self._custom_args
        
        where_clause, args = self._where.build()
        query, args = self._build(where_clause, *args)
        return query, args
    

    def reset(self):
        self._where.reset()
        self._custom_query = None
        self._custom_args = ()
        self._reset()
    
    
    @abstractmethod
    def _build(self, where_clause : str, *args : SqlValue) -> tuple[str, tuple[SqlValue, ...]]:
        pass

    
    @abstractmethod
    def _reset(self):
        pass


    @property
    def where(self):
        return self._where


class Select(Statement):

    def __init__(self, tablename : str) -> None:
        super().__init__(tablename)
        
        self._columns   : list[str] = []
        self._order_by  : list[str] = []
        self._aggregate : str | None = None


    def __call__(self, *columns : str):
        self._columns.extend(columns)
        return self
    
    
    def aggregate(self, agr : Literal['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']):
        
        if self._aggregate:
            raise ValueError("Can't aggregate columns multiple times")
        
        self._aggregate = agr
        return self

    
    def order_by(self, column : str, ascending : bool = True):
        order = "ASC" if ascending else "DESC"
        self._order_by.append(f"{column} {order}")
        return self
    

    def is_active(self):
        return bool(self._columns or self._order_by or self._aggregate or self._custom_query or self._custom_args)
        
    
    def _build(self, where_clause : str, *args : SqlValue):

        order   = sql.format_list(self._order_by, brakets=False)
        columns = sql.format_list(self._columns,  brakets=False)
        
        columns = "*" if not columns else columns
        columns = columns if not self._aggregate else f"{self._aggregate}({columns})"

        query = sql.select(self.tablename, columns, where_clause, order)
        
        self.__init__(self.tablename)
        return query, args
    
    
    def _reset(self):
        self._columns = []
        self._order_by = []
        self._aggregate = None
    

class Delete(Statement):

    __command__ = "DELETE"

    def _build(self, where_clause : str, *args : SqlValue):
        
        if not where_clause:
            raise ValueError("Delete statement must have a where clause")
        
        query = sql.delete_rows(self.tablename, where_clause)
        return query, args
    
    def _reset(self):
        pass
    

class Update(Statement):

    __command__ = "UPDATE"

    def __init__(self, tablename : str) -> None:
        super().__init__(tablename)
        self._set_clauses : list[str] = []
        self._set_args    : list[SqlValue] = []


    def set(self, column : str, value : SqlValue):
        self._set_clauses.append(f"{column} = ?")
        self._set_args.append(value)
        return self


    def _build(self, where_clause : str, *args : SqlValue):
        set_clause = sql.format_list(self._set_clauses, brakets=False)
        query = f"UPDATE {self.tablename} SET {set_clause} WHERE {where_clause};"
        return query, tuple(self._set_args + list(args))
    
    
    def _reset(self):
        self._set_clauses = []
        self._set_args = []