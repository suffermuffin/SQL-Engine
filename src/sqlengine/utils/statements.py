from __future__ import annotations
from typing     import Sequence, Literal, Generator, Self, TYPE_CHECKING
from abc        import ABC, abstractmethod

if TYPE_CHECKING:
    from .statements import Statement 
    from ..sqltable  import SqlTableMixin

from . import sqlgen as sql
from .types import SqlValue, SqlRow
from .html_repr import repr_html


class Where[T : Statement]:

    def __init__(self, statement : T):
        
        self._statement = statement

        self.clause   : list[str] = []
        self.args     : list[SqlValue] = []

    @property
    def then(self) -> T:
        return self._statement
    
    def op(self, column : str, value : SqlValue, operator : str) -> Self:
        self.clause.append(f"{column} {operator} ?")
        self.args.append(value)
        return self

    def join(self, lop : str = "AND") -> Self:
        joined = f" {lop} ".join(self.clause)
        self.clause = [f"({joined})"]
        return self

    def eq(self, column : str, value : SqlValue) -> Self:
        return self.op(column, value, "=")

    def neq(self, column : str, value : SqlValue) -> Self:
        return self.op(column, value, "!=")
    
    def gt(self, column : str, value : SqlValue) -> Self:
        return self.op(column, value, ">")
    
    def gte(self, column : str, value : SqlValue) -> Self:
        return self.op(column, value, ">=")
    
    def lt(self, column : str, value : SqlValue) -> Self:
        return self.op(column, value, "<")
    
    def lte(self, column : str, value : SqlValue) -> Self:
        return self.op(column, value, "<=")
    
    def in_(self, column : str, values : Sequence[SqlValue]) -> Self:
        if isinstance(values, str):
            raise ValueError("Got string as sequence of values in in_, expected tuple/list/etc...")
        placeholder = sql.values_placeholder(len(values))
        self.clause.append(f"{column} IN {placeholder}")
        self.args.extend(values)
        return self
    
    def between(self, column : str, start : SqlValue, stop : SqlValue) -> Self:
        self.clause.append(f"{column} BETWEEN ? AND ?")
        self.args.extend((start, stop))
        return self
    
    def custom(self, where_clause : str, *args : SqlValue) -> Self:
        self.clause.append(where_clause)
        self.args.extend(args)
        return self

    def build(self, lop : str = "AND") -> tuple[str, tuple[SqlValue, ...]]:
        where_clause = f" {lop} ".join(self.clause).strip()
        args = tuple(self.args)
        return where_clause, args
    
    def reset(self):
        self.args = []
        self.clause = []

    def __str__(self) -> str:
        return self._statement.__str__()

    def __repr__(self) -> str:
        return self._statement.__repr__()

    def _repr_html_(self):
        if isinstance(self._statement, Select):
            return self._statement._repr_html_()
        return None
    
    def __len__(self) -> int:
        return len(self.args)


class Statement(ABC):

    __command__ : Literal["SELECT", "INSERT", "UPDATE", "DELETE"]

    def __init__(self, table : SqlTableMixin) -> None:

        self._table = table
        self._where = Where(self)

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
        if self.__command__ == "INSERT":
            raise AttributeError("INSERT statement does not have where clause")
        return self._where
    

    def __repr__(self) -> str:
        query, _ = self.build()
        return f"{self.__command__}: {query}"
    

    def __str__(self) -> str:
        query, _ = self.build()
        return query
    

class MutationalStatement(Statement, ABC):

    def execute(self):
        query, args = self.build()
        self._table.execute(query, *args)


class Select(Statement):

    __command__ = "SELECT"

    def __init__(self, table : SqlTableMixin) -> None:
        super().__init__(table)
        
        self._columns   : list[str] = []
        self._order_by  : list[str] = []
        self._aggregate : str | None = None
        self._limit     : int | None = None


    def __call__(self, *columns : str) -> Self:
        self._columns.extend(columns)
        return self
    
    
    def aggregate(self, agr : Literal['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']) -> Self:
        
        if self._aggregate:
            raise ValueError("Can't aggregate columns multiple times")
        
        self._aggregate = agr
        return self

    
    def order_by(self, column : str, ascending : bool = True) -> Self:
        order = "ASC" if ascending else "DESC"
        self._order_by.append(f"{column} {order}")
        return self
    

    def limit(self, n : int) -> Self:
        self._limit = n
        return self
    

    def fetchone(self) -> SqlRow:
        query, args = self.build()
        return self._table.fetchone(query, *args)
        

    def fetchmany(self, size : int = 1) -> list[SqlRow]:
        query, args = self.build()
        return self._table.fetchmany(query, *args, size=size)

    
    def fetchall(self) -> list[SqlRow]:
        query, args = self.build()
        return self._table.fetchall(query, *args)
    

    def fetchmany_iterator(self, batch_size: int) -> Generator[list[SqlRow], None, None]:
        """
        Yields all rows in batches, each batch in its own transaction.
        
        Args:
            batch_size (int): Size of each batch

        Examples:

            >>> with table.transaction():
            >>>     for batch in table.select.where.gt("Age", 30).then.fetchmany_iterator(1000):
            >>>         process_batch(batch)
        """
        if not self._table.in_transaction():
            raise RuntimeError("To use the `fetchall_iterator()` method you have \
                    to keep open the transaction of the table with `transaction()` manager")
        
        query, exec_args = self.build()

        iter_cursor = self._table.tx_conn.cursor()
        iter_cursor.execute(query, exec_args)

        while batch := iter_cursor.fetchmany(batch_size):
            yield batch

    
    def __iter__(self) -> Generator[SqlRow, None, None]:
        """ Database rows iterator """
        
        if not self._table.in_transaction():
            raise RuntimeError("To use the __iter__ method you have \
                to keep open the transaction of the table with `transaction()` manager")
        
        query, exec_args = self.build()
        
        iter_cursor = self._table.tx_conn.cursor()
        iter_cursor.execute(query, exec_args)

        while row := iter_cursor.fetchone(): 
            yield row
    

    def _build(self, where_clause : str, *args : SqlValue):

        order   = sql.format_list(self._order_by, brakets=False)
        columns = sql.format_list(self._columns,  brakets=False)
        
        columns = "*" if not columns else columns
        columns = columns if not self._aggregate else f"{self._aggregate}({columns})"

        if self._limit:
            limit = "?"
            args  = (*args, self._limit)
        
        else:
            limit = None
        
        query = sql.select(self._table.tablename, columns, where_clause, order, limit)
        
        return query, args
    
    
    def _reset(self):
        self._columns   = []
        self._order_by  = []
        self._aggregate = None
        self._limit     = None


    def _repr_html_(self):
        
        if self._aggregate:
            return None
        
        limit     = 26
        columns   = self._table.columns if len(self._columns) == 0 or "*" in self._columns else self._columns
        repr_rows = self.fetchmany(limit)

        return repr_html(self._table.tablename, columns, repr_rows, limit=limit-1)
    

class Delete(MutationalStatement):

    __command__ = "DELETE"

    def _build(self, where_clause : str, *args : SqlValue):
        
        if not where_clause:
            raise ValueError("Delete statement must have a where clause")
        
        query = sql.delete_rows(self._table.tablename, where_clause)
        return query, args
    
    def _reset(self):
        pass
    

class Update(MutationalStatement):

    __command__ = "UPDATE"

    def __init__(self, table : SqlTableMixin) -> None:
        super().__init__(table)
        self._set_clauses : list[str] = []
        self._set_args    : list[SqlValue] = []


    def set(self, column : str, value : SqlValue):
        self._set_clauses.append(f"{column} = ?")
        self._set_args.append(value)
        return self


    def _build(self, where_clause : str, *args : SqlValue):
        set_clause = sql.format_list(self._set_clauses, brakets=False)
        query = f"UPDATE {self._table.tablename} SET {set_clause} WHERE {where_clause};"
        set_args = self._set_args.copy()
        set_args.extend(args)
        return query, tuple(set_args)
    
    
    def _reset(self):
        self._set_clauses = []
        self._set_args = []