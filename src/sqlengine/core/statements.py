from typing import Sequence, Literal, Generator, Self
from abc    import ABC, abstractmethod

from . import sqlgen as sql
from .connection import ConnectionManager

from .types import SqlValue, SqlRow, Schema
from .repr  import to_html


class Where[T : "Statement"]:
    """ Where clause build helper """
    
    def __init__(self, statement : T):
        
        self._statement = statement
        
        self._clause   : list[str] = []
        self._args     : list[SqlValue] = []


    @property
    def then(self) -> T:
        """ Returns upper statement object """
        return self._statement
    
    
    def __call__(self, where_clasuse : str, *args : SqlValue) -> Self:
        """ Shortcut to custom where clause """
        return self.custom(where_clasuse, *args)

    
    def op(self, column : str, value : SqlValue, operator : str) -> Self:
        self._clause.append(f"{column} {operator} ?")
        self._args.append(value)
        return self

    
    def join(self, lop : str = "AND") -> Self:
        """ Joins previous expression via logical operator `lop` """
        joined = f" {lop} ".join(self._clause)
        self._clause = [f"({joined})"]
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
    
    
    def like(self, column : str, pattern : str) -> Self:
        """ 
        Like operator. Pattern is a SQL wildcard pattern 
        (i.e. `%` for any string, `_` for one character).
        """
        return self.op(column, pattern, "LIKE")
    
    
    def is_null(self, column : str) -> Self:
        self._clause.append(f"{column} IS NULL")
        return self
    
    
    def inverted(self) -> Self:
        """ Invert last where clause with NOT """
        self._clause[-1] = f"NOT ({self._clause[-1]})"
        return self
    
    
    def in_(self, column : str, values : Sequence[SqlValue]) -> Self:
        if isinstance(values, str):
            raise ValueError("Got string as sequence of values in in_, expected tuple/list/etc...")
        placeholder = sql.values_placeholder(len(values))
        self._clause.append(f"{column} IN {placeholder}")
        self._args.extend(values)
        return self
    
    
    def between(self, column : str, start : SqlValue, stop : SqlValue) -> Self:
        self._clause.append(f"{column} BETWEEN ? AND ?")
        self._args.extend((start, stop))
        return self
    
    
    def custom(self, where_clause : str, *args : SqlValue) -> Self:
        """ Add custom where clause (e.g. `where.custom("Age > ? AND Age != ?", 10, 25)`) """
        self._clause.append(where_clause)
        self._args.extend(args)
        return self

    
    def build(self, lop : str = "AND") -> tuple[str, tuple[SqlValue, ...]]:
        where_clause = f" {lop} ".join(self._clause).strip()
        args = tuple(self._args)
        return where_clause, args
    
    
    def reset(self) -> None:
        self._args = []
        self._clause = []

    
    def __str__(self) -> str:
        return self._statement.__str__()

    
    def __repr__(self) -> str:
        return self._statement.__repr__()

    
    def _repr_html_(self) -> str | None:
        if isinstance(self._statement, Select):
            return self._statement._repr_html_()
        return None
    
    
    def __len__(self) -> int:
        return len(self._args)


class Statement(ABC):
    """
    Statement object that helps you build queries and execute them
    """

    def __init__(self, connection : ConnectionManager, tableschema : Schema) -> None:

        self._tableschema = tableschema
        self._connection  = connection
        self._where: Where[Self] = Where(self)

        self._custom_query : str | None = None
        self._custom_args  : tuple[SqlValue, ...] = ()
    
    
    def custom_query(self, query : str, *args) -> Self:
        """ Custom query that completely replaces builder's expression """
        self._custom_query = query
        self._custom_args  = args
        return self
    

    def build(self) -> tuple[str, tuple[SqlValue, ...]]:
        """ Build complete expression with sorted arguments and operations """
        if self._custom_query:
            return self._custom_query, self._custom_args
        
        where_clause, args = self._where.build()
        query, args = self._build(where_clause, *args)
        return query, args
    

    def reset(self) -> None:
        """ Reset statement to reuse the object """
        self._where.reset()
        self._custom_query = None
        self._custom_args = ()
        self._reset()
    
    
    @property
    def where(self) -> Where[Self]:
        """ Where clause builder """
        return self._where
    
    
    @abstractmethod
    def _build(self, where_clause : str, *args : SqlValue) -> tuple[str, tuple[SqlValue, ...]]:
        pass

    
    @abstractmethod
    def _reset(self) -> None:
        pass


    def __repr__(self) -> str:
        query, args = self.build()
        return f"{query} {args}"
    

    def __str__(self) -> str:
        query, _ = self.build()
        return query
    

class MutationalStatement(Statement, ABC):
    
    def execute(self) -> None:
        query, args = self.build()
        self._connection.execute(query, *args)


class Select(Statement):

    def __init__(self, connection : ConnectionManager, tableschema : Schema) -> None:
        super().__init__(connection, tableschema)
        
        self._columns   : list[str] = []
        self._order_by  : list[str] = []
        self._aggregate : str | None = None
        self._limit     : int | None = None


    def __call__(self, *columns : str) -> Self:
        return self.columns(*columns)
    

    def columns(self, *columns : str) -> Self:
        """ Column selector """
        self._columns.extend(columns)
        return self
    
    
    def aggregate(self, by : Literal['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']) -> Self:
        
        if self._aggregate:
            raise ValueError("Can't aggregate columns multiple times")
        
        self._aggregate = by
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
        return self._connection.fetchone(query, *args)
        

    def fetchmany(self, size : int = 1) -> list[SqlRow]:
        query, args = self.build()
        return self._connection.fetchmany(query, *args, size=size)

    
    def fetchall(self) -> list[SqlRow]:
        query, args = self.build()
        return self._connection.fetchall(query, *args)
    

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
        if not self._connection.in_transaction():
            raise RuntimeError("To use the `fetchall_iterator()` method you have \
                    to keep open the transaction of the table with `transaction()` manager")
        
        query, exec_args = self.build()

        iter_cursor = self._connection.tx_conn.cursor()
        iter_cursor.execute(query, exec_args)

        while batch := iter_cursor.fetchmany(batch_size):
            yield batch

    
    def __iter__(self) -> Generator[SqlRow, None, None]:
        """ Select statement rows iterator """
        
        if not self._connection.in_transaction():
            raise RuntimeError("To use the __iter__ method you have \
                to keep open the transaction of the table with `transaction()` manager")
        
        query, exec_args = self.build()
        
        iter_cursor = self._connection.tx_conn.cursor()
        iter_cursor.execute(query, exec_args)

        while row := iter_cursor.fetchone(): 
            yield row
    

    def _build(self, where_clause : str, *args : SqlValue) -> tuple[str, tuple[SqlValue, ...]]:

        order   = sql.format_list(self._order_by, brackets=False)
        columns = sql.format_list(self._columns,  brackets=False)
        
        columns = "*" if not columns else columns
        columns = columns if not self._aggregate else f"{self._aggregate}({columns})"

        if self._limit is not None:
            limit = "?"
            args  = (*args, self._limit)
        
        else:
            limit = None
        
        query = sql.select(self._tableschema["tablename"], columns, where_clause, order, limit)
        
        return query, args
    
    
    def _reset(self) -> None:
        self._columns   = []
        self._order_by  = []
        self._aggregate = None
        self._limit     = None


    def _resolve_columns(self) -> list[str]:
        return (
            self._tableschema["columns"] 
            if len(self._columns) == 0 or "*" in self._columns 
            else self._columns
        )


    def _repr_html_(self) -> str | None:
        
        if self._aggregate:
            return None
        
        limit     = 26
        columns   = self._resolve_columns()
        repr_rows = self.fetchmany(limit)

        return to_html(self._tableschema["tablename"], columns, repr_rows, limit=limit-1)
    

class Delete(MutationalStatement):

    def _build(self, where_clause : str, *args : SqlValue) -> tuple[str, tuple[SqlValue, ...]]:
        
        if not where_clause:
            raise ValueError("Delete statement must have a where clause")
        
        query = sql.delete_rows(self._tableschema["tablename"], where_clause)
        return query, args
    
    
    def _reset(self) -> None:
        pass
    

class Update(MutationalStatement):

    def __init__(self, connection : ConnectionManager, tableschema : Schema) -> None:
        super().__init__(connection, tableschema)
        self._set_clauses : list[str] = []
        self._set_args    : list[SqlValue] = []

    
    def __call__(self, column : str, value : SqlValue) -> Self:
        return self.set(column, value)


    def set(self, column : str, value : SqlValue) -> Self:
        """ Set value to a column """
        self._set_clauses.append(f"{column} = ?")
        self._set_args.append(value)
        return self


    def _build(self, where_clause : str, *args : SqlValue) -> tuple[str, tuple[SqlValue, ...]]:
        set_clause = sql.format_list(self._set_clauses, brackets=False)
        query = f"UPDATE {self._tableschema["tablename"]} SET {set_clause} WHERE {where_clause};"
        return query, (*self._set_args, *args)
    
    
    def _reset(self) -> None:
        self._set_clauses = []
        self._set_args = []