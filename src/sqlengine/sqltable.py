import logging
import os
import sqlite3

from typing import Sequence, Literal, Any, overload

from .core import sqlgen as sql
from .core.repr import to_html

from .core import ConnectionManager, Select, Update, Delete
from .core.types import SqlRow, SqlValue, SqlType, Schema
from .core.types import register_resolve_types

logger = logging.getLogger("sqlengine")
logger.setLevel(os.getenv("SQL_ENGINE_LOG_LEVEL", "WARNING").upper())


class SqlTableMixin:
    """
    Lightweight wrapper for SQLite3 tables
    
    Args:
        database (str): database filename to connect to. If it not exists - will create new one first.
            If `":memory:"` is passed, then database will be created in memory and you will have to
            create table manually with `create_table()` method inside `transaction()` block.
        force_drop (bool): If `True` - will drop existing table.
        **connection_params (dict): Params to create connection with. 
            Reference: https://docs.python.org/3/library/sqlite3.html#sqlite3.connect

    Attributes:
        __tablename__ (Optional[str]): Name of the table that will be used in queries. 
            If omitted in inherited class declaration, then it will take the class name.
        __columns__ (list[str]): Colum names of the table
        __types__ (list[SqlType | str]): Colum types of the table
        __primary__ (list[str]): List of primary keys

    Examples:
        >>> class Employees(SqlTableMixin):
        >>>     __columns__   = ["ID", "name", "surname", "salary", "position"]
        >>>     __types__     = [int, str, str, float, "TEXT NOT NULL"]
        >>>     __primary__   = ["ID", "name"]
        >>> 
        >>> table = Employees(":memory:")
    """

    __tablename__ : str
    __columns__   : list[str]
    __types__     : list[SqlType | str]
    __primary__   : list[str]

    def __init__(self, database: str | Literal[":memory:"], force_drop : bool = False, **connection_params) -> None:
        
        self._validate_attributes()
        resolved_types, connection_params = register_resolve_types(self.__types__, **connection_params)

        self.database            = database
        self._connection_manager = ConnectionManager(database, **connection_params)
        self.__types_sql__       = resolved_types

        self._write_db(force_drop)


    def _validate_attributes(self) -> None:

        if not hasattr(self, "__tablename__") or self.__tablename__ is None:
            self.__tablename__ = self.__class__.__name__

        missing_attrs = [
            attr for attr in 
            [ "__columns__", "__types__", "__primary__"] 
            if not hasattr(self, attr)
        ]

        if missing_attrs:
            raise AttributeError(f'{self.tablename} is missing attributes: {missing_attrs}')
        
        n_types, n_cols = len(self.__types__), len(self.__columns__)

        if not n_types == n_cols:
            raise AttributeError(f'`__types__` and `__columns__`: length mismatch: types = {n_types}, columns = {n_cols}')
        
        wrong_primaries = [
            prim for prim in self.__primary__ if
            prim not in self.__columns__
        ]

        if wrong_primaries:
            raise AttributeError(f'`__primary__`: Keys {wrong_primaries} can\'t be primaries as they are not declared in __columns__')
        

    def _write_db(self, force_drop : bool) -> None:

        if self.database == ":memory:":
            logger.debug(f"{self.tablename}: Using in-memory database")
            return
        
        if not os.path.exists(self.database):
            logger.debug(f'{self.tablename}: {self.database} does not exist. Creating...')
            parent_dir = self.database.removesuffix(os.path.basename(self.database))
            if parent_dir: 
                os.makedirs(parent_dir, exist_ok=True)

        elif force_drop is True:
            self.drop_table(confirm=True)

        self.create_table()

    
    def create_table(self) -> None:
        """ Create table if not exists """
       
        query = sql.create_table(
            self.tablename, self.columns, 
            self.types_sql, self.primary
        )

        self._connection_manager.execute(query)


    def drop_table(self, confirm : bool = False) -> None:
        """ Drops table if it exists. """
        
        if not confirm:
            raise ValueError("To drop table you have to pass `confirm=True`")
        
        self._connection_manager.execute(sql.drop_table(self.tablename))
    

    def transaction(self, autocommit : bool = True):
        """ 
        Creates context manager to use class methods in transaction

        Args:
            autocommit (bool): If `True`, will commit changes at the end of transaction
        
        Examples:

            >>> with table.transaction():
            >>>     for idx, age in table.select("ID", "Age"):
            >>>         table.update("Age", age + 1).where.eq("ID", idx).then.execute()
            >>>     print(table.select)
        """
        
        return self._connection_manager.transaction(autocommit)

    
    def insert(self, *args, **kwargs) -> None:
        """ 
        Insert single row

        Args:
            *args (Any): Arguments in order of declared __columns__
            **kwargs (Any): Unused

        Example:
            >>> table = MyTable("mydb.db")
            >>> table.columns 
            >>> # ["ID", "Name", "Age"]
            >>> table.insert(0, "Daniel", 27)
        """
        query = sql.insert_row(self.tablename, self.columns)
        self._connection_manager.execute(query, *args)

    
    def upsert(self, *args, **kwargs) -> None:
        """ 
        Upsert (update or insert) single row, resolving conflicts
        via the declared `primary` key
        
        Args:
            *args (Any): Arguments in order of declared __columns__
            **kwargs (Any): Unused

        Example:
            >>> table = MyTable("mydb.db")
            >>> table.columns 
            >>> # ["ID", "Name", "Age"]
            >>> table.upsert(0, "Daniel", 27)
            >>> table.upsert(0, "Daniel", 21)
        """
        query = sql.upsert(self.tablename, self.columns, self.primary)
        self._connection_manager.execute(query, *args)

    
    def insert_many(self, rows: Sequence[SqlRow]) -> None:
        """
        Bulk insert multiple rows
        
        Args:
            rows (list[SqlRow]): List of tuples, each tuple contains 
                values for one row in the order of __columns__
        """
        query = sql.insert_row(self.tablename, self.columns)
        return self._connection_manager.executemany(query, rows)


    def head(self, n : int = 5) -> list[SqlRow]:
        """ Returns first `n` rows unordered """
        return self.select.limit(n).fetchall()
    

    def in_transaction(self) -> bool:
        """ Returns True if instance is in transaction """
        return self._connection_manager.in_transaction()
    
    
    def open_connection(self) -> None:
        """ Opens unmanaged transaction """
        return self._connection_manager.open()


    def close_connection(self) -> None:
        """ Closes unmanaged transaction """
        return self._connection_manager.close()


    def commit(self) -> None:
        """ Commit current transaction """
        return self._connection_manager.commit()


    def rollback(self) -> None:
        """ Rollback current transaction """
        return self._connection_manager.rollback()


    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"database={self.database}, "
            f"tablename={self.tablename}, "
            f"columns={sql.format_list(self.columns)}, "
            f"types={sql.format_list(self.types)}, "
            f"primary={sql.format_list(self.primary)})"
        )
    

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(database={self.database}, tablename={self.tablename})"
    

    def _repr_html_(self) -> str | None:

        if self.database == ":memory:":
            return None
        
        return to_html(self.tablename, self.columns, self.head(11), 10)
    

    def __len__(self) -> int:
        
        length = self.select.aggregate("COUNT").fetchone()[0]
        
        if not isinstance(length, int):
            raise ValueError("Unreachable")
        return length

    
    @overload
    def __getitem__(self, key : tuple[SqlValue, ...] | SqlValue) -> SqlRow: ...
    @overload
    def __getitem__(self, key : slice) -> list[SqlRow]: ...
    
    def __getitem__(self, key : tuple[SqlValue, ...] | SqlValue | slice ) -> SqlRow | list[SqlRow]:
        """ Get row by primary key """

        if len(self.primary) > 1 and not (isinstance(key, tuple) and len(key) == len(self.primary)):
            raise IndexError("`key` expected to be a tuple of equal leght to `primary` for multi index tables")
            
        select = self.select

        if isinstance(key, slice):
            
            primary = self.primary[0]

            if key.start is None:
                start = select(primary).aggregate('MIN').fetchone()[0] or 0
                select.reset()
            else:
                start = key.start

            if key.stop is None:
                stop = select(primary).aggregate('MAX').fetchone()[0] or 0
                select.reset()
            else:
                stop = key.stop

            if key.step is None:
                step = 1
            else:
                step = key.step

            if not (isinstance(start, int) and isinstance(stop, int)):
                raise IndexError("Looks like like `primary` key is not integer type, or you passed non-integer slice")

            if abs(step) == 1:
                _start = min(start, stop)
                _stop  = max(start, stop)
                
                select.order_by(primary, step > 0).where.between(primary, _start, _stop)
                return select.fetchall()
            
            ids = [i for i in range(start, stop, step)]
  
            select.order_by(primary, step > 0).where.in_(primary, ids)
            return select.fetchall()
        
        
        if isinstance(key, tuple):

            for col, val in zip(self.primary, key):
                select.where.eq(col, val)
            return select.fetchone()

        select.where.eq(self.primary[0], key)
        return select.fetchone()
    

    @property
    def update(self) -> Update:
        """ UPDATE statement builder and executor """
        return Update(self.conn, self.schema)

    
    @property
    def delete(self) -> Delete:
        """ DELETE statement builder and executor """
        return Delete(self.conn, self.schema)
    

    @property
    def select(self) -> Select:
        """ SELECT statement builder and fetcher """
        return Select(self.conn, self.schema)


    @property
    def columns(self) -> list[str]:
        """ List of table column names """
        return self.__columns__

    
    @property
    def types(self) -> list[SqlType | str]:
        """ List of table column dtypes as declared"""
        return self.__types__
    
    
    @property
    def types_sql(self) -> list[str]:
        """ List of table column dtypes converted to SQL native and registered types """
        return self.__types_sql__

    
    @property
    def primary(self) -> list[str]:
        """ List of table column primary keys """
        return self.__primary__

    
    @property
    def tablename(self) -> str:
        """ Name of the table """
        return self.__tablename__
    

    @property
    def shape(self) -> tuple[int, int]:
        """ Table shape (n_cols, n_rows) """
        return (len(self.columns), len(self))
    
    
    @property
    def schema(self) -> Schema:
        """ Table schema """

        return Schema({
            "tablename": self.__tablename__,
            "columns"  : self.__columns__,
            "types"    : self.__types__,
            "primary"  : self.__primary__
        })
    

    @property
    def conn(self) -> ConnectionManager:
        return self._connection_manager
    

    @property
    def tx_conn(self) -> sqlite3.Connection:
        """ Access connection while in transaction """
        return self._connection_manager.tx_conn


    @property
    def tx_cursor(self) -> sqlite3.Cursor:
        """ Access cursor while in transaction """
        return self._connection_manager.tx_cursor
    

    @property # TODO: add setter
    def connection_params(self) -> dict[str, Any]:
        return self._connection_manager.connection_params
