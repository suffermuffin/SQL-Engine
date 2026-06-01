import logging
import os
import sqlite3

from typing import Sequence, Literal, Any
from typing import get_origin, get_args, overload, get_type_hints

from .exceptions import TableDeclarationError

from ._internal import sqlgen as sql
from ._internal.repr import to_html

from ._internal import ConnectionManager, Select, Update, Delete
from ._internal.types import SqlRow, SqlValue, ColumnType
from ._internal.types import Schema, Primary
from ._internal.types import register_resolve_types

logger = logging.getLogger("sqlengine")
logger.setLevel(os.getenv("SQL_ENGINE_LOG_LEVEL", "WARNING").upper())


class SqlTableMixin:
    """
    Lightweight wrapper for SQLite3 tables
    
    Args:
        database (str): database filename to connect to. If it does not exists - will create new one first.
            If `":memory:"` is passed, then database will be set in memory and you will have to
            create table manually with `create_table()` method inside `transaction()` block.
        force_drop (bool): If `True` - will drop existing table.
        **connection_params: Params to create connection with. Reference: https://docs.python.org/3/library/sqlite3.html#sqlite3.connect

    Attributes:
        __tablename__ (Optional[str]): Name of the table that will be used in queries. 
            If omitted in inherited class declaration, then it will take the class name.
        __columns__ (list[str]): Column names of the table
        __types__ (list[ColumnType]): Column types of the table
        __primary__ (list[str]): List of primary keys

    Example:
    ```python
    from sqlengine import SqlTableMixin, Primary
    
    class Employees(SqlTableMixin):
        ID       : Primary[int]
        name     : Primary[str]
        surname  : str   | None
        salary   : float | None
        position : str
        
    table = Employees("mydb.sqlite3")
    ```
    """

    __tablename__ : str
    __columns__   : list[str]
    __types__     : list[ColumnType]
    __primary__   : list[str]

    def __init__(self, database: str | Literal[":memory:"], force_drop : bool = False, **connection_params) -> None:
        
        self.database = database

        resolved_types, connection_params = register_resolve_types(self.__types__, **connection_params)

        self._connection_manager = ConnectionManager(database, **connection_params)
        self.__types_sql__       = resolved_types

        self._write_db(force_drop)

    
    def __init_subclass__(cls) -> None:

        annotations = get_type_hints(cls)
        
        tablename = cls.__tablename__ if \
            hasattr(cls, "__tablename__") and cls.__tablename__ is not None\
            else cls.__name__

        columns = cls.__columns__ if hasattr(cls, "__columns__") else []
        types   = cls.__types__   if hasattr(cls, "__types__")   else []
        primary = cls.__primary__ if hasattr(cls, "__primary__") else []

        for name, type_ in annotations.items():
            if name.startswith("_") or name.endswith("_"):
                continue
            
            if name in columns:
                raise TableDeclarationError(f"Annotated column `{name}` is already in __columns__")
            
            columns.append(name)
            
            if not get_origin(type_) == Primary:
                types.append(type_)
                continue

            if name in primary:
                raise TableDeclarationError(f"Annotated primary column `{name}` is already in __primary__")
            
            primary_type = get_args(type_)[0]

            if not primary_type:
                raise TableDeclarationError(("Primary type was declared without the type. "
                "Usage: `my_column : Primary[T]`, where T is desired type"))
            
            types.append(primary_type)
            primary.append(name)
            
        
        cls.__tablename__ = tablename

        cls.__columns__ = columns
        cls.__types__   = types
        cls.__primary__ = primary
        cls._validate_attributes()


    @classmethod
    def _validate_attributes(cls) -> None:

        missing_attrs = [
            attr for attr in 
            [ "__columns__", "__types__", "__primary__", "__tablename__"]
            if not hasattr(cls, attr)
        ]

        if missing_attrs:
            raise TableDeclarationError(f'{cls.__name__} is missing attributes: {missing_attrs}')
        
        n_types, n_cols = len(cls.__types__), len(cls.__columns__)

        if not n_types == n_cols:
            raise TableDeclarationError((f'`__types__` and `__columns__`: length mismatch: types = {n_types}, columns = {n_cols}'))
        
        if len(cls.__primary__) < 1:
            raise TableDeclarationError(f'`__primary__`: Number of primary keys must be at least 1')
        
        wrong_primaries = [
            prim for prim in cls.__primary__ if
            prim not in cls.__columns__
        ]

        if wrong_primaries:
            raise TableDeclarationError(f'`__primary__`: Keys {wrong_primaries} can\'t be primaries as they are not declared in __columns__')
        

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
        
        Example:
            
        ```python
        with table.transaction():
            for idx, age in table.select("ID", "Age"):
                table.update("Age", age + 1).where.eq("ID", idx).then.execute()
        ```
        """
        
        return self._connection_manager.transaction(autocommit)

    
    def insert(self, *args, **kwargs) -> None:
        """ 
        Insert single row

        Args:
            *args (SqlValue): Arguments in order of declared __columns__
            **kwargs (SqlValue): Column to value mapping

        Example:

        ```python
        table = MyTable("mydb.db")
        table.columns # -> ["ID", "Name", "Age"]
        table.insert(0, "Daniel", 27)
        table.insert(ID=1, name="Boris", age=26)
        ```
        """
        columns = self.columns[:len(args)]
        columns.extend(kwargs.keys())
        query = sql.insert_row(self.tablename, columns)
        self._connection_manager.execute(query, *args, *kwargs.values())

    
    def upsert(self, *args, **kwargs) -> None:
        """ 
        Upsert (update or insert) single row, resolving conflicts
        via the declared `primary` key
        
        Args:
            **args (SqlValue): Arguments in order of declared __columns__
            **kwargs (SqlValue): Column to value mapping

        Example:

        ```python
        table = MyTable("mydb.db")
        table.columns # -> ["ID", "Name", "Age"]
        table.upsert(0, "Daniel", 27)
        table.upsert(ID=0, age=21)
        ```
        """
        columns = self.columns[:len(args)]
        columns.extend(kwargs.keys())
        query = sql.upsert(self.tablename, columns, self.primary)
        self._connection_manager.execute(query, *args, *kwargs.values())

    
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

        
        def resolve_slice(key : slice) -> tuple[int, int, int]:
            
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
            
            return start, stop, step
            
        
        if isinstance(key, slice):
            
            primary = self.primary[0]
            start, stop, step = resolve_slice(key)

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
        """ 
        UPDATE statement builder and executor 
        
        Example:
        
        ```python
        table.update.set("City", "Karaganda").where.eq("Country", "Czech Republic").then.execute()
        ```
        """
        return Update(self.conn, self.schema)

    
    @property
    def delete(self) -> Delete:
        """ 
        DELETE statement builder and executor 
        
        Example:
        
        ```python
        table.delete.where.eq("ID", 0).then.execute()
        ```
        """
        return Delete(self.conn, self.schema)
    

    @property
    def select(self) -> Select:
        """ 
        SELECT statement builder and fetcher

        Example:
        
        ```python
        table.select("Email").where.eq("SupportRepId", 3).then.aggregate("COUNT").fetchone()
        ```
        """
        return Select(self.conn, self.schema)


    @property
    def columns(self) -> list[str]:
        """ List of table column names """
        return self.__columns__

    
    @property
    def types(self) -> list[ColumnType]:
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
        """ Access connection manager instance """
        return self._connection_manager
    

    @property
    def tx_conn(self) -> sqlite3.Connection:
        """ Access connection while in transaction """
        return self._connection_manager.tx_conn


    @property
    def tx_cursor(self) -> sqlite3.Cursor:
        """ Access cursor while in transaction """
        return self._connection_manager.tx_cursor
    

    @property
    def connection_params(self) -> dict[str, Any]:
        """ Connection parameters used in connection creation """
        return self._connection_manager.connection_params

    
    @connection_params.setter
    def connection_params(self, value : dict[str, Any]) -> None:
        self._connection_manager.connection_params = value
