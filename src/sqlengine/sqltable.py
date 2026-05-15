import logging
import os
import sqlite3
from contextlib import contextmanager
from typing import Sequence, Literal, Generator, overload

from .utils import sqlgen as sql
from .utils.statements import Statement, Select, Update, Delete
from .utils.types import (SqlRow, SqlValue, SqlType, Schema, 
                        register_type, is_custom_type, pytype_to_sqltype)

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
    __types_sql__ : list[str]

    def __init__(self, database: str | Literal[":memory:"], force_drop : bool = False, **connection_params) -> None:
        
        self.database = database
        self.connection_params = connection_params
        self._is_managed_transaction = False
        self._statements : list[Statement] = []

        self._validate_attributes()
        self._register_types()
        self._write_db(force_drop)
        
        self._select = Select(self.tablename)

    
    def _validate_attributes(self):

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
        

    def _register_types(self):
        
        resolved : list[str] = []
        assert_register_types = False

        for type_ in self.__types__:
            
            if isinstance(type_, str):
                resolved.append(type_)
                continue
            
            if is_custom_type(type_):
                ctname = type_.__name__.upper()
                
                register_type(type_, ctname)
                resolved.append(ctname)
                
                if not assert_register_types:
                    assert_register_types = True
                
                logger.debug(f'Registered type `{ctname}` in sqlite3')
                continue 
            
            sql_type = pytype_to_sqltype(type_)
            resolved.append(sql_type)
        
        if assert_register_types and ("detect_types" not in self.connection_params):
            self.connection_params.update(dict(detect_types=sqlite3.PARSE_DECLTYPES))

        self.__types_sql__ = resolved
        

    def _write_db(self, force_drop : bool):

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


    def connect(self) -> sqlite3.Connection:
        """ Shortcut to sqlite3 connection context manager """
        return sqlite3.connect(self.database, **self.connection_params)
    

    def open_connection(self):
        """ Opens unmanaged transaction """
        if self.in_transaction():
            raise RuntimeError("Can't re-open existing connection")
        
        self._trans = self.connect()
        self._trans_cursor = self._trans.cursor()

    
    def close_connection(self):
        """ Closes unmanaged transaction """
        if not self.in_transaction():
            return
        
        if self._is_managed_transaction:
            raise RuntimeError("Can't manually close managed transaction")
        
        self._trans_cursor.close()
        self._trans.close()
        del(self._trans_cursor)
        del(self._trans)


    def commit(self):
        if not self.in_transaction():
            raise RuntimeError("Can't commit outside transaction mode")
        
        self._trans.commit()


    def rollback(self):
        if not self.in_transaction():
            raise RuntimeError("Can't rollback outside transaction mode")
        
        self._trans.rollback()
    

    @contextmanager
    def transaction(self, autocommit : bool = True):
        """ 
        Creates context manager to use class methods in transaction

        Args:
            autocommit (bool): If `True`, will commit changes at the end of transaction
        
        Examples:

            >>> from sqlengine import sqlgen as sql
            >>> with table.transaction():
            >>>     for idx, name, age in table:
            >>>         where = sql.where_equals("ID", idx)
            >>>         table.update(where, {"Age" : age + 1})
            >>>     print(table.select())
        """
        
        self.open_connection()
        self._is_managed_transaction = True
        logger.debug(f"{self.tablename}: Transaction started")
        
        try:
            yield

        except Exception as e:
            logger.error(f"{self.tablename}: Error while in transaction: {e}")
            logger.debug(e, exc_info=True)
            self._trans.rollback()
            raise e
        
        else:
            if autocommit:
                self._trans.commit()

        finally:
            self._is_managed_transaction = False
            self.close_connection()
            logger.debug(f"{self.tablename}: Transaction finished")

    
    def in_transaction(self) -> bool:
        """ Returns True if instance is in transaction """
        return hasattr(self, "_trans") and hasattr(self, "_trans_cursor")

    
    @overload
    def _fetch(self, query : str, *args, method : Literal["fetchone"]) -> SqlRow: ...
    @overload
    def _fetch(self, query : str, *args, method : Literal["fetchall"]) -> list[SqlRow]: ...

    def _fetch(self, query : str, *args, method : Literal["fetchone", "fetchall"]) -> SqlRow | list[SqlRow]:
        
        logger.debug(f"{self.tablename}: {query=} {args=}")

        if self.in_transaction():
            self._trans_cursor.execute(query, *args)
            return getattr(self._trans_cursor, method)()

        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query, *args)
            return getattr(cursor, method)()
        
    
    def _execute(self, query : str, *args, method : Literal["execute", "executemany"] = "execute") -> None:
        """
        Shortcut to connect() -> execute[<many>]() -> commit() for single operations. 
        Can be used in transaction using `transaction()` manager.

        Args:
            query (str): SQL query to execute on SQLite3 DB
            *args (Any): Arguments to the execution
            method (str): "execute" or "executemany"
        """
        logger.debug(f"{self.tablename}: {query} {args[0] if args else ''}")

        if self.in_transaction():
            getattr(self._trans_cursor, method)(query, *args)
            return
        
        with self.connect() as conn:
            cursor = conn.cursor()
            getattr(cursor, method)(query, *args)
            conn.commit()


    def execute(self, query : str, *args) -> None:
        """
        Shortcut to connect() -> execute() -> commit() for single operations. 
        Can be used in transaction using `transaction()` manager.

        Args:
            query (str): SQL query to execute on SQLite3 DB
            *args (tuple[SqlValue]): Arguments to the execution
        """
        return self._execute(query, *args, method="execute")
    
    
    def executemany(self, query : str, *args) -> None:
        """
        Shortcut to connect() -> executemany() -> commit() for single operations. 
        Can be used in transaction using `transaction()` manager.

        Args:
            query (str): SQL query to execute on SQLite3 DB
            *args (list[tuple[SqlValue]]): Arguments to the execution
        """
        return self._execute(query, *args, method="executemany")
    

    def execute_statements(self):

        logger.debug(f"Statements length: {len(self._statements)}")
        
        while self._statements:
            statement   = self._statements.pop(0)
            query, args = statement.build()
            self._execute(query, args, method='execute')
    
    
    def create_table(self) -> None:
        """ Create table if not exists """
       
        query = sql.create_table(
            self.tablename, self.columns, 
            self.types_sql, self.primary
        )

        self.execute(query)


    def drop_table(self, confirm : bool = False) -> None:
        """ Drops table if it exists. """
        
        if not confirm:
            raise ValueError("To drop table you have to pass `confirm=True`")
        
        self.execute(sql.drop_table(self.tablename))


    def fetchone(self, query : str, *args) -> SqlRow:
        """
        Fetch first row based on `query`

        Args:
            query (str): SQL query

        Returns:
            out (SqlRow): Single row
        """
        return self._fetch(query, *args, method="fetchone")
    

    def fetchmany(self, query : str, *args, size : int = 1) -> list[SqlRow]:
        """
        Fetch first `size` rows based on `query`

        Args:
            query (str): SQL query
            size (str): Number of rows to return

        Returns:
            out (list[SqlRow]): list of `size` rows
        """
        logger.debug(f"{self.tablename}: {query} {args}")

        if self.in_transaction():
            self._trans_cursor.execute(query, *args)
            return self._trans_cursor.fetchmany(size)

        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query, *args)
            return cursor.fetchmany(size)
    

    def fetchall(self, query : str, *args) -> list[SqlRow]:
        """
        Fetch all rows based on `query`

        Args:
            query (str): SQL query

        Returns:
            out (list[SqlRow]): list of rows
        """
        return self._fetch(query, *args, method="fetchall")
    
    @overload
    def fetch_select(self, fetch : Literal['one']) -> SqlRow: ...
    @overload
    def fetch_select(self, fetch : Literal['many'], size : int) -> list[SqlRow]: ...
    @overload
    def fetch_select(self, fetch : Literal['all']) -> list[SqlRow]: ...
    
    def fetch_select(self, fetch : Literal['one', 'many', 'all'] = 'all', size : int = 1) -> SqlRow | list[SqlRow]:
        query, args = self._select.build()
        self.select.reset()

        if fetch == 'all':
            return self.fetchall(query, args)
        elif fetch == 'many':
            return self.fetchmany(query, args, size=size)
        return self.fetchone(query, args)

    
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
        self.execute(query, args)

    
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
        self.execute(query, args)

    
    def update(self) -> Update:
        """
        Update columns based on `where_clause` 
    
        Args:
            where_clause (str): describes search filter with SQL condition query
            set_values (dict[str, SqlValue]): dict where keys are column names and
                values are corresponding new values to set

        Example:
            >>> # This will set "unemployed" as `Occupation` and 0.0 as `Salary`
            >>> # for all the salespeople and CEO
            >>> from sqlengine import sqlgen as sql
            >>> where = sql.where("Occupation", "IN", ["seller", "CEO"])
            >>> table.update(where, {"Occupation" : "unemployed", "Salary" : 0.0})
        """
        update = Update(self.tablename)
        self._statements.append(update)
        return update


    def insert_many(self, rows: Sequence[SqlRow]) -> None:
        """
        Bulk insert multiple rows
        
        Args:
            rows (list[SqlRow]): List of tuples, each tuple contains 
                values for one row in the order of __columns__
        """
        query = sql.insert_row(self.tablename, self.columns)
        return self.executemany(query, rows)
    
    
    def delete(self) -> Delete:
        delete = Delete(self.tablename)
        self._statements.append(delete)
        return delete
    

    @property
    def select(self):
        if self._select.is_active():
            raise RuntimeError("Can't run more then one active select statements at a time. First, call one of the fetch methods.")
        return self._select


    def head(self, n : int = 5) -> list[SqlRow]:
        """ Returns first `n` rows unordered """
        return self.fetch_select('many', size=n)
    

    def row_id(self, column : str) -> int:
        """ Returns id of given column name. Usefull to get value index of the row. """
        if column not in self.columns:
            raise ValueError(f"No column `{column}` in columns. Available: {[self.columns]}")
        return self.columns.index(column)
    

    def fetchmany_iterator(self, batch_size: int) -> Generator[list[SqlRow], None, None]:
        """
        Yields all rows in batches, each batch in its own transaction.
        
        Args:
            batch_size (int): Size of each batch

        Examples:

            >>> from sqlengine import sqlgen as sql
            >>> where = sql.where("Age" ">" 30)
            >>> query = sql.select(table.tablename, where)
            >>> with table.transaction():
            >>>     for batch in table.fetchmany_iterator(1000):
            >>>         process_batch(batch)
        """
        if not self.in_transaction():
            raise RuntimeError("To use the `fetchall_iterator()` method you have \
                    to keep open the transaction with `transaction()` manager")
        
        query, exec_args = self._select.build()
        self._select.reset()

        iter_cursor = self._trans.cursor()
        iter_cursor.execute(query, exec_args)

        while batch := iter_cursor.fetchmany(batch_size):
            yield batch
    

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"database={self.database}, "
            f"tablename={self.tablename}, "
            f"columns={sql.format_list(self.columns)}, "
            f"types={sql.format_list(self.types)}, "
            f"primary={sql.format_list(self.primary)})"
        )
    

    def __len__(self) -> int:
        self.select().aggregate("COUNT")
        length = self.fetch_select('one')[0]
        if not isinstance(length, int):
            raise ValueError("Whata duck")
        return length
    

    def __iter__(self) -> Generator[SqlRow, None, None]:
        """ Database rows iterator """
        
        if not self.in_transaction():
            raise RuntimeError("To use the __iter__ method you have \
                to keep open the transaction with `transaction()` manager")
        
        iter_cursor = self._trans.cursor()
        iter_cursor.execute(sql.select(self.tablename))

        while row := iter_cursor.fetchone(): 
            yield row

    
    @overload
    def __getitem__(self, key : tuple[SqlValue, ...] | SqlValue) -> SqlRow: ...
    @overload
    def __getitem__(self, key : slice) -> list[SqlRow]: ...
    
    def __getitem__(self, key : tuple[SqlValue, ...] | SqlValue | slice ) -> SqlRow | list[SqlRow]:
        """ Get row by primary key """

        if len(self.primary) > 1:
            if not isinstance(key, tuple):
                raise IndexError("`key` expected to be a tuple for multi index tables")
            if not len(key) == len(self.primary):
                raise IndexError("`key` expected to be a tuple of equal leght to `primary` for multi index tables")

        if isinstance(key, slice):
            
            logger.debug(f"Got slice: {key}")

            primary = self.primary[0]

            if key.start is None:
                self.select()(primary).aggregate('MIN')
                start = self.fetch_select('one')[0] or 0
            else:
                start = key.start

            if key.stop is None:
                self.select()(primary).aggregate('MAX')
                stop = self.fetch_select('one')[0] or 0
            else:
                stop = key.stop

            if key.step is None:
                step = 1
            else:
                step = key.step

            logger.debug(f"Transformed: {start}, {stop}, {step}")

            if not (isinstance(start, int) and isinstance(stop, int)):
                raise ValueError("Looks like like `primary` key is not integer type, or you passed non-integer slice")

            if abs(step) == 1:
                _start = min(start, stop)
                _stop  = max(start, stop)
                self.select.order_by(primary, step > 0).where.between(primary, _start, _stop)
                return self.fetch_select('all')
            
            ids = [i for i in range(start, stop, step)]
            self.select.order_by(primary, step > 0).where.inside(primary, ids)

            return self.fetch_select('one')
        
        
        if isinstance(key, tuple):
            select = self.select
            for col, val in zip(self.primary, key):
                select.where.eq(col, val)
            return self.fetch_select('one')


        self.select.where.eq(self.primary[0], key)
        return self.fetch_select('one')


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
    def tx_conn(self) -> sqlite3.Connection:
        """ Gives access to connection while in transaction """
        if not self.in_transaction():
            raise RuntimeError("`tx_conn` is not available outside the transaction mode")
        return self._trans
    
    
    @property
    def tx_cursor(self) -> sqlite3.Cursor:
        """ Gives access to connection cursor while in transaction """
        if not self.in_transaction():
            raise RuntimeError("`tx_cursor` is not available outside the transaction mode")
        return self._trans_cursor
    
    
    @property
    def schema(self) -> Schema:
        """ Table schema """

        return Schema({
            "tablename": self.__tablename__,
            "columns"  : self.__columns__,
            "types"    : self.__types__,
            "primary"  : self.__primary__
        })
