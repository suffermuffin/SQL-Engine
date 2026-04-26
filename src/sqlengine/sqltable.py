import logging
import os
import sqlite3
from contextlib import contextmanager
from typing import Iterable, Sequence, Literal, Generator, overload

from .utils import sqlgen as sql
from .utils.types import SqlRow, SqlValue

logger = logging.getLogger(__name__)


class SqlTableMixin:
    """
    Lightweighted wrapper for SQLite3 tables
    
    Args:
        database (str): database filename to connect to. If it not exists - will create new one first.
            If `":memory:"` is passed, then database will be created in memory and you will have to
            create table manually with `create_table()` method inside `transaction()` block.
        force_drop (bool): If `True` - will drop existing table.
        **connection_params (dict): Params to create connection with. 
            Reference: https://docs.python.org/3/library/sqlite3.html#sqlite3.connect

    """

    __columns__   : list[str]
    __types__     : list[str]
    __primary__   : list[str]
    __tablename__ : str

    def __init__(self, database: str | Literal[":memory:"], force_drop : bool = False, **connection_params) -> None:
        
        self.database = database
        self.connection_params = connection_params

        self._validate_attributes()
        self._validate_write_db(force_drop)

    
    def _validate_write_db(self, force_drop : bool):

        if self.database == ":memory:":
            logger.debug(f"{self.__class__.__name__}: Using in-memory database")
            return

        if not self.database.endswith('.db'):
            raise ValueError("`database` must have '.db' extension")
        
        if not os.path.exists(self.database):
            logger.info(f'{self.__class__.__name__}: {self.database} does not exist. Creating...')
            parent_dir = self.database.removesuffix(os.path.basename(self.database))
            if parent_dir: 
                os.makedirs(parent_dir, exist_ok=True)

        elif force_drop is True:
            self.drop_table()

        self.create_table()

    
    def _validate_attributes(self):

        missing_attrs = [
            attr for attr in 
            [ "__columns__", "__types__", "__primary__", "__tablename__"] 
            if not hasattr(self, attr)
        ]

        if missing_attrs:
            raise AttributeError(f'{self.__class__.__name__} is missing attributes: {missing_attrs}')
        
        n_types, n_cols = len(self.__types__), len(self.__columns__)

        if not n_types == n_cols:
            raise AttributeError(f'__types__ and __columns__ length mismatch: types = {n_types}, columns = {n_cols}')


    @staticmethod
    def _flatten_rows(rows : Sequence[Iterable]) -> list[SqlValue]:
        return [item for row in rows for item in row]
    
    
    def connect(self) -> sqlite3.Connection:
        """ Shortcut to connection context manager """
        return sqlite3.connect(self.database, **self.connection_params)
    

    @contextmanager
    def transaction(self): 
        """ Creates context manager to use class methods in transaction """

        if self.in_transaction():
            raise RuntimeError("Nested transactions are not permited")

        self._trans = self.connect()
        self._trans_cursor = self._trans.cursor()
        
        logger.debug(f"{self.__class__.__name__}: Transaction started")
        
        try:
            yield

        except Exception as e:
            logger.error(f"{self.__class__.__name__}: Error while in transaction: {e}")
            logger.debug(e, exc_info=True)
            self._trans.rollback()
            raise e
        
        else:
            self._trans.commit()

        finally:
            self._trans.close()
            del(self._trans)
            del(self._trans_cursor)
            logger.debug(f"{self.__class__.__name__}: Transaction finished")

    
    def in_transaction(self) -> bool:
        """ Returns True if instance is in transaction """
        return hasattr(self, "_trans") and hasattr(self, "_trans_cursor")

    
    @overload
    def _fetch(self, query : str, method : Literal["fetchone"], *args) -> SqlRow: ...
    @overload
    def _fetch(self, query : str, method : Literal["fetchall", "fetchmany"], *args) -> list[SqlRow]: ...

    def _fetch(self, query : str, method : Literal["fetchone", "fetchall", "fetchmany"], *args) -> SqlRow | list[SqlRow]:
        
        logger.debug(f"{self.__class__.__name__}: {query}")

        if self.in_transaction():
            self._trans_cursor.execute(query)
            return getattr(self._trans_cursor, method)(*args)

        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            return getattr(cursor, method)(*args)

    
    def execute(self, query : str, *args) -> None:
        """
        Shortcut to connect() -> execute() -> commit() for single operations. 
        Can be used in transaction using `transaction()` manager.

        Args:
            query (str): SQL query to execute on SQLite3 DB
            *args (Any): Arguments to the execution
        """

        logger.debug(f"{self.__class__.__name__}: {query} {args}")

        if self.in_transaction():
            self._trans_cursor.execute(query, *args)
            return
        
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query, *args)
            conn.commit()
    
    
    def create_table(self) -> None:
        """ Create table if not exists """
       
        query = sql.create_table(
            self.tablename, self.columns, 
            self.types, self.primary
        )

        self.execute(query)


    def drop_table(self) -> None:
        """ Drops table if it exists. """
        self.execute(sql.drop_table(self.tablename))


    def fetchone(self, query : str) -> SqlRow:
        return self._fetch(query, "fetchone")
    

    def fetchmany(self, query : str, size : int = 1) -> list[SqlRow]:
        return self._fetch(query, "fetchmany", size)
    

    def fetchall(self, query : str) -> list[SqlRow]:
        return self._fetch(query, "fetchall")
    
    
    def insert(self, *args, **kwargs) -> None:
        """ Insert single row. """
        query = sql.insert_row(self.tablename, self.columns)
        self.execute(query, args, **kwargs)

    
    def insert_many(self, rows: Sequence[SqlRow]) -> None:
        """
        Insert multiple rows in a single transaction.
        
        Args:
            rows: List of tuples, each tuple contains values for one row
                in the order of __columns__
        """
        
        query = sql.insert_many(self.tablename, self.columns, len(rows))
        flat_args = self._flatten_rows(rows)
        
        return self.execute(query, flat_args)
    

    def fetchall_iterator(self, query: str, batch_size: int) -> Generator[list[SqlRow], None, None]:
        """ Yields all rows in batches, each batch in its own transaction. """

        if not self.in_transaction():
            raise RuntimeError(
                (
                    "To use the `fetchall_iterator()` method you have "
                    "to keep open the transaction with `transaction()` manager"
                )
            )

        iter_cursor = self._trans.cursor()
        iter_cursor.execute(query)

        while batch := iter_cursor.fetchmany(batch_size):
            yield batch
        
    
    def delete_rows(self, where_clause : str) -> None:
        query = sql.delete_rows(self.tablename, where_clause)
        return self.execute(query)

    
    def delete_eq(self, column : str, equals : SqlValue | Sequence[SqlValue]) -> None:
        """ Deletes row, where `column` values are equal to `equals` """
        where_clause = sql.where_equals(column, equals)
        return self.delete_rows(where_clause)
    
    
    def select(self, columns : str | list[str] = "*", where_clause : str | None = None) -> list[SqlRow]:
        """Select rows from the table."""
        query = sql.select(self.tablename, columns, where_clause)
        return self.fetchall(query)
    

    def select_eq(self, column : str, equals : SqlValue | Sequence[SqlValue], return_columns : str | list[str] = "*") -> list[SqlRow]:
        """ Returns rows or `return_columns` of rows where `column` value is equal to `equals` """
        where_clause = sql.where_equals(column, equals)
        return self.select(return_columns, where_clause)
    

    def update(self, where_clause : str, set_values : dict[str, SqlValue]) -> None:
        """
        Updates columns based on `where_clause` 
    
        Args:
            where_clause (str): describes search filter with SQL condition query
            set_values (dict[str, SqlValue]): dict where keys are column names and
                values are corresponding new values to set
        """
        
        query = sql.update(self.tablename, where_clause, set_values)
        self.execute(query)


    def upsert(self, *args, **kwargs) -> None:
        """ Upsert (update or insert) single row """
        query = sql.upsert(self.tablename, self.columns, self.primary)
        self.execute(query, args, **kwargs)
    

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"database={self.database}, "
            f"tablename={self.tablename}, "
            f"columns={sql.format_list(self.columns)}, "
            f"primary_key={sql.format_list(self.primary)})"
        )
    

    def __len__(self) -> int:
        length = self.fetchone(sql.count(self.tablename))[0]
        if not isinstance(length, int):
            return 0
        return length
    

    def __iter__(self) -> Generator[SqlRow, None, None]:
        """ Database rows iterator """
        if not self.in_transaction():
            raise RuntimeError(
                (
                    "To use the __iter__ method you have "
                    "to keep open the transaction with `transaction()` manager"
                )
            )
        
        iter_cursor = self._trans.cursor()
        iter_cursor.execute(sql.select(self.tablename))

        while row := iter_cursor.fetchone(): 
            yield row
    

    @property
    def columns(self) -> list[str]:
        return self.__columns__

    
    @property
    def types(self) -> list[str]:
        return self.__types__

    
    @property
    def primary(self) -> list[str]:
        return self.__primary__

    
    @property
    def tablename(self) -> str:
        return self.__tablename__
