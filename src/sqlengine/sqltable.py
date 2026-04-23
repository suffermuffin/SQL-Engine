import logging
import os
import sqlite3
from abc import abstractmethod
from contextlib import contextmanager
from typing import Iterable, Sequence, Literal, Generator, overload

from .utils import sqlgen as sql
from .utils.types import SqlRow, SqlValue

logger = logging.getLogger(__name__)


class SqlTableMixin:

    __columns__   : list[str]
    __types__     : list[str]
    __primary__   : list[str]
    __tablename__ : str

    def __init__(self, db_filename: str, force_drop : bool = False) -> None:
        
        self.db_filename  = db_filename

        self._validate_attributes()
        self._validate_write_db(force_drop)
        self.create_table()

    
    def _validate_write_db(self, force_drop : bool):

        if not self.db_filename.endswith('.db'):
            raise ValueError("`db_filename` must have '.db' extension")
        
        if not os.path.exists(self.db_filename):
            logger.info(f'{self.__class__.__name__}: {self.db_filename} does not exist. Creating...')
            parent_dir = self.db_filename.removesuffix(os.path.basename(self.db_filename))
            if parent_dir: 
                os.makedirs(parent_dir, exist_ok=True)

        elif force_drop:
            self.drop_table()

    
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


    
    def _insert_args(self, *args):
        query = sql.insert_row(self.tablename, self.columns)
        self.execute(query, args)


    @staticmethod
    def _flatten_rows(rows : Sequence[Iterable]):
        return [item for row in rows for item in row]
    
    
    def connect(self) -> sqlite3.Connection:
        """ Shortcut to connection context manager """
        return sqlite3.connect(self.db_filename)
    

    @contextmanager
    def transaction(self): 
        """ Creates context manager to use class methods in transaction """

        if self.in_transaction():
            raise RuntimeError("Nested transactions are not permited")

        self._trans = self.connect()
        self._trans_cursor = self._trans.cursor()
        
        try:
            logger.debug(f"{self.__class__.__name__}: Transaction started")
            yield

        except Exception as e:
            logger.error(f"{self.__class__.__name__}: Error while in transaction:")
            logger.error(e, exc_info=True)
            self._trans.rollback()
            raise e
        
        else:
            self._trans.commit()

        finally:
            logger.debug(f"{self.__class__.__name__}: Transaction finished")
            self._trans.close()
            del(self._trans)
            del(self._trans_cursor)

    
    def in_transaction(self) -> bool:
        """ Returns True if instance is in transaction """
        return hasattr(self, "_trans") and hasattr(self, "_trans_cursor")

    
    @overload
    def _fetch(self, query : str, method : Literal["fetchone"], *args) -> SqlRow: ...
    @overload
    def _fetch(self, query : str, method : Literal["fetchall", "fetchmany"], *args) -> list[SqlRow]: ...

    def _fetch(self, query : str, method : Literal["fetchone", "fetchall", "fetchmany"], *args) -> SqlRow | list[SqlRow]:
        
        logger.debug(f"{self.__class__.__name__}.{method}(): {query}")

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
        Can be used in transaction using `transaction()` manager 
        """

        logger.debug(f"{self.__class__.__name__}: {query} {self._flatten_rows(args) or ''}")

        if self.in_transaction():
            self._trans_cursor.execute(query, *args)
            return
        
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query, *args)
            conn.commit()


    def drop_table(self):
        self.execute(sql.drop_table(self.tablename))


    def fetchone(self, query : str) -> SqlRow:
        return self._fetch(query, "fetchone")
    

    def fetchmany(self, query : str, size : int = 1) -> list[SqlRow]:
        return self._fetch(query, "fetchmany", size)
    

    def fetchall(self, query : str) -> list[SqlRow]:
        return self._fetch(query, "fetchall")
    

    def create_table(self):
        """ Create table if not exists """
       
        query = sql.create_table(
            self.tablename, self.columns, 
            self.types, self.primary
        )

        self.execute(query)
    

    def insert_many(self, rows: list[tuple]) -> None:
        """
        Insert multiple rows in a single transaction.
        
        Args:
            rows: List of tuples, each tuple contains values for one row
                in the order of __columns__
        """
        
        query = sql.bulk_insert(self.tablename, self.columns, len(rows))
        flat_args = self._flatten_rows(rows)
        
        return self.execute(query, flat_args)
    

    def fetchall_iterator(self, query: str, batch_size: int = 1000) -> Generator[list[SqlRow], None, None]:
        """ Yields all rows in batches, each batch in its own transaction. """

        if not self.in_transaction():
            raise RuntimeError(("To use `fetchall_iterator()` method, first you have "
                                "to keep open transaction using `transaction()` manager"))

        self._trans_cursor.execute(query)

        while batch := self._trans_cursor.fetchmany(batch_size):
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
    

    def select_eq(
            self, 
            column : str, 
            equals : SqlValue | Sequence[SqlValue],
            return_columns : str | list[str] = "*"
        ) -> list[SqlRow]:
        """ Returns rows or `return_columns` of rows where `column` value is equal to `equals` """
        where_clause = sql.where_equals(column, equals)
        return self.select(return_columns, where_clause)
    

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"db_filename={self.db_filename}, "
            f"table={self.tablename}, "
            f"columns={sql.format_list(self.columns)}, "
            f"primary_key={sql.format_list(self.primary)}"
            f")"
        )
    

    @abstractmethod
    def insert(self, *args, **kwargs) -> None:
        """ Insert single row. """
        self._insert_args(*args, **kwargs)
        raise NotImplementedError("Declare this method with correct types")
    

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
