import logging
import os
import sqlite3
import time
from abc import abstractmethod
from typing import Iterable, Sequence

from .utils import sqlgen as sql
from .utils.types import SqlRow, SqlValue

LOGGER = logging.getLogger(__name__)


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
            LOGGER.info(f'{self.__class__.__name__}: {self.db_filename} does not exist. Creating...')
            parent_dir = self.db_filename.removesuffix(os.path.basename(self.db_filename))
            if parent_dir: os.makedirs(parent_dir, exist_ok=True)

        elif force_drop:
            self._drop_table()

    
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


    def _drop_table(self):
        LOGGER.warning(f'DROPPING {self.tablename} TABLE IN 3s')
        time.sleep(3)
        self.execute(sql.drop_table(self.tablename))

    
    def _insert_args(self, *args):
        query = sql.insert_row(self.tablename, self.columns)
        self.execute(query, args)


    @staticmethod
    def _flatten_rows(rows : Sequence[Iterable]):
        return [item for row in rows for item in row]
    
    
    def connect(self):
        """ Helper to connect to the database """
        return sqlite3.connect(self.db_filename)

    
    def execute(self, query : str, *args) -> None:
        """ Shortcut to Connection.execute() -> commit() for single operations """

        LOGGER.debug(f"{self.__class__.__name__}: {query} {', '.join([str(a) for a in args])}")
        
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query, *args)
            conn.commit()

    
    def fetchall(self, query : str) -> list[SqlRow]:
        
        LOGGER.debug(f"{self.__class__.__name__}: {query}")

        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()

        return rows
    

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
        if not rows: return
        
        query     = sql.bulk_insert(self.tablename, self.columns, len(rows))
        flat_args = self._flatten_rows(rows)
        
        return self.execute(query, flat_args)
    
    
    def insert_many_transaction(self, rows: list[tuple], batch_size: int = 1000) -> None:
        """
        Insert many rows in batches, each batch in its own transaction.
        Good for very large datasets to avoid memory issues.
        """
        LOGGER.info(f"{self.__class__.__name__}: inserting {len(rows)} rows in {(len(rows)-1)//batch_size + 1} batches")
        
        with self.connect() as conn:
            cursor = conn.cursor()

            for i in range(0, len(rows), batch_size):
                
                batch = rows[i:i + batch_size]
                
                query         = sql.bulk_insert(self.tablename, self.columns, len(batch))
                flat_args     = self._flatten_rows(batch)
                
                cursor.execute(query, flat_args)
            
            conn.commit()
    
    
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
            f"columns={len(self.columns)}, "
            f"primary_key=({', '.join(self.primary)})"
            f")"
        )
    

    @abstractmethod
    def insert(self, *args, **kwargs) -> None:
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
