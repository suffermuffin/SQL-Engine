import logging
import os
import sqlite3
from contextlib import contextmanager
from typing import Sequence, Literal, Generator, overload

from .utils import sqlgen as sql
from .utils.types import SqlRow, SqlValue

logger = logging.getLogger(__name__)
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
        __types__ (list[str]): Colum types of the table
        __primary__ (list[str]): List of primary keys
    """

    __tablename__ : str
    __columns__   : list[str]
    __types__     : list[str]
    __primary__   : list[str]

    def __init__(self, database: str | Literal[":memory:"], force_drop : bool = False, **connection_params) -> None:
        
        self.database = database
        self.connection_params = connection_params

        self._validate_attributes()
        self._validate_write_db(force_drop)


    def _validate_write_db(self, force_drop : bool):

        if self.database == ":memory:":
            logger.debug(f"{self.tablename}: Using in-memory database")
            return
        
        if not os.path.exists(self.database):
            logger.info(f'{self.tablename}: {self.database} does not exist. Creating...')
            parent_dir = self.database.removesuffix(os.path.basename(self.database))
            if parent_dir: 
                os.makedirs(parent_dir, exist_ok=True)

        elif force_drop is True:
            self.drop_table(confirm=True)

        self.create_table()

    
    def _validate_attributes(self):

        if not hasattr(self, "__tablename__"):
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
            raise AttributeError(f'__types__ and __columns__ length mismatch: types = {n_types}, columns = {n_cols}')
        
        wrong_primaries = [
            prim for prim in self.__primary__ if
            prim not in self.__columns__
        ]

        if wrong_primaries:
            raise AttributeError(f'`__primary__`: Keys {wrong_primaries} can\'t be primaries as they are not declared in __columns__')


    def connect(self) -> sqlite3.Connection:
        """ Shortcut to sqlite3 connection context manager """
        return sqlite3.connect(self.database, **self.connection_params)
    

    @contextmanager
    def transaction(self): 
        """ 
        Creates context manager to use class methods in transaction 
        
        Examples:

            >>> from src.sqlengine import sqlgen as sql
            >>> with table.transaction():
            >>>     for idx, name, age in table:
            >>>         where = sql.where_equals("ID", idx)
            >>>         table.update(where, {"Age" : age + 1})
            >>>     print(table.select())
        """
        if self.in_transaction():
            raise RuntimeError("Nested transactions are not permitted")

        self._trans = self.connect()
        self._trans_cursor = self._trans.cursor()
        
        logger.debug(f"{self.tablename}: Transaction started")
        
        try:
            yield

        except Exception as e:
            logger.error(f"{self.tablename}: Error while in transaction: {e}")
            logger.debug(e, exc_info=True)
            self._trans.rollback()
            raise e
        
        else:
            self._trans.commit()

        finally:
            self._trans.close()
            del(self._trans_cursor)
            del(self._trans)
            logger.debug(f"{self.tablename}: Transaction finished")

    
    def in_transaction(self) -> bool:
        """ Returns True if instance is in transaction """
        return hasattr(self, "_trans") and hasattr(self, "_trans_cursor")

    
    @overload
    def _fetch(self, query : str, method : Literal["fetchone"], *args) -> SqlRow: ...
    @overload
    def _fetch(self, query : str, method : Literal["fetchall", "fetchmany"], *args) -> list[SqlRow]: ...

    def _fetch(self, query : str, method : Literal["fetchone", "fetchall", "fetchmany"], *args) -> SqlRow | list[SqlRow]:
        
        logger.debug(f"{self.tablename}: {query}")

        if self.in_transaction():
            self._trans_cursor.execute(query)
            return getattr(self._trans_cursor, method)(*args)

        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            return getattr(cursor, method)(*args)
        
    
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
    
    
    def create_table(self) -> None:
        """ Create table if not exists """
       
        query = sql.create_table(
            self.tablename, self.columns, 
            self.types, self.primary
        )

        self.execute(query)


    def drop_table(self, confirm : bool = False) -> None:
        """ Drops table if it exists. """
        
        if not confirm:
            logger.warning("Pass `confirm=True` to drop the table")
            return
        
        self.execute(sql.drop_table(self.tablename))


    def fetchone(self, query : str) -> SqlRow:
        """
        Fetch first row based on `query`

        Args:
            query (str): SQL query

        Returns:
            out (SqlRow): Single row
        """
        return self._fetch(query, "fetchone")
    

    def fetchmany(self, query : str, size : int = 1) -> list[SqlRow]:
        """
        Fetch first `size` rows based on `query`

        Args:
            query (str): SQL query
            size (str): Number of rows to return

        Returns:
            out (list[SqlRow]): list of `size` rows
        """
        return self._fetch(query, "fetchmany", size)
    

    def fetchall(self, query : str) -> list[SqlRow]:
        """
        Fetch all rows based on `query`

        Args:
            query (str): SQL query

        Returns:
            out (list[SqlRow]): list of rows
        """
        return self._fetch(query, "fetchall")
    
    
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

    
    def update(self, where_clause : str, set_values : dict[str, SqlValue]) -> None:
        """
        Updates columns based on `where_clause` 
    
        Args:
            where_clause (str): describes search filter with SQL condition query
            set_values (dict[str, SqlValue]): dict where keys are column names and
                values are corresponding new values to set

        Example:
            >>> # This will set "unemployed" as Occupation and 0.0 as Salary
            >>> # for all the salespeople and CEO
            >>> from sqlengine import sqlgen as sql
            >>> where = sql.where("Occupation", "IN", ["seller", "CEO"])
            >>> table.update(where, {"Occupation" : "unemployed", "Salary" : 0.0})
        """
        query = sql.update(self.tablename, where_clause, set_values)
        self.execute(query)


    def insert_many(self, rows: Sequence[SqlRow]) -> None:
        """
        Bulk insert multiple rows
        
        Args:
            rows (list[SqlRow]): List of tuples, each tuple contains 
                values for one row in the order of __columns__
        """
        query = sql.insert_row(self.tablename, self.columns)
        return self.executemany(query, rows)
    
    
    def delete_rows(self, where_clause : str) -> None:
        query = sql.delete_rows(self.tablename, where_clause)
        return self.execute(query)

    
    def delete_eq(self, column : str, equals : SqlValue | Sequence[SqlValue]) -> None:
        """ Deletes row, where `column` values are equal to `equals` """
        where_clause = sql.where_equals(column, equals)
        return self.delete_rows(where_clause)
    
    
    def select(self, columns : str | list[str] = "*", where_clause : str | None = None) -> list[SqlRow]:
        """
        Fetch all rows of `columns` from the table with optional `where_clause` filter.\n
        This executes query in form of "SELECT `columns` FROM table WHERE `where_clause`"
        
        Args:
            columns (str | list[str]): columns of which rows to return. If "*" is provided -
                returns all.
            where_clause (str): describes search filter with SQL condition query
        
        Returns:
            out (list[SqlRow]): list of all rows, where values are in order of provided
                `columns`

        Examples:

            >>> from sqlengine import sqlgen as sql
            >>> where = sql.where("Occupation", "IN", ["seller", "worker"])
            >>> table.select(["ID", "Name"], where)
            >>> table.select("*", "ID = 0")
        """
        query = sql.select(self.tablename, columns, where_clause)
        return self.fetchall(query)
    

    def select_eq(self, column : str, equals : SqlValue | Sequence[SqlValue], return_columns : str | list[str] = "*") -> list[SqlRow]:
        """ 
        Returns rows or `return_columns` of rows where `column` value is equal to `equals`.
        Interface/shortcut for `select` method. This executes query in form of
        "SELECT `return_columns` FROM table WHERE `column` [= | IN] `equals`"
        
        Args:
            column (str): column which rows to compare to `equals`.
            equals (SqlValue | list[SqlValue]): values to search for in `column`.
            return_columns (str | list[str]): rows of which columns to return. If "*" is provided -
                returns all.
        
        Returns:
            out (list[SqlRow]): list of all rows, where values are in order of provided
                `return_columns`

        Examples:

            >>> table.select_eq("Occupation", ["seller", "worker"], ["ID", "Name"])
            >>> table.select_eq("ID", 0, "*")
        """
        where_clause = sql.where_equals(column, equals)
        return self.select(return_columns, where_clause)
    

    def fetchall_iterator(self, query: str, batch_size: int) -> Generator[list[SqlRow], None, None]:
        """
        Yields all rows in batches, each batch in its own transaction.
        
        Args:
            query (str): SQL query
            batch_size (int): Size of each batch

        Examples:

            >>> from sqlengine import sqlgen as sql
            >>> where = sql.where("Age" ">" 30)
            >>> query = sql.select(table.tablename, where)
            >>> with table.transaction():
            >>>     for batch in table.fetchall_iterator(query, 1000):
            >>>         process_batch(batch)
        """
        if not self.in_transaction():
            raise RuntimeError("To use the `fetchall_iterator()` method you have \
                    to keep open the transaction with `transaction()` manager")

        iter_cursor = self._trans.cursor()
        iter_cursor.execute(query)

        while batch := iter_cursor.fetchmany(batch_size):
            yield batch
    

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
            raise RuntimeError("To use the __iter__ method you have \
                to keep open the transaction with `transaction()` manager")
        
        iter_cursor = self._trans.cursor()
        iter_cursor.execute(sql.select(self.tablename))

        while row := iter_cursor.fetchone(): 
            yield row


    @property
    def columns(self) -> list[str]:
        """ List of table column names """
        return self.__columns__

    
    @property
    def types(self) -> list[str]:
        """ List of table column dtypes """
        return self.__types__

    
    @property
    def primary(self) -> list[str]:
        """ List of table column primary keys """
        return self.__primary__

    
    @property
    def tablename(self) -> str:
        """ Name of the table """
        return self.__tablename__
    

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
