import sqlite3
import logging
import os

from typing import overload, Literal, Sequence
from contextlib import contextmanager

from .types import SqlValue, SqlRow


logger = logging.getLogger("sqlengine")
logger.setLevel(os.getenv("SQL_ENGINE_LOG_LEVEL", "WARNING").upper())


class ConnectionManager:

    _trans : sqlite3.Connection
    _trans_cursor : sqlite3.Cursor

    def __init__(self, database : str, **connection_params) -> None:
        
        self.database = database
        self.connection_params = connection_params

        self._is_managed_transaction = False

    
    @overload
    def _fetch(self, query : str, args : SqlRow, 
               method : Literal["fetchone"]) -> SqlRow: ...
    @overload
    def _fetch(self, query : str, args : SqlRow, 
               method : Literal["fetchall"]) -> list[SqlRow]: ...

    def _fetch(
            self, 
            query  : str, 
            args   : SqlRow = (), 
            method : Literal["fetchone", "fetchall"] = "fetchall"
        ) -> SqlRow | list[SqlRow]:
        
        logger.debug(f"{self.database}: {query} {args}")

        if self.in_transaction():
            self._trans_cursor.execute(query, args)
            return getattr(self._trans_cursor, method)()

        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query, args)
            return getattr(cursor, method)()

    
    @overload    
    def _execute(self, query : str, args : tuple[SqlValue, ...], method : Literal["execute"]) -> None: ...
    @overload
    def _execute(self, query : str, args : Sequence[SqlRow], method : Literal["executemany"]) -> None: ...
    
    def _execute(self, query : str, args : tuple[SqlValue, ...] | Sequence[SqlRow] = (), method : Literal["execute", "executemany"] = "execute") -> None:
        """
        Shortcut to connect() -> execute[<many>]() -> commit() for single operations. 
        Can be used in transaction using `transaction()` manager.

        Args:
            query (str): SQL query to execute on SQLite3 DB
            *args (Any): Arguments to the execution
            method (str): "execute" or "executemany"
        """
        logger.debug(f"{self.database}: {query} {args}")

        if self.in_transaction():
            getattr(self._trans_cursor, method)(query, args)
            return
        
        with self.connect() as conn:
            cursor = conn.cursor()
            getattr(cursor, method)(query, args)
            conn.commit()


    def execute(self, query : str, *args : SqlValue) -> None:
        """
        Shortcut to connect() -> execute() -> commit() for single operations. 
        Can be used in transaction using `transaction()` manager.

        Args:
            query (str): SQL query to execute on SQLite3 DB
            *args (tuple[SqlValue, ...]): Arguments to the execution
        """
        return self._execute(query, args, method="execute")
    
    
    def executemany(self, query : str, args : Sequence[SqlRow]) -> None:
        """
        Shortcut to connect() -> executemany() -> commit() for single operations. 
        Can be used in transaction using `transaction()` manager.

        Args:
            query (str): SQL query to execute on SQLite3 DB
            *args (list[tuple[SqlValue, ...]]): Arguments to the execution
        """
        return self._execute(query, args, method="executemany")


    def fetchone(self, query : str, *args : SqlValue) -> SqlRow:
        """
        Fetch first row based on `query`

        Args:
            query (str): SQL query
            *args (tuple[SqlValue, ...]): Arguments to the execution

        Returns:
            row (SqlRow): Single row
        """
        return self._fetch(query, args, method="fetchone")
    

    def fetchmany(self, query : str, *args : SqlValue, size : int = 1) -> list[SqlRow]:
        """
        Fetch first `size` rows based on `query`

        Args:
            query (str): SQL query
            *args (tuple[SqlValue, ...]): Arguments to the execution
            size (str): Number of rows to return

        Returns:
            rows (list[SqlRow]): list of `size` rows
        """
        logger.debug(f"{self.database}: {query} {args}")

        if self.in_transaction():
            self._trans_cursor.execute(query, args)
            return self._trans_cursor.fetchmany(size)

        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query, args)
            return cursor.fetchmany(size)
    

    def fetchall(self, query : str, *args : SqlValue) -> list[SqlRow]:
        """
        Fetch all rows based on `query`

        Args:
            query (str): SQL query
            *args (tuple[SqlValue, ...]): Arguments to the execution

        Returns:
            rows (list[SqlRow]): list of rows
        """
        return self._fetch(query, args, method="fetchall")

    
    def in_transaction(self) -> bool:
        """ Returns True if instance is in transaction """
        return hasattr(self, "_trans") and hasattr(self, "_trans_cursor")


    def connect(self) -> sqlite3.Connection:
        """ Shortcut to sqlite3 connection context manager """
        return sqlite3.connect(self.database, **self.connection_params)
    

    def open(self) -> None:
        """ Opens unmanaged transaction """
        if self.in_transaction():
            raise RuntimeError("Can't re-open existing connection")
        
        self._trans = self.connect()
        self._trans_cursor = self._trans.cursor()

    
    def close(self) -> None:
        """ Closes unmanaged transaction """
        if not self.in_transaction():
            return
        
        if self._is_managed_transaction:
            raise RuntimeError("Can't manually close managed transaction")
        
        self._trans_cursor.close()
        self._trans.close()
        del(self._trans_cursor)
        del(self._trans)


    def commit(self) -> None:
        if not self.in_transaction():
            raise RuntimeError("Can't commit outside transaction mode")
        
        self._trans.commit()


    def rollback(self) -> None:
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

            >>> with table.transaction():
            >>>     for idx, age in table.select("ID", "Age"):
            >>>         table.update("Age", age + 1).where.eq("ID", idx).then.execute()
            >>>     print(table.select)
        """
        
        self.open()
        self._is_managed_transaction = True
        logger.debug(f"{self.database}: Transaction started")
        
        try:
            yield

        except Exception as e:
            logger.error(f"{self.database}: Error while in transaction: {e}")
            logger.debug(e, exc_info=True)
            self._trans.rollback()
            raise e
        
        else:
            if autocommit:
                self._trans.commit()

        finally:
            self._is_managed_transaction = False
            self.close()
            logger.debug(f"{self.database}: Transaction finished")
    
    
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

