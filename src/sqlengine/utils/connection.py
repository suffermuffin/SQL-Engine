import logging
import os
import sqlite3

from contextlib import contextmanager

from ..sqltable   import SqlTableMixin
from .._internal  import ConnectionManager
from ..exceptions import NestedTransactionError


logger = logging.getLogger("sqlengine")
logger.setLevel(os.getenv("SQL_ENGINE_LOG_LEVEL", "WARNING").upper())


@contextmanager
def shared_connection(*args : SqlTableMixin, autocommit : bool = True, **connection_params):
    """
    Creates shared connection across one or more databases for multiple tables by
    manipulating their transaction attributes

    Args:
        *args (SqlTableMixin): tuple of instances of table classes inherited from `SqlTableMixin`
        autocommit (bool): If `True`, will commit changes at the end of transaction
        **connection_params (dict): Params to create connections with. This argument will be shared
            across different connections. Reference: https://docs.python.org/3/library/sqlite3.html#sqlite3.connect
    
    Examples:

    ```python
    from sqlengine.utils import shared_connection
    with shared_connection(table1, table2, **table1.connection_params):
        for (id1,), (id2, temp) in zip(table1.select("ID").limit(20), table2.select("ID", "Temperature").limit(20)):
            if id1 == id2:
                table.update.where.eq("ID", id2).then.set("Salary", temp).execute()
    ```
    """

    tables_in_trans = [
        str(table) for table in args if table.in_transaction()
    ]
    
    if tables_in_trans:
        raise NestedTransactionError(f"Tables {tables_in_trans} are already in transaction")
    
    unique_databases = set(table.database for table in args)
    database_map : dict[str, list[ConnectionManager]] = {}
    
    for db in unique_databases:
        database_map[db] = [table.conn for table in args if table.database == db]

    connections : list[sqlite3.Connection] = []

    for database, con_managers in database_map.items():
        
        con = sqlite3.connect(database, **connection_params)
        connections.append(con)
        
        for con_man in con_managers:
            table_cur = con.cursor()
            setattr(con_man, "_trans", con)
            setattr(con_man, "_trans_cursor", table_cur)
            con_man._is_managed_transaction = True

    logger.debug(f"Starting shared transaction across {len(database_map)} databases")
    
    try:
        yield
    
    except Exception as e:
        logger.error(f"Error while in shared transaction: {e}")
        logger.debug(e, exc_info=True)
        
        for con in connections:
            con.rollback()
        
        raise e
    
    else:
        if autocommit:
            for con in connections:
                con.commit()
    
    finally:
        
        for table in args:
            con_man = table.conn
            con_man._is_managed_transaction = False
            con_man._trans_cursor.close()
            delattr(con_man, "_trans_cursor")
            delattr(con_man, "_trans")
        
        for con in connections:
            con.close()
        
        logger.debug("Shared transaction finished")
