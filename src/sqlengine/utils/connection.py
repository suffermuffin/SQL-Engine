import sqlite3
import logging
import os

from contextlib import contextmanager
from ..sqltable import SqlTableMixin

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("SQL_ENGINE_LOG_LEVEL", "WARNING").upper())


@contextmanager
def shared_connection(*args : SqlTableMixin, **connection_params):
    """
    Creates shared connection across one or more databases for multiple tables by
    manipulating their transaction attributes

    Args:
        *args (SqlTableMixin): tuple of instances of table classes inherited from `SqlTableMixin`
        **connection_params (dict): Params to create connections with. This argument will be shared
            across different connections. Reference: https://docs.python.org/3/library/sqlite3.html#sqlite3.connect
    
    Examples:

        >>> from sqlengine.utils import shared_connection
        >>> with shared_connection(table1, table2, **table1.connection_params):
        >>>     for row1, row2 in zip(table1, table2):
        >>>         id1  = row1[table1.row_id("ID")]
        >>>         temp = row2[table1.row_id("temp")]
        >>>         id2  = row2[table2.row_id("ID")]
        >>>         if id1 == id2:
        >>>             where = sql.where_equals("ID", id2)
        >>>             table_e.update(where, {"salary" : temp})
    """

    tables_in_trans = [table.__class__.__name__ for table in args if table.in_transaction()]
    
    if tables_in_trans:
        raise RuntimeError(f"Tables {tables_in_trans} are already in transaction")
    
    unique_databases = set(table.database for table in args)
    database_map : dict[str, list[SqlTableMixin]] = {}
    
    for db in unique_databases:
        database_map[db] = [table for table in args if table.database == db]

    connections : list[sqlite3.Connection] = []

    for database, tables in database_map.items():
        
        con = sqlite3.connect(database, **connection_params)
        connections.append(con)
        
        for table in tables:
            table_cur = con.cursor()
            setattr(table, "_trans", con)
            setattr(table, "_trans_cursor", table_cur)

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
        for con in connections:
            con.commit()
    
    finally:
        
        for table in args:
            table.tx_cursor.close()
            delattr(table, "_trans_cursor")
            delattr(table, "_trans")
        
        for con in connections:
            con.close()
        
        logger.debug("Shared transaction finished")
