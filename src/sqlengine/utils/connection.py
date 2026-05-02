import sqlite3
import logging
import os

from contextlib import contextmanager
from ..sqltable import SqlTableMixin

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("SQL_ENGINE_LOG_LEVEL", "WARNING").upper())


@contextmanager
def shared_connection(*args : SqlTableMixin, **connection_params):

    tables_in_trans = [table.__class__.__name__ for table in args if table.in_transaction()]
    
    if tables_in_trans:
        raise RuntimeError(f"Tables {tables_in_trans} are already in transaction")
    
    unique_databases = set([table.database for table in args])

    logger.debug(f"Starting shared transaction across {len(unique_databases)} databases")

    database_map : dict[str, list[SqlTableMixin]] = {}
    
    for db in unique_databases:
        database_map[db] = []

    for table in args:
        database_map[table.database].append(table)

    connections : list[sqlite3.Connection] = []

    for database, tables in database_map.items():
        
        con = sqlite3.connect(database, **connection_params)
        connections.append(con)
        
        for table in tables:
            table_cur = con.cursor()
            setattr(table, "_trans", con)
            setattr(table, "_trans_cursor", table_cur)

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