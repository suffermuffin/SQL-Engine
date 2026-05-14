import unittest

import sqlite3
import os
import logging
import sys
import warnings

from sqlengine       import schema
from sqlengine       import sqlgen as sql
from sqlengine.utils import shared_connection

from src.utils  import format_logging, download_file, CHINOOK_URL
from src.tables import Employees, Coordinates, Point, coord_schema, COORDS_DATA, EMPLOYEES_DATA


TEST_DIR   = "temp/"
CHINOOK_DB = "temp/chinook.db"
TEST_DB    = "temp/test.db"
LOG_LVL    = os.getenv("LOG_LEVEL", "CRITICAL").upper()

_TEARDOWM = False


format_logging(LOG_LVL)
logging.getLogger("sqlengine").setLevel(LOG_LVL)

logger = logging.getLogger(__name__)


class TestSqlTable(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls) -> None:
        
        if not os.path.exists(TEST_DIR):
            logger.info(f"Creating directory {TEST_DIR}")
            os.mkdir(TEST_DIR)

        if not os.path.exists(CHINOOK_DB):
            logger.info(f"Downloading {CHINOOK_DB}")
            download_file(CHINOOK_URL, CHINOOK_DB)
        
        cls.coord_table   = Coordinates(TEST_DB, True)
        cls.empl_table    = Employees(TEST_DB, True)
        cls.coord_table_s = schema.table_from_schema(":memory:", coord_schema)

        logger.info('Setup complete')

    
    @classmethod
    def tearDownClass(cls) -> None:
        
        def remove_test_dir():
            if os.path.exists(CHINOOK_DB):
                os.remove(CHINOOK_DB)
            
            if os.path.exists(TEST_DIR):
                try:
                    os.rmdir(TEST_DIR)
                except OSError as e:
                    warnings.warn(f"Can't remove {TEST_DIR}: {repr(e)}")

        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)
        
        if _TEARDOWM:
            remove_test_dir()

        logger.info('Teardown complete')


    def setUp(self) -> None:
        self.coord_table.create_table()
        self.empl_table.create_table()

    
    def tearDown(self) -> None:
        
        self.assertFalse(self.coord_table.in_transaction())
        self.assertFalse(self.empl_table.in_transaction())
        self.assertFalse(self.coord_table_s.in_transaction())

        self.coord_table.close_connection()
        self.empl_table.close_connection()
        self.coord_table_s.close_connection()

        self.coord_table.drop_table(True)
        self.empl_table.drop_table(True)
    
    
    def test_getitem_single_primary(self):
        
        self.coord_table.insert_many(COORDS_DATA)

        for row_or, row_re in zip(COORDS_DATA[:5], self.coord_table[:5]):
            self.assertEqual(row_or[0], row_re[0])
            self.assertEqual(row_or[1], row_re[1])
            self.assertIsInstance(row_re[2], Point)
            assert isinstance(row_re[2], Point)
            self.assertEqual(row_or[2].x, row_re[2].x)
            self.assertEqual(row_or[2].y, row_re[2].y)
            self.assertEqual(row_or[3], row_re[3])
        
    
    def test_getitem_double_primary(self):
        
        self.empl_table.insert_many(EMPLOYEES_DATA)

        for data_or in EMPLOYEES_DATA:
            key_1, key_2 = data_or[:2]
            data_re = self.empl_table[[key_1, key_2]]
            self.assertEqual(len(data_or), len(data_re))
            for val_or, val_re in zip(data_or, data_re):
                self.assertEqual(val_or, val_re)
        
        self.empl_table.drop_table(True)


    def test_schema(self):
        
        chinook_tables = schema.table_from_database(CHINOOK_DB)
        schemas        = schema.get_database_schemas(CHINOOK_DB)
        names          = schema.get_database_tablenames(CHINOOK_DB)

        self.assertEqual(len(chinook_tables), len(names))
        self.assertEqual(len(chinook_tables), len(schemas))

        for table, sch in zip(chinook_tables, schemas):
            self.assertEqual(table.schema, sch, msg=f"table `{table.tablename}` schema: {table.schema} != {sch}")
    
    
    def test_transaction_nesting(self):
        
        with self.assertRaises(RuntimeError):
            with self.coord_table.transaction():
                with self.coord_table.transaction():
                    pass

        with self.assertRaises(RuntimeError):
            with self.coord_table.transaction():
                with shared_connection(self.coord_table, self.empl_table, **self.coord_table.connection_params):
                    pass

        with self.assertRaises(RuntimeError):
            with shared_connection(self.coord_table, self.empl_table, **self.coord_table.connection_params):
                with self.coord_table.transaction():
                    pass

    
    def test_shared_connection(self):
        
        with shared_connection(self.coord_table, self.empl_table, self.coord_table_s, **self.coord_table.connection_params):
            self.coord_table_s.create_table()

            self.coord_table.insert_many(COORDS_DATA)
            self.empl_table.insert_many(EMPLOYEES_DATA)

            for row in self.coord_table:
                self.coord_table_s.insert(*row)
            
            self.assertEqual(self.coord_table.shape, self.coord_table_s.shape)

    
    def test_shared_connection_commit(self):

        params = self.coord_table.connection_params
        step_1_shapes = (self.coord_table.shape, self.empl_table.shape)

        with shared_connection(self.coord_table, self.empl_table, **params):
            self.coord_table.insert_many(COORDS_DATA)
            self.empl_table.insert_many(EMPLOYEES_DATA)
            step_2_shapes = (self.coord_table.shape, self.empl_table.shape)
        
        self.assertEqual(step_2_shapes, (self.coord_table.shape, self.empl_table.shape))
        self.assertNotEqual(step_1_shapes, step_2_shapes)

    
    def test_transaction_commit_coord(self):

        step_1_coord_shape = self.coord_table.shape

        with self.coord_table.transaction():
            self.coord_table.insert_many(COORDS_DATA)
            step_2_coord_shape = self.coord_table.shape

        self.assertEqual(step_2_coord_shape, self.coord_table.shape)
        self.assertNotEqual(step_1_coord_shape, step_2_coord_shape)
    
    
    def test_transaction_commit_empl(self):

        step_1_empl_shape = self.empl_table.shape

        with self.empl_table.transaction():
            self.empl_table.insert_many(EMPLOYEES_DATA)
            step_2_empl_shape = self.empl_table.shape

        self.assertEqual(step_2_empl_shape, self.empl_table.shape)
        self.assertNotEqual(step_1_empl_shape, step_2_empl_shape)


    def test_shared_connection_rollback(self):
        
        params = self.coord_table.connection_params

        coords_data_1, coords_data_2 = COORDS_DATA[:len(COORDS_DATA)//2], COORDS_DATA[len(COORDS_DATA)//2:]
        employees_data_1, employees_data_2 = EMPLOYEES_DATA[:len(EMPLOYEES_DATA)//2], EMPLOYEES_DATA[len(EMPLOYEES_DATA)//2:]

        self.coord_table.insert_many(coords_data_1)
        self.empl_table.insert_many(employees_data_1)

        step_1_shapes = (self.coord_table.shape, self.empl_table.shape)

        try:
            with shared_connection(self.coord_table, self.empl_table, **params):
                self.coord_table.insert_many(coords_data_2)
                self.empl_table.insert_many(employees_data_2)
                raise ValueError
        except ValueError:
            pass

        self.assertEqual(step_1_shapes, (self.coord_table.shape, self.empl_table.shape))

        with shared_connection(self.coord_table, self.empl_table, **params):
            self.coord_table.insert_many(coords_data_2)
            self.empl_table.insert_many(employees_data_2)
            step_2_shapes = (self.coord_table.shape, self.empl_table.shape)
        
        self.assertEqual(step_2_shapes, (self.coord_table.shape, self.empl_table.shape))
    

    def test_transaction_rollback_coords(self):
        
        coords_data_1, coords_data_2 = COORDS_DATA[:len(COORDS_DATA)//2], COORDS_DATA[len(COORDS_DATA)//2:]
        self.coord_table.insert_many(coords_data_1)
        step_1_shape = self.coord_table.shape

        try:
            with self.coord_table.transaction():
                self.coord_table.insert_many(coords_data_2)
                raise ValueError
        except ValueError:
            pass

        self.assertEqual(step_1_shape, self.coord_table.shape)

    
    def test_transaction_rollback_empl(self):
        
        employees_data_1, employees_data_2 = EMPLOYEES_DATA[:len(EMPLOYEES_DATA)//2], EMPLOYEES_DATA[len(EMPLOYEES_DATA)//2:]

        self.empl_table.insert_many(employees_data_1)
        step_1_shape = self.empl_table.shape

        try:
            with self.empl_table.transaction():
                self.empl_table.insert_many(employees_data_2)
                raise ValueError
        except ValueError:
            pass

        self.assertEqual(step_1_shape, self.empl_table.shape)

    
    def test_big_operations(self):
        
        chinook_tables = schema.table_from_database(CHINOOK_DB)
        biggest_table = max(chinook_tables, key=lambda x: len(x))

        copy_biggest_table = schema.table_from_schema(":memory:", biggest_table.schema)

        with shared_connection(copy_biggest_table, biggest_table):
            
            copy_biggest_table.create_table()

            query = sql.select(biggest_table.tablename)

            for batch in biggest_table.fetchall_iterator(query, 1000):
                copy_biggest_table.insert_many(batch)
            
            self.assertEqual(biggest_table.shape, copy_biggest_table.shape)

    
    def test_unmanaged_connection_iteration(self):
        
        self.coord_table.open_connection()
        self.coord_table.insert_many(COORDS_DATA)
        
        for idx, _, _, temp in self.coord_table:
            assert isinstance(temp, float)
            self.coord_table.update(f"ID = {idx}", {"temp" : temp*2})

        self.coord_table.commit()
        self.coord_table.close_connection()

        rows = self.coord_table.select()

        self.assertEqual(len(rows), len(COORDS_DATA))

        for row_or, row_re in zip(COORDS_DATA, rows):
            self.assertEqual(row_or[-1]*2, row_re[-1])

    
    def test_unmanaged_connection_rollback(self):
        
        self.coord_table.open_connection()
        self.coord_table.insert_many(COORDS_DATA)

        assert len(COORDS_DATA) > 0, "No coords data"

        self.assertEqual(len(self.coord_table), len(COORDS_DATA))

        self.coord_table.rollback()
        self.coord_table.close_connection()

        self.assertEqual(len(self.coord_table), 0)


    def test_unmanaged_connection_attrs(self):
        with self.assertRaises(RuntimeError):
            self.empl_table.tx_cursor

        with self.assertRaises(RuntimeError):
            self.empl_table.tx_conn

        self.empl_table.open_connection()

        conn = self.empl_table.tx_conn
        self.assertIsInstance(conn, sqlite3.Connection)

        curs = self.empl_table.tx_cursor
        self.assertIsInstance(curs, sqlite3.Cursor)

        self.empl_table.close_connection()

        with self.assertRaises(RuntimeError):
            self.empl_table.tx_cursor

        with self.assertRaises(RuntimeError):
            self.empl_table.tx_conn

    
    def test_unmanaged_attr_manip_edge_case(self):

        self.empl_table.open_connection()
        conn = self.empl_table.tx_conn
        conn.close()

        with self.assertRaises(sqlite3.ProgrammingError):
            self.empl_table.close_connection()

        # additional teardown
        delattr(self.empl_table, "_trans_cursor")
        delattr(self.empl_table, "_trans")

    
    def test_transaction_attr_manip_edge_case(self):

        with self.assertRaises(sqlite3.ProgrammingError):
            with self.empl_table.transaction():
                self.empl_table.tx_conn.close()

        # additional teardown
        delattr(self.empl_table, "_trans_cursor")
        delattr(self.empl_table, "_trans")


    def test_transaction_edge_case(self):
        
        with self.assertRaises(RuntimeError):
            with self.empl_table.transaction():
                self.empl_table.close_connection()

        with self.assertRaises(RuntimeError):
            with self.empl_table.transaction():
                self.empl_table.open_connection()

    
    def test_shared_connection_edge_case(self):
        
        with self.assertRaises(RuntimeError):
            with shared_connection(self.coord_table, self.empl_table, **self.coord_table.connection_params):
                self.coord_table.close_connection()

        with self.assertRaises(RuntimeError):
            with shared_connection(self.coord_table, self.empl_table, **self.coord_table.connection_params):
                self.empl_table.open_connection()


    def test_transaction_manual_commit(self):

        with self.coord_table.transaction(autocommit=False):
            self.coord_table.insert_many(COORDS_DATA)

        self.assertEqual(len(self.coord_table), 0)

        with self.coord_table.transaction(autocommit=False):
            self.coord_table.insert_many(COORDS_DATA)
            self.coord_table.commit()

        self.assertEqual(len(self.coord_table), len(COORDS_DATA))


    def test_shared_connection_manual_commit(self):
        
        with shared_connection(self.coord_table, self.empl_table, autocommit=False):
            self.coord_table.insert_many(COORDS_DATA)
            self.empl_table.insert_many(EMPLOYEES_DATA)

        self.assertEqual(len(self.coord_table), 0)
        self.assertEqual(len(self.empl_table), 0)

        with shared_connection(self.coord_table, self.empl_table, autocommit=False):
            self.coord_table.insert_many(COORDS_DATA)
            self.empl_table.insert_many(EMPLOYEES_DATA)
            self.coord_table.commit()
            self.empl_table.commit()            

        self.assertEqual(len(self.coord_table), len(COORDS_DATA))
        self.assertEqual(len(self.empl_table), len(EMPLOYEES_DATA))


    def test_shared_connection_with_one_table(self):
        with shared_connection(self.coord_table):
            self.coord_table.tx_conn
            self.coord_table.tx_cursor

            self.coord_table.insert_many(COORDS_DATA)

            for idx, _, _, temp in self.coord_table:
                assert isinstance(temp, float)
                self.coord_table.update(f"ID = {idx}", {"temp" : temp*2})

        self.assertEqual(len(self.coord_table), len(COORDS_DATA))



if __name__ == '__main__':
    
    if "--teardown-all" in sys.argv:
        _TEARDOWM = True
        sys.argv.remove("--teardown-all")
    
    unittest.main()