import unittest

import os
import logging
import sys

from src.utils  import format_logging, download_file, CHINOOK_URL
from src.tables import (Employees, Coordinates, coord_schema,
                        coords_data, employees_data)
from sqlengine  import schema

TEST_DIR   = "temp/"
CHINOOK_DB = "temp/chinook.db"
TEST_DB    = "temp/test.db"

_TEARDOWM = False

format_logging()

logger = logging.getLogger(__name__)


class TestSqlTable(unittest.TestCase):
    
    def setUp(self) -> None:
        
        if not os.path.exists(TEST_DIR):
            logger.info(f"Creating directory {TEST_DIR}")
            os.mkdir(TEST_DIR)

        if not os.path.exists(CHINOOK_DB):
            logger.info(f"Downloading {CHINOOK_DB}")
            download_file(CHINOOK_URL, CHINOOK_DB)
        
        self.coord_table    = Coordinates(TEST_DB, True)
        self.empl_table     = Employees(TEST_DB, True)
        self.coord_table_s  = schema.table_from_schema(":memory:", coord_schema)
        self.chinook_tables = schema.table_from_database(CHINOOK_DB)

        logger.info('Setup complete')


    def remove_test_dir(self):
        
        if os.path.exists(CHINOOK_DB):
            os.remove(CHINOOK_DB)
        
        if os.path.exists(TEST_DIR):
            try:
                os.rmdir(TEST_DIR)
            except OSError as e:
                logger.error("Can't remove %s: %s", TEST_DIR, e)

    
    def tearDown(self) -> None:

        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)

        if _TEARDOWM:
            self.remove_test_dir()

        logger.info('Teardown complete')

    
    def test_getitem_single_primary(self):
        self.coord_table.create_table()
        self.coord_table.insert_many(coords_data)

        retrieved = self.coord_table[:5]
        for row_or, row_re in zip(coords_data[:5], retrieved):
            self.assertEqual(row_or[0], row_re[0])
            self.assertEqual(row_or[1], row_re[1])
        
        
    def test_getitem_double_primary(self):
        self.empl_table.create_table()
        self.empl_table.insert_many(employees_data)


if __name__ == '__main__':
    
    if "--teardown-all" in sys.argv:
        _TEARDOWM = True
        sys.argv.remove("--teardown-all")
    
    unittest.main()