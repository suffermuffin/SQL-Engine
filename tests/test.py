import unittest

import os
import logging
import sys

from src.utils  import format_logging, download_file, CHINOOK_URL
from src.tables import Employees, Coordinates, coord_schema
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

    
    def test_setup(self):
        coord_table    = Coordinates(TEST_DB, True)
        empl_table     = Employees(TEST_DB, True)
        coord_table_s  = schema.table_from_schema(TEST_DB, coord_schema)
        chinook_tables = schema.table_from_database(CHINOOK_DB)


if __name__ == '__main__':
    
    if "--teardown" in sys.argv:
        _TEARDOWM = True
        sys.argv.remove("--teardown")
    
    unittest.main()