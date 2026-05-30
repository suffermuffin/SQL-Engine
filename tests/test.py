import unittest

import sqlite3
import os
import logging
import sys
import warnings

from sqlengine       import schema, SqlTableMixin, Primary
from sqlengine       import exceptions
from sqlengine.utils import shared_connection

from src.utils  import format_logging, download_file, CHINOOK_URL
from src.tables import Employees, Coordinates, Point, coord_schema, COORDS_DATA, EMPLOYEES_DATA


TEST_DIR   = "temp/"
CHINOOK_DB = "temp/chinook.db"
TEST_DB    = "temp/test.db"
TEST_CSV   = "temp/test.csv"
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

        if os.path.exists(TEST_CSV):
            os.remove(TEST_CSV)            
        
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

        self.coord_table.conn.close()
        self.empl_table.conn.close()
        self.coord_table_s.conn.close()

        self.coord_table.drop_table(True)
        self.empl_table.drop_table(True)
    
    
    def test_getitem_single_primary(self):

        cases = [
            slice(1, 6, 2),
            slice(10, 6, -1),
            slice(None, 5, None),
            slice(None, 100, None),
            slice(None, None, 3),
            slice(None, None, 100),
        ]
        table = self.coord_table
        
        table.conn.open()
        table.insert_many(COORDS_DATA)
        
        for s in cases:
            with self.subTest(slice=s):

                for row_or, row_re in zip(COORDS_DATA[s], self.coord_table[s]):
                    self.assertEqual(row_or[0], row_re[0])
                    self.assertEqual(row_or[1], row_re[1])
                    self.assertIsInstance(row_re[2], Point)
                    assert isinstance(row_re[2], Point)
                    self.assertEqual(row_or[2].x, row_re[2].x)
                    self.assertEqual(row_or[2].y, row_re[2].y)
                    self.assertEqual(row_or[3],   row_re[3])
        
        table.conn.close()
            
    
    def test_getitem_double_primary(self):
        
        table = self.empl_table

        cases = [(keys[0], keys[1]) for keys in EMPLOYEES_DATA]

        table.conn.open()
        table.insert_many(EMPLOYEES_DATA)

        for i, (k1, k2) in enumerate(cases):
            with self.subTest(key=(k1, k2)):
                re_row = table[k1, k2]
                or_row = EMPLOYEES_DATA[i]
                self.assertEqual(len(re_row), len(or_row))
                for val_re, val_or in zip(re_row, or_row):
                    self.assertEqual(val_re, val_or)
        
        table.conn.close()



    def test_schema(self):
        
        chinook_tables = schema.table_from_database(CHINOOK_DB)
        schemas        = schema.get_database_schemas(CHINOOK_DB)
        names          = schema.get_database_tablenames(CHINOOK_DB)

        self.assertEqual(len(chinook_tables), len(names))
        self.assertEqual(len(chinook_tables), len(schemas))

        for table, sch in zip(chinook_tables, schemas):
            self.assertEqual(table.schema, sch, msg=f"table `{table.tablename}` schema: {table.schema} != {sch}")
    
    
    def test_transaction_nesting(self):
        
        with self.assertRaises(exceptions.NestedTransactionError):
            with self.coord_table.transaction():
                with self.coord_table.transaction():
                    pass

        with self.assertRaises(exceptions.NestedTransactionError):
            with self.coord_table.transaction():
                with shared_connection(self.coord_table, self.empl_table, **self.coord_table.connection_params):
                    pass

        with self.assertRaises(exceptions.NestedTransactionError):
            with shared_connection(self.coord_table, self.empl_table, **self.coord_table.connection_params):
                with self.coord_table.transaction():
                    pass

    
    def test_shared_connection(self):

        table_mem = self.coord_table_s
        table_cor = self.coord_table
        table_emp = self.empl_table
        
        with shared_connection(table_cor, table_emp, table_mem, **table_cor.connection_params):
            table_mem.create_table()

            table_cor.insert_many(COORDS_DATA)
            table_emp.insert_many(EMPLOYEES_DATA)

            for row in table_cor.select:
                table_mem.insert(*row)
            
            self.assertEqual(table_cor.shape, table_mem.shape)

    
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

            biggest_table.select()

            logging.getLogger("sqlengine").setLevel("CRITICAL")
            
            for batch in biggest_table.select("*").fetchmany_iterator(1000):
                copy_biggest_table.insert_many(batch)
            
            logging.getLogger("sqlengine").setLevel(LOG_LVL)
            
            self.assertEqual(biggest_table.shape, copy_biggest_table.shape)

    
    def test_unmanaged_connection_iteration(self):
        
        table = self.coord_table

        table.conn.open()
        table.insert_many(COORDS_DATA)
        
        for idx, _, _, temp in table.select:
            assert isinstance(temp, float)
            table.update.set("temp", temp*2).where.eq("ID", idx).then.execute()

        table.commit()
        table.conn.close()

        rows = table.select.fetchall()

        self.assertEqual(len(rows), len(COORDS_DATA))

        for row_or, row_re in zip(COORDS_DATA, rows):
            self.assertEqual(row_or[-1]*2, row_re[-1])

    
    def test_unmanaged_connection_rollback(self):
        
        self.coord_table.conn.open()
        self.coord_table.insert_many(COORDS_DATA)

        assert len(COORDS_DATA) > 0, "No coords data"

        self.assertEqual(len(self.coord_table), len(COORDS_DATA))

        self.coord_table.rollback()
        self.coord_table.conn.close()

        self.assertEqual(len(self.coord_table), 0)


    def test_unmanaged_connection_attrs(self):
        with self.assertRaises(exceptions.OutsideTransactionError):
            self.empl_table.tx_cursor

        with self.assertRaises(exceptions.OutsideTransactionError):
            self.empl_table.tx_conn

        self.empl_table.conn.open()

        conn = self.empl_table.tx_conn
        self.assertIsInstance(conn, sqlite3.Connection)

        curs = self.empl_table.tx_cursor
        self.assertIsInstance(curs, sqlite3.Cursor)

        self.empl_table.conn.close()

        with self.assertRaises(exceptions.OutsideTransactionError):
            self.empl_table.tx_cursor

        with self.assertRaises(exceptions.OutsideTransactionError):
            self.empl_table.tx_conn

    
    def test_unmanaged_attr_manip_edge_case(self):

        self.empl_table.conn.open()
        conn = self.empl_table.tx_conn
        conn.close()

        with self.assertRaises(sqlite3.ProgrammingError):
            self.empl_table.conn.close()

        # additional teardown
        delattr(self.empl_table.conn, "_trans_cursor")
        delattr(self.empl_table.conn, "_trans")

    
    def test_transaction_attr_manip_edge_case(self):

        with self.assertRaises(sqlite3.ProgrammingError):
            with self.empl_table.transaction():
                self.empl_table.tx_conn.close()

        # additional teardown
        delattr(self.empl_table.conn, "_trans_cursor")
        delattr(self.empl_table.conn, "_trans")


    def test_transaction_edge_case(self):
        
        with self.assertRaises(exceptions.TransactionError):
            with self.empl_table.transaction():
                self.empl_table.conn.close()

        with self.assertRaises(exceptions.TransactionError):
            with self.empl_table.transaction():
                self.empl_table.conn.open()

    
    def test_shared_connection_edge_case(self):
        
        with self.assertRaises(exceptions.TransactionError):
            with shared_connection(self.coord_table, self.empl_table, **self.coord_table.connection_params):
                self.coord_table.conn.close()

        with self.assertRaises(exceptions.TransactionError):
            with shared_connection(self.coord_table, self.empl_table, **self.coord_table.connection_params):
                self.empl_table.conn.open()


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
        
        table = self.coord_table

        with shared_connection(table):
            table.tx_conn
            table.tx_cursor

            table.insert_many(COORDS_DATA)

            for idx, _, _, temp in table.select:
                assert isinstance(temp, float)
                table.update.set("temp", temp*2).where.eq("ID", idx).then.execute()

        self.assertEqual(len(table), len(COORDS_DATA))

    
    def test_delete(self):

        table = self.coord_table

        table.insert_many(COORDS_DATA)
        lenght_init = len(table)
        table.delete.where.eq("ID", 0).then.execute()
        length_after = len(table)
        self.assertEqual(lenght_init - 1, length_after)
        self.assertEqual(table[0], None)

    
    def test_multiple_statements(self):

        table = self.coord_table

        table.insert_many(COORDS_DATA)
        table.delete.where.eq("ID", 0).then.execute()
        table.update.set("name", "new_loc").where.eq("ID", 1).then.execute()

        self.assertEqual(len(table), len(COORDS_DATA) - 1)
        
        loc = table[1][1]
        self.assertEqual(loc, "new_loc")

    
    def test_statements_in_transaction(self):

        table = self.coord_table_s

        with table.transaction():
            table.create_table()
            table.insert_many(COORDS_DATA)
            table.delete.where.eq("ID", 0).then.execute()
            table.update.set("name", "new_loc").where.eq("ID", 1).then.execute()

            self.assertEqual(len(table), len(COORDS_DATA) - 1)
            loc = table[1][1]
            self.assertEqual(loc, "new_loc")

    
    def test_select_statements(self):
        table = schema.table_from_database(CHINOOK_DB, "Customer")

        table.conn.open()
        
        limit = 15

        with self.subTest("limit"):
            rows = table.select.limit(limit).fetchall()
            self.assertEqual(len(rows), limit)

        with self.subTest("Order, limit and column"):
            rows = table.select("CustomerId").limit(limit).order_by("CustomerId").fetchall()
            self.assertEqual(len(rows), limit)
            for row, idx in zip(rows, range(1, limit+1)):
                self.assertEqual(len(row), 1)
                self.assertEqual(row[0], idx)
        

        with self.subTest("Complex expression"):

            rows = table\
                .select("CustomerId", "Address", "City", "SupportRepId")\
                .where\
                    .in_("SupportRepId", (3,4))\
                .then\
                    .order_by("CustomerId")\
                    .limit(50)\
                    .fetchall()
            
        table.conn.close()


    def test_update_statement(self):
        _table  = schema.table_from_database(CHINOOK_DB, "Customer")
        _schema = _table.schema

        table = schema.table_from_schema(":memory:", _schema)
        
        _table.open_connection()
        table.open_connection()
        
        table.create_table()

        
        with self.subTest("Copy customers"):
            for _row in _table.select.fetchmany_iterator(50):
                table.insert_many(_row)
            
            table.commit()

        _table.close_connection()
        
        
        with self.subTest("Update cities"):
        
            new_city = "Karaganda"
            table.update.set("City", new_city).where.eq("Country", "Czech Republic").then.execute()
            rows = table.select("City").where.in_("CustomerId", (6, 5)).then.fetchall()

            for row in rows:
                self.assertEqual(new_city, row[0])

        
        with self.subTest("Compare all cities"):
            rows = table.select("City").where.eq("Country", "Czech Republic").then.fetchall()
            for row in rows:
                self.assertEqual(new_city, row[0])
        
        table.close_connection()

    
    def test_class_declaration(self):
        
        with self.subTest("Should raise attr error as of no primaries"):
            with self.assertRaises(exceptions.TableDeclarationError):
                class EdgeCaseTable1(SqlTableMixin):
                    val : str
                    key : int
            
        
        with self.subTest("Should raise attr error as N is not declared in columns"):
            with self.assertRaises(exceptions.TableDeclarationError):
                class EdgeCaseTable2(SqlTableMixin):
                    val : str
                    key : Primary[int]

                    __primary__ = ['N']
            
        
        with self.subTest("Should raise attr error as of double declaration of columns"):
            with self.assertRaises(exceptions.TableDeclarationError):
                class EdgeCaseTable3(SqlTableMixin):
                    val : str
                    key : Primary[int]

                    __columns__ = ["val", "key"]
                    __types__   = [str, "INTEGER"]

        
        with self.subTest("Should raise attr error as of double declaration of primaries"):
            with self.assertRaises(exceptions.TableDeclarationError):
                class EdgeCaseTable4(SqlTableMixin):
                    val : str
                    key : Primary[int]

                    __primary__ = ["key"]

        
        with self.subTest("Should work"):
            class EdgeCaseTable5(SqlTableMixin):
                
                __columns__ = ["sql_col1", "sql_col2"]
                __types__   = ["NVARCHAR(160)", float]
                __primary__ = ["sql_col1"]

                val : str
                key : Primary[int]


            table = EdgeCaseTable5(TEST_DB, True)

            self.assertEqual(table.types, ["NVARCHAR(160)", float, str, int])
            
            with table.transaction():
                table.insert("sql_col1_value", 1.0, "val_value", 0)
                row = table['sql_col1_value', 0]

                self.assertEqual(len(row), 4)

                table.drop_table(True)

        
        with self.subTest("Custom Prime"):
            
            from datetime import datetime

            class DateTime(datetime):
                
                def to_sql(self) -> str:
                    return self.strftime("%Y-%m-%d %H:%M")

                @classmethod
                def from_sql(cls, sql : bytes):
                    return cls.fromisoformat(sql.decode('utf-8'))
                
                def __repr__(self):
                    return f"DateTime({self.time})"


            class ReservationIndex(SqlTableMixin):

                user_id : Primary[int]
                room_id : Primary[str]
                time_at : Primary[DateTime]
                user_name : str | None


            table = ReservationIndex(":memory:")
            time  = DateTime.now()
            
            with table.transaction(autocommit=False):
                
                table.create_table()
                table.insert(1, "loft_1", time, None)
                
                self.assertEqual(len(table), 1)

                dt = table.select("time_at").fetchone()[0]
                
                self.assertIsInstance(dt, DateTime)
    

    def test_kwargs_insert(self):

        class Homies(SqlTableMixin):

            __columns__   = ["ID", "Name", "Age"]
            __types__     = [int, str | None, int | None]
            __primary__   = ["ID"]

        table = Homies(":memory:")

        with table.transaction():
            table.create_table()
            
            iidx, iage, iname = (0, 20, "John")
            table.insert(ID=iidx, Age=iage, Name=iname)
            idx, name, age = table[iidx]

            self.assertEqual(idx,  iidx)
            self.assertEqual(name, iname)
            self.assertEqual(age,  iage)

            uidx, uage = (1, 14)
            table.upsert(ID=uidx, age=uage)
            idx, name, age = table[uidx]

            self.assertEqual(idx,  uidx)
            self.assertIsNone(name)
            self.assertEqual(age,  uage)

            uidx, uage = (1, 20)
            table.upsert(uidx, age=uage)
            idx, name, age = table[uidx]

            self.assertEqual(idx,  uidx)
            self.assertIsNone(name)
            self.assertEqual(age,  uage)

            uname = "Boris"
            table.upsert(uidx, name=uname)
            idx, name, age = table[uidx]

            self.assertEqual(idx,  uidx)
            self.assertEqual(name, uname)
            self.assertEqual(age,  uage)


    def test_conversions(self):

        from sqlengine.utils import to_csv, to_dicts, to_dicts_stream
        
        table = self.coord_table

        def validate_indicies(data : list[tuple], path : str):
            with open(path, 'r') as f:
                for i, line in enumerate(f.readlines()):
                    row = line.strip().split(',')
                    if i == 0:
                        self.assertEqual(row, table.columns)
                        continue
                    self.assertEqual(data[i-1][0], int(row[0]))

        
        _half = len(COORDS_DATA)//2
        data_1, data_2  = COORDS_DATA[_half:], COORDS_DATA[:_half]

        with table.transaction(autocommit=False):

            with self.subTest("Csv One Shot"):
                table.insert_many(data_1)
                to_csv(table, TEST_CSV)
                
                validate_indicies(data_1, TEST_CSV)

            table.rollback()

            with self.subTest("Strean Csv"):
                table.insert_many(data_2)
                to_csv(table, TEST_CSV, stream_batch_size=2)

                validate_indicies(data_2, TEST_CSV)
                
            table.rollback()

            with self.subTest("Dict One Shot"):
                table.insert_many(data_1)
                dicts = to_dicts(table)

                self.assertEqual(len(dicts), len(data_1))

                for dict_, data in zip(dicts, data_1):
                    self.assertEqual(table.columns, list(dict_.keys()))
                    self.assertEqual(data, tuple(dict_.values()))
            
            table.rollback()

            with self.subTest("Dict Stream"):
                
                batch_size = 2
                table.insert_many(data_2)
                
                assert len(table) == len(data_2)

                result = []
                
                for batch in to_dicts_stream(table, batch_size):
                    result.extend(batch)

                for dict_, data in zip(result, data_2):
                    self.assertEqual(table.columns, list(dict_.keys()))
                    self.assertEqual(data, tuple(dict_.values()))
                

    # Generated
    
    def test_where_basic_equality(self):
        table = self.coord_table
        table.insert_many(COORDS_DATA)

        # eq
        where = table.select.where.eq("ID", 3)
        query, args = where.build()
        self.assertEqual(query, "ID = ?")
        self.assertEqual(args, (3,))

        # neq, gt, lt, gte, lte
        where = table.select.where.neq("temp", 2.2).gt("ID", 1).lt("temp", 5.0)
        query, args = where.build()
        self.assertIn("temp != ?", query)
        self.assertIn("ID > ?", query)
        self.assertIn("temp < ?", query)
        self.assertEqual(args, (2.2, 1, 5.0))

        # like
        where = table.select.where.like("name", "loc%")
        query, args = where.build()
        self.assertEqual(query, "name LIKE ?")
        self.assertEqual(args, ("loc%",))

    
    def test_where_in_and_between(self):
        table = self.coord_table
        table.insert_many(COORDS_DATA)

        # IN
        where = table.select.where.in_("ID", [1, 3, 5])
        query, args = where.build()
        self.assertEqual(query, "ID IN (?, ?, ?)")
        self.assertEqual(args, (1, 3, 5))

        # BETWEEN
        where = table.select.where.between("temp", 2.0, 6.0)
        query, args = where.build()
        self.assertEqual(query, "temp BETWEEN ? AND ?")
        self.assertEqual(args, (2.0, 6.0))

        with self.assertRaises(exceptions.SqlEngineError):
            table.select.where.in_("ID", "1,2,3")

    
    def test_where_is_null_and_inverted(self):
        table = self.empl_table
        table.insert_many(EMPLOYEES_DATA)

        # IS NULL
        where = table.select.where.is_null("position")
        query, args = where.build()
        self.assertEqual(query, "position IS NULL")
        self.assertEqual(args, ())

        # Inverted (NOT)
        where = table.select.where.eq("salary", 100.0).inverted()
        query, args = where.build()
        self.assertEqual(query, "NOT (salary = ?)")
        self.assertEqual(args, (100.0,))

    
    def test_where_join(self):
        table = self.empl_table
        table.insert_many(EMPLOYEES_DATA)

        where = table.select.where.eq("salary", 100.0).eq("position", "pos0").join("OR")
        query, args = where.build()

        self.assertEqual(query, "(salary = ? OR position = ?)")
        self.assertEqual(args, (100.0, "pos0"))

    
    def test_where_custom_and_call(self):
        table = self.coord_table
        
        where = table.select.where("temp > ? AND name != ?", 5.0, "loc5")
        query, args = where.build()
        self.assertEqual(query, "temp > ? AND name != ?")
        self.assertEqual(args, (5.0, "loc5"))

    
    def test_where_reset(self):
        table = self.coord_table
        w = table.select.where
        w.eq("a", 1).gt("b", 2)
        w.reset()
        query, args = w.build()
        self.assertEqual(query, "")
        self.assertEqual(args, ())

    
    def test_select_columns_and_fetchone(self):
        table = self.coord_table
        table.insert_many(COORDS_DATA)

        # __call__
        row = table.select("ID", "name").where.eq("ID", 3).then.fetchone()
        self.assertEqual(row, (3, "loc3"))

        # columns
        row = table.select.columns("temp").where.eq("ID", 5).then.fetchone()
        self.assertEqual(row, (5.5,))

    
    def test_select_order_by_and_limit(self):
        table = self.coord_table
        table.insert_many(COORDS_DATA)

        rows = table.select("ID", "temp").order_by("temp", ascending=False).limit(3).fetchall()
        expected = [(10, 10.10), (7, 7.7), (6, 6.6)]
        self.assertEqual(rows, expected)

    
    def test_select_aggregate(self):
        table = self.empl_table
        table.insert_many(EMPLOYEES_DATA)

        # COUNT
        count = table.select.aggregate("COUNT").fetchone()[0]
        self.assertEqual(count, len(EMPLOYEES_DATA))

        # SUM
        total_salary = table.select("salary").aggregate("SUM").fetchone()[0]
        expected_sum = sum(row[3] for row in EMPLOYEES_DATA)

        assert isinstance(total_salary, float)

        self.assertAlmostEqual(total_salary, expected_sum)

        # AVG, MIN, MAX
        avg = table.select("salary").aggregate("AVG").fetchone()[0]
        min_ = table.select("salary").aggregate("MIN").fetchone()[0]
        max_ = table.select("salary").aggregate("MAX").fetchone()[0]
        
        assert isinstance(avg, float)

        self.assertAlmostEqual(avg, expected_sum / len(EMPLOYEES_DATA))
        self.assertEqual(min_, 100.0)
        self.assertEqual(max_, 144.4)

        # multi aggregation
        with self.assertRaises(exceptions.SqlEngineError):
            table.select.aggregate("COUNT").aggregate("SUM")

    
    def test_select_fetchmany_and_iterator(self):
        table = self.coord_table
        table.insert_many(COORDS_DATA)

        # fetchmany
        rows = table.select.limit(5).fetchmany(3)
        self.assertEqual(len(rows), 3)

        # fetchmany_iterator
        with table.transaction():
            batches = list(table.select.fetchmany_iterator(2))
        
            self.assertEqual(len(batches), 5)
        
            all_rows = [row for batch in batches for row in batch]
            self.assertEqual(len(all_rows), len(COORDS_DATA))

        # __iter__
        with table.transaction():
            rows_iter = list(table.select.where.gt("ID", 2).then)
            expected_ids = [row[0] for row in COORDS_DATA if row[0] > 2]
            self.assertEqual(len(rows_iter), len(expected_ids))

    
    def test_select_repr_html(self):
        table = self.coord_table
        table.insert_many(COORDS_DATA)

        # HTML
        html = table.select._repr_html_()
        self.assertIsInstance(html, str)

        assert isinstance(html, str)

        self.assertIn("<table", html)

        agg = table.select.aggregate("COUNT")
        self.assertIsNone(agg._repr_html_())

    # UPDATE
    def test_update_execute(self):
        table = self.coord_table
        table.insert_many(COORDS_DATA)

        table.update.set("temp", 99.9).where.eq("ID", 2).then.execute()
        row = table.select("temp").where.eq("ID", 2).then.fetchone()
        self.assertEqual(row, (99.9,))

        table.update.set("name", "updated").where.in_("ID", [0, 3, 5]).then.execute()
        rows = table.select("name").where.in_("ID", [0,3,5]).then.fetchall()
        names = [row[0] for row in rows]
        self.assertEqual(names, ["updated", "updated", "updated"])

    # SELECT
    def test_delete_execute(self):
        table = self.coord_table
        table.insert_many(COORDS_DATA)
        initial_len = len(table)

        table.delete.where.eq("ID", 7).then.execute()
        self.assertEqual(len(table), initial_len - 1)
        self.assertIsNone(table[7])

        # Can't delete without where
        with self.assertRaises(exceptions.SqlEngineError):
            table.delete.execute()


    def test_complex_query(self):
        table = self.empl_table
        table.insert_many(EMPLOYEES_DATA)

        rows = table.select("ID", "salary")\
            .where\
                .gt("salary", 110)\
                .lt("salary", 140)\
            .then\
                .order_by("ID")\
                .fetchall()
        
        
        expected = [(1, 111.1), (1, 111.1), (2, 122.2), (3, 133.3)]
        
        self.assertEqual(rows, expected)

        # join with OR
        rows = table.select("ID", "position")\
            .where\
                .eq("position", "pos0")\
                .eq("ID", 3)\
                .join("OR")\
            .then\
                .fetchall()

        expected_positions = [(row[0], row[1]) for row in EMPLOYEES_DATA if row[4] == "pos0" or row[0] == 3]
        self.assertEqual(len(rows), len(expected_positions))

    
    def test_custom_query(self):
        table = self.coord_table
        table.insert_many(COORDS_DATA)

        # Completely custom query
        rows = table.select.custom_query("SELECT name, temp FROM Coordinates WHERE ID > ?", 5).fetchall()
        expected = [(name, temp) for id_, name, _, temp in COORDS_DATA if id_ > 5]
        self.assertEqual(rows, expected)


if __name__ == '__main__':
    
    if "--teardown-all" in sys.argv:
        _TEARDOWM = True
        sys.argv.remove("--teardown-all")
    
    unittest.main()