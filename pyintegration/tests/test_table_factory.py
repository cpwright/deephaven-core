#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
import unittest
from dataclasses import dataclass

import jpy
import numpy as np

from deephaven2 import DHError, read_csv, time_table, empty_table, merge, merge_sorted, dtypes, new_table
from deephaven2.column import byte_col, char_col, short_col, bool_col, int_col, long_col, float_col, double_col, \
    string_col, datetime_col, pyobj_col, jobj_col
from deephaven2.constants import NULL_DOUBLE, NULL_FLOAT, NULL_LONG, NULL_INT, NULL_SHORT, NULL_BYTE
from deephaven2.table_factory import DynamicTableWriter
from tests.testbase import BaseTestCase
import time

JArrayList = jpy.get_type("java.util.ArrayList")


@dataclass
class TableFactoryCustomClass:
    f1: int
    f2: str


class TableFactoryTestCase(BaseTestCase):
    def setUp(self):
        self.test_table = read_csv("tests/data/test_table.csv")
        #print ("Sleeping")
        #time.sleep(10)
        #print ("Slept")

    def tearDown(self) -> None:
        self.test_table = None
        raise Exception("Why am I here instead of broken?")

    def test_merge(self):
        t1 = self.test_table.update(formulas=["Timestamp=new io.deephaven.time.DateTime(0L)"])
        t2 = self.test_table.update(formulas=["Timestamp=io.deephaven.time.DateTime.now()"])
        mt = merge([t1, t2])
        self.assertFalse(mt.is_refreshing)

if __name__ == '__main__':
    unittest.main()
