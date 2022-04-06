#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest
#import time
#import jpy
#import gc
from dataclasses import dataclass

import numpy as np

from deephaven2 import DHError, new_table, dtypes
from deephaven2.column import byte_col, char_col, short_col, bool_col, int_col, long_col, float_col, double_col, \
    string_col, datetime_col, pyobj_col, jobj_col
from deephaven2.constants import NULL_LONG, MAX_LONG
from deephaven2.numpy import to_numpy, to_table
from deephaven2._jcompat import j_array_list


@dataclass
class TestNumyCustomClass:
    f1: int
    f2: str


class NumpyTestCase(unittest.TestCase):
    def setUp(self):
        print("Creating input cols", flush=True)
        input_cols = [
            pyobj_col(name="PyObj", data=[TestNumyCustomClass(1, "1"), TestNumyCustomClass(-1, "-1")]),
        ]
        print("Creating table", flush=True)
        self.test_table = new_table(cols=input_cols)

        print("Creating numpy array", flush=True)
        self.np_array_dict = {
            "PyObj": np.array([TestNumyCustomClass(1, "1"), TestNumyCustomClass(-1, "-1")]),
        }
        print("Created numpy array", flush=True)

    def tearDown(self) -> None:
        self.test_table = None
        #print("Sleeping")
        #time.sleep(5)
        #print("Collect")
        #gc.collect()
        #print("System.GC")
        #jpy.get_type("java.lang.System").gc()
        #print("Sleep Again")
        #time.sleep(5)
        #print("Sleep Again and Again")
        #time.sleep(5)
        #print("Sleep Done" )

    def test_to_numpy(self):
        pass
        print("Doing test")
        for col in self.test_table.columns:
            with self.subTest(f"test single column to numpy- {col.name}"):
                np_array = to_numpy(self.test_table, [col.name])
                self.assertEqual((2, 1), np_array.shape)
                np.array_equal(np_array, self.np_array_dict[col.name])
        print("Did test")

if __name__ == '__main__':
    unittest.main()
