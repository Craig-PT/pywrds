__author__ = 'cpt'

import unittest
from pandas import Series, DataFrame
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from pywrds import utility as wrds_util
from dateutil.relativedelta import relativedelta


class UtilityTest(unittest.TestCase):

    def setUp(self):
        # Example
        idx = [datetime(2011, 2, 7, 0, 0),
               datetime(2011, 2, 7, 0, 1),
               datetime(2011, 2, 8, 0, 1, 30),
               datetime(2011, 2, 9, 0, 2),
               datetime(2011, 2, 10, 0, 4),
               datetime(2011, 2, 11, 0, 5),
               datetime(2011, 2, 12, 0, 5, 10),
               datetime(2011, 2, 12, 0, 6),
               datetime(2011, 2, 13, 0, 8),
               datetime(2011, 2, 14, 0, 9)]
        self.idx = pd.Index(idx)

        vals = np.arange(len(idx)).astype(float)
        self.s = Series(vals, index=idx)
        self.df = DataFrame({'a': vals, 'b':2*vals}, index=idx)

    def test_rolling_apply(self):
        """Test with window based on dates.
        """

        # Test different frequencies
        # 1. Daily.
        func = lambda x: x.mean()
        window = relativedelta(days=3)
        res = wrds_util.rolling_apply(self.s, window=window,
                                      func=func)
        exp_res = wrds_util.rolling_mean(self.s, window=window)
        # TODO: Check res equal ... how to do for Series?

        func = lambda x: x[x == 0.0].count()
        res = wrds_util.rolling_apply(self.s, window=window,
                                      func=func)
        self.assertTrue(res.values.sum() == 4, "Should only have 4 zero counts")

    def test_rolling_mean(self):
        """Test with window based on dates.
        """

        # Test different frequencies
        # 1. Daily.
        window = relativedelta(days=3)
        rm = pd.rolling_mean(self.s, window='3D')
        rm = wrds_util.rolling_mean(self.s, window=window)
        # TODO: Some check ...

if __name__ == "__main__":
    unittest.main()
