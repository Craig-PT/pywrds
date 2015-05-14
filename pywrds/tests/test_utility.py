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

    def test_rolling_mean(self):
        """Test with window based on dates.
        """

        # Test different frequencies
        # 1. Daily.
        rm = wrds_util.rolling_mean(self.s, window=relativedelta(days=3))
        rm = wrds_util.rolling_mean(self.s, window='2D')
        # TODO: Some check ...

if __name__ == "__main__":
    unittest.main()
