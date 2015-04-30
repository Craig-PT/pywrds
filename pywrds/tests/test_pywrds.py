__author__ = 'cpt'

import unittest
from pywrds.wrdsapi import WrdsSession
from pywrds.query import CrspQuery


class PywrdsSessionTest(unittest.TestCase):

    def setUp(self):
        self.session = WrdsSession()

    def test_CrpsQuery(self):
        return NotImplementedError

    def test_query(self):

        testQuery = CrspQuery(self.session)

        testQuery.get_crsp()

        print self.query
        return True

if __name__ == "__main__":
    unittest.main()
