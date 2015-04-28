__author__ = 'cpt'

# Script to test wrdsapi functioning.

import pywrds as pywrds

# Initialise a WrdsSession
from pywrds import wrdsapi


# Initialise a WrdsSession
test_session = wrdsapi.WrdsSession()

# Call for some data.
test_session.get_wrds('crsp.dsf', 2007, 2, 1)
