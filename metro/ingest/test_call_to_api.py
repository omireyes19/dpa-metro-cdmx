import marbles.core
from call_to_api import call_to_api
import sys

class TestMarbles(marbles.core.TestCase):

    def test_records_not_empty(self):
        #print(self.year)
        records = call_to_api.get_information("2020","01","Chabacano")
        self.assertNotEqual(len(records), 0, note=" the names should be uppercase because bla bla bla")
