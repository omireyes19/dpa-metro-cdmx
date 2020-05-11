import marbles.core
from ingest.call_to_api import call_to_api

class TestMarbles(marbles.core.TestCase):
    def test_records_not_empty(self,year,month,station):
        records = call_to_api.get_information(year,month,station)
        self.assertNotEqual(len(records), 0, note=" the names should be uppercase because bla bla bla")
