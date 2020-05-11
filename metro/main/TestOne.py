from call_to_api import call_to_api
from ParametrizedTestCase import ParametrizedTestCase


class TestOne(ParametrizedTestCase):

    def test_records_not_empty(self):
        print(self.year)
        records = call_to_api.get_information(self,self.year,self.month,self.station)
        self.assertNotEqual(len(records), 0, note=" the names should be uppercase because bla bla bla")