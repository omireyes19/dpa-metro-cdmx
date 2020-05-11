from call_to_api import call_to_api
from translation import translation
from ParametrizedTestCase import ParametrizedTestCase


class TestOne(ParametrizedTestCase):
    def test_records_not_empty(self):
        records = call_to_api.get_information(self,self.year,self.month,self.station)
        self.assertNotEqual(len(records), 0)

    def test_full_layout(self):
        df = translation.get_dataframe(self,self.raw_json)
        number_of_columns = len(df.columns)
        self.assertEqual(number_of_columns, 3)