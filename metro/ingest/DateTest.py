from ingest.date_variables import date_variables
from ingest.ParametrizedDateTest import ParametrizedDateTest


class DateTest(ParametrizedDateTest):
    def test_date_cols_created(self):
        df = date_variables.add_date_variables(self, self.csv_data)
        self.assertEqual(len(df.columns), 10)
