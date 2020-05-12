from call_to_api import call_to_api
from translation import translation
from ParametrizedTestCase import ParametrizedTestCase
from date_variables import date_variables
from train.interquartile_range import interquartile_range
from train.predictions import predictions

class TestOne(ParametrizedTestCase):
    def test_records_not_empty(self):
        records = call_to_api.get_information(self,self.year,self.month,self.station)
        self.assertNotEqual(len(records), 0)

    def test_full_layout(self):
        df = translation.get_dataframe(self,self.raw_json)
        number_of_columns = len(df.columns)
        self.assertEqual(number_of_columns, 3)

    def test_date_cols_created(self):
        previous_columns = len(self.csv_data.columns)
        df = date_variables.add_date_variables(csv_data)
        self.assertEqual(len(df.columns), previous_columns + 3)

    def test_label_creation(self):
        label_df = interquartile_range.create_label(interquartile_range.join_range(self.cleaned_data, interquartile_range.calculate_range(self.cleaned_data)))
        distinct_labels = label_df['label'].distinct()
        self.assertIn(distinct_labels, range(1,4)

    def test_influx_values(self):
        min_influx = min(self.cleaned_data['influx'])
        self.assertGreaterEqual(min_influx, 0)

    def test_predictions(self):
        predictions_df = predictions.get_predictions(spark,self.model_data)
        column_names_df = predictions_df.columns
        self.assertTrue(column_names_df.contains('prediction')
