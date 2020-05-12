from train.predictions import predictions
from train.ParametrizedPredictionsTest import ParametrizedPredictionsTest


class PredictionsTest(ParametrizedPredictionsTest):
    def test_predictions(self):
        pred = predictions()
        predictions_df = pred.get_predictions(self.spark, self.model_data)
        column_names_df = predictions_df.columns

        print(column_names_df)
        self.assertTrue(column_names_df.contains('prediction'))
