from train.predictions import predictions
from train.ParametrizedPredictionsTest import ParametrizedPredictionsTest


class PredictionsTest(ParametrizedPredictionsTest):
    def test_predictions(self):
        pred = predictions()
        model = pred.get_predictions(self.X_train, self.y_train)

        self.assertTrue("sklearn.pipeline.Pipeline" == type(model))
