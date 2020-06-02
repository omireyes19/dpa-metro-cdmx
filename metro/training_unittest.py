import luigi
import luigi.contrib.s3
import boto3
import unittest
from math import floor
import pandas as pd
from io import StringIO
from datetime import date
from label_creation_metadata import label_task_metadata
from train.ParametrizedPredictionsTest import ParametrizedPredictionsTest
from train.PredictionsTest import PredictionsTest

class training_unittest_task(luigi.Task):
    bucket_metadata = 'dpa-metro-metadata'
    today = date.today().strftime("%d%m%Y")
    year = luigi.IntParameter()
    month = luigi.IntParameter()

    def requires(self):
        return label_task_metadata(self.year,self.month)

    def run(self):
        ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
        s3_resource = ses.resource('s3')

        obj = s3_resource.Object("dpa-metro-label", "year={}/month={}/{}.csv".format(str(self.year), str(self.month).zfill(2), str(self.year)+str(self.month).zfill(2)))

        file_content = obj.get()['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(file_content))

        df = df[['date', 'day', 'month', 'station', 'line', 'day_of_week', 'holiday', 'label']]

        df['month_year'] = pd.DatetimeIndex(df['date']).to_period('M')
        dif_months = df['month_year'].unique()
        num_dif_months = len(dif_months)
        n = floor(num_dif_months * 0.7)
        filter_month = dif_months[[n]]
        X_train = df[df['month_year']<=filter_month[0]].drop(['month_year', 'label', 'date'], axis = 1)
        y_train =  df[df['month_year']<=filter_month[0]]['label']

        suite = unittest.TestSuite()
        suite.addTest(ParametrizedPredictionsTest.parametrize(PredictionsTest, X_train=X_train, y_train=y_train))
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        test_exit_code = int(not result.wasSuccessful())

        if test_exit_code == 1:
            raise Exception('El modelo no se creó correctamente')
        else:
            with self.output().open('w') as output_file:
                output_file.write(str(self.today)+","+str(self.year)+","+str(self.month))

    def output(self):
        output_path = "s3://{}/model_unittest/DATE={}/{}.csv". \
            format(self.bucket_metadata,str(self.year)+"-"+str(self.month),str(self.today))
        return luigi.contrib.s3.S3Target(path=output_path)

if __name__ == '__main__':
    luigi.run()
