import luigi
import luigi.contrib.s3
import boto3
import unittest
import pandas as pd
from io import StringIO
from datetime import date
from label_creation_metadata import label_task_metadata
from train.ParametrizedPredictionsTest import ParametrizedPredictionsTest
from train.PredictionsTest import PredictionsTest
from luigi.contrib.spark import PySparkTask
from pyspark.sql import SparkSession

class training_unittest_task(PySparkTask):
    bucket_metadata = 'dpa-metro-metadata'
    today = date.today().strftime("%d%m%Y")
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    station = luigi.Parameter()

    def requires(self):
        return label_task_metadata(self.year,self.month,self.station)

    def run(self):
        spark = SparkSession.builder.appName("Pyspark").getOrCreate()

        ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
        s3_resource = ses.resource('s3')

        obj = s3_resource.Object("dpa-metro-label","year={}/month={}/station={}/{}.csv".format(str(self.year),str(self.month).zfill(2),self.station,self.station.replace(' ', '')))
        print(ses)

        file_content = obj.get()['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(file_content))

        df["year"] = self.year
        df["month"] = self.month

        suite = unittest.TestSuite()
        suite.addTest(ParametrizedPredictionsTest.parametrize(PredictionsTest, year=self.year, month=self.month,
                                                       station=self.station, spark=spark,  model_data = df))
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        test_exit_code = int(not result.wasSuccessful())

        if test_exit_code == 1:
            raise Exception('La columna de predicciones no se creo correctamente')
        else:
            with self.output().open('w') as output_file:
                output_file.write(str(self.today)+","+str(self.year)+","+str(self.month)+","+self.station)

    def output(self):
        output_path = "s3://{}/model_unittest/DATE={}/{}.csv". \
            format(self.bucket_metadata,str(self.year)+"-"+str(self.month),str(self.today))
        return luigi.contrib.s3.S3Target(path=output_path)

if __name__ == '__main__':
    luigi.run()
