import luigi
import luigi.contrib.s3
import boto3
import unittest
import pandas as pd
from io import StringIO
from datetime import date
from math import floor
from cleaned_ingest_metadata import cleaned_task_metadata
from train.ParametrizedLabelTest import ParametrizedLabelTest
from train.LabelTest import LabelTest

class label_unittest_task(luigi.Task):
    bucket_metadata = 'dpa-metro-metadata'
    today = date.today().strftime("%d%m%Y")
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    station = luigi.Parameter()

    def requires(self):
        return cleaned_task_metadata(self.year,self.month,self.station)

    def run(self):
        station = "station"
        year = "year"
        month = "month"
        date = "date"

        ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
        s3_resource = ses.resource('s3')

        obj = s3_resource.Object("dpa-metro-cleaned","year={}/month={}/station={}/{}.csv".format(str(self.year),str(self.month).zfill(2),self.station,self.station.replace(' ', '')))
        print(ses)

        file_content = obj.get()['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(file_content))

        df[year] = self.year
        df[month] = self.month
        df[station] = self.station

        n = floor(df.shape[0] * .7)
        df = df.sort_values(by = date, axis = 0)
        df_subset = df[0:n]

        suite = unittest.TestSuite()
        suite.addTest(ParametrizedLabelTest.parametrize(LabelTest, year=self.year, month=self.month,
                                                       station=self.station, cleaned_data=df_subset))
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        test_exit_code = int(not result.wasSuccessful())

        if test_exit_code == 1:
            raise Exception('La etiqueta tiene valores fuera de rango')
        else:
            with self.output().open('w') as output_file:
                output_file.write(str(self.today)+","+str(self.year)+","+str(self.month)+","+self.station)

    def output(self):
        output_path = "s3://{}/label_unittest/DATE={}/{}.csv". \
            format(self.bucket_metadata,str(self.today),str(self.today))
        return luigi.contrib.s3.S3Target(path=output_path)

if __name__ == '__main__':
    luigi.run()
