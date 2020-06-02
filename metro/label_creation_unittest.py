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
from datetime import datetime
from dateutil.relativedelta import *
from calendar import monthrange
from train.interquartile_range import interquartile_range

class label_unittest_task(luigi.Task):
    bucket_metadata = 'dpa-metro-metadata'
    today = date.today().strftime("%d%m%Y")
    year = luigi.IntParameter()
    month = luigi.IntParameter()

    def requires(self):
        return cleaned_task_metadata(self.year,self.month)

    def run(self):
        def months_of_history(year, month):
            day = monthrange(year, month)[1]
            d1 = datetime(year, month, day)
            return (d1.year - 2018) * 12 + d1.month

        cut_date = floor(months_of_history(self.year, self.month) * .7)

        ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
        s3_resource = ses.resource('s3')

        df = pd.DataFrame()
        for i in range(cut_date):
            reference_date = datetime(2010, 1, 1) + relativedelta(months=i)

            obj = s3_resource.Object("dpa-metro-cleaned", "year={}/month={}/{}.csv".format(str(reference_date.year), str(reference_date.month).zfill(2), str(reference_date.year)+str(reference_date.month).zfill(2)))

            file_content = obj.get()['Body'].read().decode('utf-8')
            aux = pd.read_csv(StringIO(file_content))

            df = pd.concat([df, aux])

        intquar_ran = interquartile_range()

        print(intquar_ran.calculate_range(df).columns)
        final = intquar_ran.create_label(intquar_ran.join_range(df, intquar_ran.calculate_range(df)))

        suite = unittest.TestSuite()
        suite.addTest(ParametrizedLabelTest.parametrize(LabelTest, year=self.year, month=self.month, cleaned_data=final))

        result = unittest.TextTestRunner(verbosity=2).run(suite)
        test_exit_code = int(not result.wasSuccessful())

        if test_exit_code == 1:
            raise Exception('La etiqueta tiene valores fuera de rango')
        else:
            with self.output().open('w') as output_file:
                output_file.write(str(self.today)+","+str(self.year)+","+str(self.month))

    def output(self):
        output_path = "s3://{}/label_unittest/DATE={}/{}.csv". \
            format(self.bucket_metadata,str(self.year)+"-"+str(self.month),str(self.today))
        return luigi.contrib.s3.S3Target(path=output_path)

if __name__ == '__main__':
    luigi.run()
