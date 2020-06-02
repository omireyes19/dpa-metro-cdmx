import luigi
import luigi.contrib.s3
import boto3
import unittest
import pandas as pd
from io import StringIO
from datetime import date
from precleaned_ingest_metadata import precleaned_task_metadata
from ingest.ParametrizedDateTest import ParametrizedDateTest
from ingest.DateTest import DateTest

class cleaned_unittest_task(luigi.Task):
    bucket_metadata = 'dpa-metro-metadata'
    today = date.today().strftime("%d%m%Y")
    year = luigi.IntParameter()
    month = luigi.IntParameter()

    def requires(self):
        return precleaned_task_metadata(self.year,self.month)

    def run(self):
        ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
        s3_resource = ses.resource('s3')

        obj = s3_resource.Object("dpa-metro-precleaned","year={}/month={}/{}.csv".format(str(self.year),str(self.month).zfill(2),str(self.year)+str(self.month).zfill(2)))
        print(ses)

        file_content = obj.get()['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(file_content))

        suite = unittest.TestSuite()
        suite.addTest(ParametrizedDateTest.parametrize(DateTest, year=self.year, month=self.month, csv_data=df))
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        test_exit_code = int(not result.wasSuccessful())

        if test_exit_code == 1:
            raise Exception('No se han creado las columnas correctamente')
        else:
            with self.output().open('w') as output_file:
                output_file.write(str(self.today)+","+str(self.year)+","+str(self.month))

    def output(self):
        output_path = "s3://{}/cleaned_unittest/DATE={}/{}.csv". \
            format(self.bucket_metadata,str(self.year)+"-"+str(self.month),str(self.today))
        return luigi.contrib.s3.S3Target(path=output_path)

if __name__ == '__main__':
    luigi.run()
