import luigi
import luigi.contrib.s3
import boto3
import unittest
from datetime import date
from raw_ingest_metadata import raw_task_metadata
from ingest.ParametrizedTranslationTest import ParametrizedTranslationTest
from ingest.TranslationTest import TranslationTest
import json

class precleaned_unittest_task(luigi.Task):
    bucket_metadata = 'dpa-metro-metadata'
    today = date.today().strftime("%d%m%Y")
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    station = luigi.Parameter()

    def requires(self):
        return raw_task_metadata(self.year, self.month, self.station)

    def run(self):
        ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
        s3_resource = ses.resource('s3')

        obj = s3_resource.Object("dpa-metro-raw","year={}/month={}/station={}/{}.json" \
                                 .format(str(self.year),str(self.month).zfill(2),self.station,self.station.replace(' ', '')))
        print(ses)

        file_content = obj.get()['Body'].read().decode('utf-8')
        json_content = json.loads(file_content)

        suite = unittest.TestSuite()
        suite.addTest(ParametrizedTranslationTest.parametrize(TranslationTest, year=self.year, month=self.month,
                                                              station=self.station, raw_json=json_content))
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        test_exit_code = int(not result.wasSuccessful())

        if test_exit_code == 1:
            raise Exception('El layout es distinto del que se espera')
        else:
            with self.output().open('w') as output_file:
                output_file.write(str(self.today)+","+str(self.year)+","+str(self.month)+","+self.station)

    def output(self):
        output_path = "s3://{}/precleaned_unittest/DATE={}/{}.csv". \
            format(self.bucket_metadata,str(self.today),str(self.today))
        return luigi.contrib.s3.S3Target(path=output_path)

if __name__ == '__main__':
    luigi.run()
