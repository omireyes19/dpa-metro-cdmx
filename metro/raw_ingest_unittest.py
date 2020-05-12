import luigi
import luigi.contrib.s3
import unittest
from datetime import date
from ingest.ParametrizedCallToAPITest import ParametrizedCallToAPITest
from ingest.CallToAPITest import CallToAPITest

class raw_unittest_task(luigi.Task):
    bucket_metadata = 'dpa-metro-metadata'
    today = date.today().strftime("%d%m%Y")
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    station = luigi.Parameter()

    def run(self):
        suite = unittest.TestSuite()
        suite.addTest(ParametrizedCallToAPITest.parametrize(CallToAPITest, year=self.year, month=self.month, station=self.station))
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        test_exit_code = int(not result.wasSuccessful())

        if test_exit_code == 1:
            print(Los datos que cargaste tienen longitud cero)
        else:
            with self.output().open('w') as output_file:
                output_file.write(str(self.today)+","+str(self.year)+","+str(self.month)+","+self.station)

    def output(self):
        output_path = "s3://{}/raw_unittest/DATE={}/{}.csv". \
            format(self.bucket_metadata,str(self.today),str(self.today))
        return luigi.contrib.s3.S3Target(path=output_path)

if __name__ == '__main__':
    luigi.run()
