import luigi
import luigi.contrib.s3
import boto3
import json
import unittest
from ParametrizedTestCase import ParametrizedTestCase
from TestOne import TestOne

class precleaned_task(luigi.Task):
	bucket = 'dpa-metro-precleaned'
	year = luigi.IntParameter()
	month = luigi.IntParameter()
	station = luigi.Parameter()

	def run(self):
		ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
		s3_resource = ses.resource('s3')

		obj = s3_resource.Object("dpa-metro-raw","year={}/month={}/station={}/{}.json"\
								 .format(str(self.year),str(self.month).zfill(2),self.station,self.station.replace(' ', '')))
		print(ses)

		file_content = obj.get()['Body'].read().decode('utf-8')
		json_content = json.loads(file_content)

		suite = unittest.TestSuite()
		suite.addTest(ParametrizedTestCase.parametrize(TestOne, year=self.year, month=self.month, station=self.station, raw_json=json_content))
		unittest.TextTestRunner(verbosity=2).run(suite)

		with self.output().open('w') as output_file:
			output_file.write("Exito")

	def output(self):
		output_path = "s3://{}/year={}/month={}/station={}/{}.csv".\
		format(self.bucket,str(self.year),str(self.month).zfill(2),self.station,self.station.replace(' ', ''))
		return luigi.contrib.s3.S3Target(path=output_path)

if __name__ == '__main__':
	luigi.run()
