import luigi
import luigi.contrib.s3
import boto3
import json
from precleaned_ingest_unittest import precleaned_unittest_task
from ingest.translation import translation

class precleaned_task(luigi.Task):
	bucket = 'dpa-metro-precleaned'
	year = luigi.IntParameter()
	month = luigi.IntParameter()

	def requires(self):
		return precleaned_unittest_task(self.year,self.month)

	def run(self):
		ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
		s3_resource = ses.resource('s3')

		obj = s3_resource.Object("dpa-metro-raw","year={}/month={}/{}.json".format(str(self.year), str(self.month).zfill(2), str(self.year)+str(self.month).zfill(2)))
		print(ses)

		file_content = obj.get()['Body'].read().decode('utf-8')
		json_content = json.loads(file_content)

		trans = translation()
		df = trans.get_dataframe(json_content)

		with self.output().open('w') as output_file:
			df.to_csv(output_file)

	def output(self):
		output_path = "s3://{}/year={}/month={}/{}.csv".\
		format(self.bucket,str(self.year), str(self.month).zfill(2), str(self.year)+str(self.month).zfill(2))
		return luigi.contrib.s3.S3Target(path=output_path)

if __name__ == '__main__':
	luigi.run()
