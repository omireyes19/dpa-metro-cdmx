import luigi
import luigi.contrib.s3
import boto3
import json
from raw_ingest_metadata import raw_task_metadata
from ingest.translation import translation

class precleaned_task(luigi.Task):
	bucket = 'dpa-metro-precleaned'
	year = luigi.IntParameter()
	month = luigi.IntParameter()
	station = luigi.Parameter()

	def requires(self):
		return raw_task_metadata(self.year,self.month,self.station)

	def run(self):
		ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
		s3_resource = ses.resource('s3')

		obj = s3_resource.Object("dpa-metro-raw","year={}/month={}/station={}/{}.json"\
								 .format(str(self.year),str(self.month).zfill(2),self.station,self.station.replace(' ', '')))
		print(ses)

		file_content = obj.get()['Body'].read().decode('utf-8')
		json_content = json.loads(file_content)

		df = translation.get_dataframe(json_content)

		with self.output().open('w') as output_file:
			df.to_csv(output_file)

	def output(self):
		output_path = "s3://{}/year={}/month={}/station={}/{}.csv".\
		format(self.bucket,str(self.year),str(self.month).zfill(2),self.station,self.station.replace(' ', ''))
		return luigi.contrib.s3.S3Target(path=output_path)

if __name__ == '__main__':
	luigi.run()
