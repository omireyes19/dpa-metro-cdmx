import luigi
import luigi.contrib.s3
import boto3
import pandas as pd
from precleaned_ingest_metadata import precleaned_task_metadata
from metro.ingest.date_variables import date_variables
from io import StringIO

class cleaned_task(luigi.Task):
	bucket = 'dpa-metro-cleaned'
	year = luigi.IntParameter()
	month = luigi.IntParameter()
	station = luigi.Parameter()

	def requires(self):
		return precleaned_task_metadata(self.year,self.month,self.station)

	def run(self):
		ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
		s3_resource = ses.resource('s3')

		obj = s3_resource.Object("dpa-metro-precleaned","year={}/month={}/station={}/{}.csv".format(str(self.year),str(self.month).zfill(2),self.station,self.station.replace(' ', '')))
		print(ses)

		file_content = obj.get()['Body'].read().decode('utf-8')
		df = pd.read_csv(StringIO(file_content))

		df_date = date_variables.add_date_variables(df)

		with self.output().open('w') as output_file:
			df_date.to_csv(output_file)

	def output(self):
		output_path = "s3://{}/year={}/month={}/station={}/{}.csv".\
		format(self.bucket,str(self.year),str(self.month).zfill(2),self.station,self.station.replace(' ', ''))
		return luigi.contrib.s3.S3Target(path=output_path)

if __name__ == '__main__':
	luigi.run()
