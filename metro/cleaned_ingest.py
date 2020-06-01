import luigi
import luigi.contrib.s3
import boto3
import pandas as pd
from cleaned_ingest_unittest import cleaned_unittest_task
from ingest.date_variables import date_variables
from io import StringIO

class cleaned_task(luigi.Task):
	bucket = 'dpa-metro-cleaned'
	year = luigi.IntParameter()
	month = luigi.IntParameter()

	def requires(self):
		return cleaned_unittest_task(self.year,self.month)

	def run(self):
		ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
		s3_resource = ses.resource('s3')

		obj = s3_resource.Object("dpa-metro-precleaned","year={}/month={}/{}.csv".format(str(self.year),str(self.month).zfill(2),str(self.year)+str(self.month).zfill(2)))
		print(ses)

		file_content = obj.get()['Body'].read().decode('utf-8')
		df = pd.read_csv(StringIO(file_content))

		date_var = date_variables()
		df_date = date_var.add_date_variables(df)

		with self.output().open('w') as output_file:
			df_date.to_csv(output_file)

	def output(self):
		output_path = "s3://{}/year={}/month={}/{}.csv".\
		format(self.bucket,str(self.year),str(self.month).zfill(2),str(self.year)+str(self.month).zfill(2))
		return luigi.contrib.s3.S3Target(path=output_path)

if __name__ == '__main__':
	luigi.run()
