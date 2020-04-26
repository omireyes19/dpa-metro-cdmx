import requests
import luigi
import luigi.contrib.s3
import boto3
import s3fs
import glob
import os
import pandas as pd
from datetime import date
from raw_ingest import raw_task
from io import StringIO

class precleaned_task(luigi.Task):
	bucket = 'dpa-metro-cleaned'
	year = luigi.IntParameter()
	month = luigi.IntParameter()
	station = luigi.Parameter()

	def requires(self):
		return precleaned_task(self.year,self.month,self.station)

	def run(self):
		ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
		s3_resource = ses.resource('s3')

		obj = s3_resource.Object("dpa-metro-precleaned","information_year_month={}/station={}/{}.csv".format(str(self.year)+'-'+str(self.month).zfill(2),self.station,self.station.replace(' ', '')))
		print(ses)

		file_content = obj.get()['Body'].read().decode('utf-8')
		df = pd.read_csv(StringIO(file_content))

		with self.output().open('w') as output_file:
			df.to_csv(output_file)

	def output(self):
		output_path = "s3://{}/information_year_month={}/station={}/{}.csv".\
		format(self.bucket,str(self.year)+'-'+str(self.month).zfill(2),self.station,self.station.replace(' ', ''))
		return luigi.contrib.s3.S3Target(path=output_path)

class precleaned_task_metadata(luigi.Task):
    bucket_metadata = 'dpa-metro-cleaned-metadata'
    today = date.today().strftime("%d%m%Y")
    year = luigi.Parameter()
    station = luigi.Parameter()

    def run(self):
        ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
        s3_resource = ses.resource('s3')

        obj = s3_resource.Bucket(self.bucket_metadata)
        print(ses)

        with self.output_metadata().open('w') as output_file:
            output_file.write(str(self.today)+","+self.year+","+self.station)

    def output_metadata(self):
        output_path = "s3://{}/DATE={}/{}.csv".\
        format(self.bucket_metadata,str(self.today),str(self.today))
        return luigi.contrib.s3.S3Target(path=output_path)

if __name__ == '__main__':
    luigi.run()
