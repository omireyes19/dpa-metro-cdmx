import requests
import luigi
import luigi.contrib.s3
import boto3
import s3fs
import json
import glob
import os
import pandas as pd
from datetime import date
from precleaned_ingest import precleaned_task
from calendar import monthrange

class precleaned_task_metadata(luigi.Task):
	bucket_metadata = 'dpa-metro-metadata'
	today = date.today().strftime("%d%m%Y")
	year = luigi.IntParameter()
	month = luigi.IntParameter()
	station = luigi.Parameter()

	def requires(self):
		return precleaned_task(self.year,self.month,self.station)

	def run(self):
		ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
		s3_resource = ses.resource('s3')

		obj = s3_resource.Bucket(self.bucket_metadata)
		print(ses)

		with self.output().open('w') as output_file:
			output_file.write(str(self.today)+","+str(self.year)+","+str(self.month)+","+self.station)

	def output(self):
		output_path = "s3://{}/precleaned/DATE={}/{}.csv".\
		format(self.bucket_metadata,str(self.today),str(self.today))
		return luigi.contrib.s3.S3Target(path=output_path)

if __name__ == '__main__':
	luigi.run()