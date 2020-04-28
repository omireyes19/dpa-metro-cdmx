import requests
import luigi
import luigi.contrib.s3
import boto3
import s3fs
import glob
import os
from cleaned_ingest import cleaned_task
from io import StringIO
import pandas as pd
import numpy as np
from math import floor
from luigi.contrib.s3 import S3Target
from luigi.contrib.spark import SparkSubmitTask, PySparkTask
from pyspark.sql import SparkSession

class training_task(PySparkTask):
	bucket = 'dpa-metro-training'
	year = luigi.IntParameter()
	month = luigi.IntParameter()
	station = luigi.Parameter()

	def input(self):
		return cleaned_task(self.year,self.month,self.station)

	def main(self,sc):
		spark = SparkSession.builder.appName("Pysparkexample").config("spark.some.config.option", "some-value").getOrCreate()
		
		ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
		s3_resource = ses.resource('s3')

		obj = s3_resource.Object("dpa-metro-precleaned","year={}/month={}/station={}/{}.csv".format(str(self.year),str(self.month).zfill(2),self.station,self.station.replace(' ', '')))
		print(ses)

		file_content = obj.get()['Body'].read().decode('utf-8')
		data = spark.read.csv(StringIO(file_content),header="true")

	def output(self):
		output_path = "s3://{}/year={}/month={}/station={}/{}.csv".\
		format(self.bucket,str(self.year),str(self.month).zfill(2),self.station,self.station.replace(' ', ''))
		return luigi.contrib.s3.S3Target(path=output_path)

import sys
from pyspark import SparkContext

if __name__ == "__main__":
	sc = SparkContext()

