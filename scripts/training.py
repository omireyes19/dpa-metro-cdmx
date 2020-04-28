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

class training_task(PySparkTask):
	bucket = 'dpa-metro-training'
	year = luigi.IntParameter()
	month = luigi.IntParameter()
	station = luigi.Parameter()

	def input(self):
		return {
			"data": S3Target("s3://dpa-metro-cleaned"),
			"task": cleaned_task(self.year,self.month,self.station)
		}

	def main(self,sc):
		sc.textFile(self.input().path).flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

		#with self.output().open('w') as output_file:
		#	df.to_csv(output_file)

	def output(self):
		output_path = "s3://{}/year={}/month={}/station={}/{}.csv".\
		format(self.bucket,str(self.year),str(self.month).zfill(2),self.station,self.station.replace(' ', ''))
		return luigi.contrib.s3.S3Target(path=output_path)

class cleaned_task_metadata(luigi.Task):
    bucket_metadata = 'dpa-metro-cleaned-metadata'
    today = date.today().strftime("%d%m%Y")
    year = luigi.Parameter()
    station = luigi.Parameter()

    def requires(self):
		return precleaned_task(self.year,self.month,self.station)

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

import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession

if __name__ == "__main__":
    sc = SparkContext()
    spark = SparkSession.builder.appName("Pysparkexample").config("spark.some.config.option", "some-value").getOrCreate()

