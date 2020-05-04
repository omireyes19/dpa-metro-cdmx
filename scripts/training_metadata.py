import requests
import luigi
import luigi.contrib.s3
import boto3
import s3fs
import glob
import os
from training import training_task
from io import StringIO
import pandas as pd
import numpy as np
from datetime import date
from math import floor
from luigi.contrib.s3 import S3Target
from luigi.contrib.spark import SparkSubmitTask, PySparkTask
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,monotonically_increasing_id
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.ml.tuning import CrossValidator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import OneHotEncoderEstimator
from pyspark.sql.types import IntegerType

class training_task_metadata(luigi.Task):
	bucket_metadata = 'dpa-metro-metadata'
	today = date.today().strftime("%d%m%Y")
	year = luigi.IntParameter()
	month = luigi.IntParameter()
	station = luigi.Parameter()

	def requires(self):
		return training_task(self.year,self.month,self.station)

	def run(self):
		ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
		s3_resource = ses.resource('s3')

		obj = s3_resource.Bucket(self.bucket_metadata)
		print(ses)

		with self.output().open('w') as output_file:
			output_file.write(str(self.today)+","+str(self.year)+","+str(self.month)+","+self.station)

	def output(self):
		output_path = "s3://{}/training/DATE={}/{}.csv".format(self.bucket_metadata,str(self.today),str(self.today))
		return luigi.contrib.s3.S3Target(path=output_path)

import sys
from pyspark import SparkContext

if __name__ == "__main__":
	sc = SparkContext()

