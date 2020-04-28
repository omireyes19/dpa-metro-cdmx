import requests
import luigi
import luigi.contrib.s3
import boto3
import s3fs
import glob
import os
from label import label_task
from io import StringIO
import pandas as pd
import numpy as np
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

class training_task(PySparkTask):
	bucket = 'dpa-metro-training'
	year = luigi.IntParameter()
	month = luigi.IntParameter()
	station = luigi.Parameter()

	def input(self):
		return label_task(self.year,self.month,self.station)

	def main(self,sc):
		spark = SparkSession.builder.appName("Pysparkexample").config("spark.some.config.option", "some-value").getOrCreate()

		ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
		s3_resource = ses.resource('s3')

		obj = s3_resource.Object("dpa-metro-label","year={}/month={}/station={}/{}.csv".format(str(self.year),str(self.month).zfill(2),self.station,self.station.replace(' ', '')))
		print(ses)

		file_content = obj.get()['Body'].read().decode('utf-8')
		df = pd.read_csv(StringIO(file_content))

		df["year"] = self.year
		df["month"] = self.month

		data = spark.createDataFrame(df)

		n = data.count()

		data = data.withColumn('line_crossing', col('line_crossing').cast(IntegerType()))
		data = data.withColumnRenamed('label', 'label_prev')

		cut_date = floor(n * .7)

		categoricalColumns = ['day_of_week', 'line']
		stages = []
		for categoricalCol in categoricalColumns:
			stringIndexer = StringIndexer(inputCol=categoricalCol, outputCol=categoricalCol + "Index")
			encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
			stages += [stringIndexer, encoder]

		label_stringIdx = StringIndexer(inputCol="label_prev", outputCol="label")
		stages += [label_stringIdx]

		numericCols = ["year", "month", "line_crossing"]
		assemblerInputs = [c + "classVec" for c in categoricalColumns] + numericCols
		assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
		stages += [assembler]

		partialPipeline = Pipeline().setStages(stages)
		pipelineModel = partialPipeline.fit(data)
		preppedDataDF = pipelineModel.transform(data)

		id_data = preppedDataDF.withColumn('id', monotonically_increasing_id())
		trainingData = id_data.filter(col('id') < cut_date).drop('id')
		testingData = id_data.filter(col('id') > cut_date).drop('id')

		rf = RandomForestClassifier(labelCol="label", featuresCol="features")

		paramGrid = ParamGridBuilder() \
			.addGrid(rf.numTrees, [int(x) for x in np.linspace(start = 10, stop = 50, num = 3)]) \
			.addGrid(rf.maxDepth, [int(x) for x in np.linspace(start = 5, stop = 25, num = 3)]) \
			.build()

		crossval = CrossValidator(estimator=rf,
                          estimatorParamMaps=paramGrid,
                          evaluator=MulticlassClassificationEvaluator(),
                          numFolds=3)

		cvModel = crossval.fit(trainingData)

		predictions = cvModel.transform(testingData)

		with self.output().open('w') as output_file:
			predictions.to_csv(output_file)

	def output(self):
		output_path = "s3://{}/year={}/month={}/station={}/{}.csv".\
		format(self.bucket,str(self.year),str(self.month).zfill(2),self.station,self.station.replace(' ', ''))
		return luigi.contrib.s3.S3Target(path=output_path)

import sys
from pyspark import SparkContext

if __name__ == "__main__":
	sc = SparkContext()

