import luigi
import luigi.contrib.s3
import boto3
from training_unittest import training_unittest_task
from train.predictions import predictions
from io import StringIO
import pandas as pd
from luigi.contrib.spark import PySparkTask
from pyspark.sql import SparkSession
import pickle

class training_task(PySparkTask):
	bucket = 'dpa-metro-training'
	bucket_model = 'dpa-metro-model'
	year = luigi.IntParameter()
	month = luigi.IntParameter()
	station = luigi.Parameter()

	def requires(self):
		return training_unittest_task(self.year,self.month,self.station)

	def run(self):
		spark = SparkSession.builder.appName("Pyspark").getOrCreate()

		ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
		s3_resource = ses.resource('s3')

		obj = s3_resource.Object("dpa-metro-label","year={}/month={}/station={}/{}.csv".format(str(self.year),str(self.month).zfill(2),self.station,self.station.replace(' ', '')))
		print(ses)

		file_content = obj.get()['Body'].read().decode('utf-8')
		df = pd.read_csv(StringIO(file_content))

		df["year"] = self.year
		df["month"] = self.month

		predict = predictions()
		predictions_df = predict.get_predictions(spark, df)

		with self.output()["predictions"].open('w') as predictions_file:
			predictions_df.to_csv(predictions_file)

		with self.output()["model"].open('w') as model_file:
			pickle.dump(cvModel.bestModel, model_file)

	def output(self):
		output_path = "s3://{}/year={}/month={}/station={}/{}.csv".\
		format(self.bucket,str(self.year),str(self.month).zfill(2),self.station,self.station.replace(' ', ''))

		model_path = "s3://{}/year={}/month={}/station={}/{}.pkl".\
		format(self.bucket_model,str(self.year),str(self.month).zfill(2),self.station,self.station.replace(' ', ''))
		return {"predictions":luigi.contrib.s3.S3Target(path=output_path)
				,"model":luigi.contrib.s3.S3Target(path=model_path, format=luigi.format.Nop)
				}


if __name__ == "__main__":
	luigi.run()

