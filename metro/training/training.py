import luigi
import luigi.contrib.s3
import boto3
from metro.metadata.label_creation_metadata import label_task_metadata
from metro.training.predictions import predictions
from io import StringIO
import pandas as pd
from luigi.contrib.spark import SparkSubmitTask, PySparkTask
from pyspark.sql import SparkSession
import pickle

class training_task(PySparkTask):
	bucket = 'dpa-metro-training'
	bucket_model = 'dpa-metro-model'
	year = luigi.IntParameter()
	month = luigi.IntParameter()
	station = luigi.Parameter()

	def requires(self):
		return label_task_metadata(self.year,self.month,self.station)

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

		predictions_df = predictions.get_predictions(spark,df)

		with self.output()["predictions"].open('w') as predictions_file:
			predictions_df.to_csv(predictions_file)

		#pickle_byte_obj = pickle.dumps(cvModel.bestModel)

		#key = "year={}/month={}/station={}/{}.pkl".\
		#format(str(self.year),str(self.month).zfill(2),self.station,self.station.replace(' ', ''))
		#s3_resource.Object(self.bucket_model,key).put(Body=pickle_byte_obj)

		#with self.output()["model"].open('wb') as model_file:
		#	pickle.dump(cvModel.bestModel, model_file)

	def output(self):
		output_path = "s3://{}/year={}/month={}/station={}/{}.csv".\
		format(self.bucket,str(self.year),str(self.month).zfill(2),self.station,self.station.replace(' ', ''))

		model_path = "s3://{}/year={}/month={}/station={}/{}.pkl".\
		format(self.bucket_model,str(self.year),str(self.month).zfill(2),self.station,self.station.replace(' ', ''))
		return {"predictions":luigi.contrib.s3.S3Target(path=output_path)
				,"model":luigi.contrib.s3.S3Target(path=model_path)
				}


if __name__ == "__main__":
	luigi.run()

