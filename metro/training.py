import luigi
import luigi.contrib.s3
import boto3
from math import floor
from datetime import datetime
from dateutil.relativedelta import *
from calendar import monthrange
from training_unittest import training_unittest_task
from train.predictions import predictions
from io import StringIO
import pandas as pd

class training_task(luigi.Task):
	bucket_model = 'dpa-metro-model'
	year = luigi.IntParameter()
	month = luigi.IntParameter()

	def requires(self):
		return training_unittest_task(self.year, self.month)

	def run(self):
		ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
		s3_resource = ses.resource('s3')

		obj = s3_resource.Object("dpa-metro-label", "year={}/month={}/{}.csv".format(str(self.year), str(self.month).zfill(2), str(self.year)+str(self.month).zfill(2)))

		file_content = obj.get()['Body'].read().decode('utf-8')
		df = pd.read_csv(StringIO(file_content))

		df = df[['date', 'day', 'month', 'station', 'line', 'day_of_week', 'holiday', 'label']]

		df['month_year'] = pd.DatetimeIndex(df['date']).to_period('M')
		dif_months = df['month_year'].unique()
		num_dif_months = len(dif_months)
		n = floor(num_dif_months * 0.7)
		filter_month = dif_months[[n]]
		X_train = df[df['month_year']<=filter_month[0]].drop(['month_year', 'label', 'date'], axis = 1)
		y_train =  df[df['month_year']<=filter_month[0]]['label']

		pred = predictions()
		final = pred.get_predictions(X_train, y_train)

		with self.output().open('wb') as model_file:
			pickle.dump(final, model_file, format=luigi.format.Nop)

	def output(self):
		model_path = "s3://{}/year={}/month={}/{}.pkl".\
		format(self.bucket_model, str(self.year), str(self.month).zfill(2), str(self.year)+str(self.month).zfill(2))
		return luigi.contrib.s3.S3Target(path=model_path)


if __name__ == "__main__":
	luigi.run()

