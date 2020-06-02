import luigi
import luigi.contrib.s3
import boto3
from math import floor
from training_unittest import training_unittest_task
from train.predictions import predictions
from io import StringIO
import pandas as pd

class training_task(luigi.Task):
	bucket = 'dpa-metro-training'
	bucket_model = 'dpa-metro-model'
	year = luigi.IntParameter()
	month = luigi.IntParameter()

	def requires(self):
		return training_unittest_task(self.year, self.month)

	def run(self):
		def months_of_history(year, month):
			day = monthrange(year, month)[1]
			d1 = datetime(year, month, day)
			return (d1.year - 2018) * 12 + d1.month

		cut_date = floor(months_of_history(self.year, self.month) * .7)

		ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
		s3_resource = ses.resource('s3')

		df = pd.DataFrame()
		for i in range(cut_date):
			reference_date = datetime(2010, 1, 1) + relativedelta(months=i)
			obj = s3_resource.Object("dpa-metro-label", "year={}/month={}/{}.csv".format(str(reference_date.year), str(reference_date.month).zfill(2), str(reference_date.year)+str(reference_date.month).zfill(2)))

			file_content = obj.get()['Body'].read().decode('utf-8')
			aux = pd.read_csv(StringIO(file_content))
			aux = aux[['date', 'day', 'month', 'station', 'line', 'day_of_week', 'holiday', 'label']]

			df = pd.concat([df, aux])

		X_train = df.drop(['month_year', 'label', 'date'], axis = 1)
		y_train = df['label']

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

