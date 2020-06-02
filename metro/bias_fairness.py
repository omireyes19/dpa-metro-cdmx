import luigi
import luigi.contrib.s3
import boto3
from training_metadata import training_task_metadata
from datetime import date
import pickle
from io import StringIO
from io import BytesIO

class bias_fairness_task(luigi.Task):
	bucket_metadata = 'dpa-metro-biasfairness'
	today = date.today().strftime("%d%m%Y")
	year = luigi.IntParameter()
	month = luigi.IntParameter()

	def requires(self):
		return training_task_metadata(self.year,self.month)

	def run(self):
		ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
		s3_resource = ses.resource('s3')

		obj = s3_resource.Object("dpa-metro-model", "year={}/month={}/{}.csv".format(str(reference_date.year), str(reference_date.month).zfill(2), str(reference_date.year)+str(reference_date.month).zfill(2)))
		file_content = obj.get()
		model = pickle.load(file_content)


		#with BytesIO() as data:
		#	s3_resource.Bucket('dpa-metro-model').download_fileobj("year={}/month={}/{}.csv".format(str(self.year), str(self.month).zfill(2), str(self.year)+str(self.month).zfill(2)), data)
		#	data.seek(0)
		#	model = pickle.load(data)

		obj = s3_resource.Object("dpa-metro-label", "year={}/month={}/{}.csv".format(str(self.year), str(self.month).zfill(2), str(self.year)+str(self.month).zfill(2)))

		file_content = obj.get()['Body'].read().decode('utf-8')
		df = pd.read_csv(StringIO(file_content))

		df = df[['date', 'day', 'month', 'station', 'line', 'day_of_week', 'holiday', 'label']]

		df['month_year'] = pd.DatetimeIndex(df['date']).to_period('M')
		dif_months = df['month_year'].unique()
		num_dif_months = len(dif_months)
		n = floor(num_dif_months * 0.7)
		filter_month = dif_months[[n]]
		X_test =  df[df['month_year']>filter_month[0]].drop(['month_year', 'label', 'date'], axis = 1)
		y_test =  df[df['month_year']>filter_month[0]]['label']

		y_pred = model.predict(X_test)

		with self.output().open('w') as output_file:
			y_pred.to_csv(output_file)

	def output(self):
		output_path = "s3://{}/training/DATE={}/{}.csv".format(self.bucket_metadata,str(self.year)+"-"+str(self.month),str(self.today))
		return luigi.contrib.s3.S3Target(path=output_path)

if __name__ == "__main__":
	luigi.run()

