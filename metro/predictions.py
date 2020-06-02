import luigi
import luigi.contrib.s3
import boto3
from bias_fairness_metadata import bias_fairness_task_metadata
from datetime import date
import pickle
import pandas as pd
from io import StringIO
from io import BytesIO

class predictions_task(luigi.Task):
	bucket = 'dpa-metro-predictions'
	today = date.today().strftime("%d%m%Y")
	year = luigi.IntParameter()
	month = luigi.IntParameter()

	def requires(self):
		return bias_fairness_task_metadata(self.year,self.month)

	def run(self):
		ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
		s3_resource = ses.resource('s3')

		with BytesIO() as data:
			s3_resource.Bucket('dpa-metro-model').download_fileobj("year={}/month={}/{}.pkl".format(str(self.year), str(self.month).zfill(2), str(self.year)+str(self.month).zfill(2)), data)
			data.seek(0)
			model = pickle.load(data)

		obj = s3_resource.Object("dpa-metro-biasfairness", "year={}/month={}/{}.csv".format(str(self.year), str(self.month).zfill(2), str(self.year)+str(self.month).zfill(2)))

		file_content = obj.get()['Body'].read().decode('utf-8')
		df = pd.read_csv(StringIO(file_content))

		df = df[['date', 'day', 'month', 'station', 'line', 'day_of_week', 'holiday', 'label', 'label_predicted']]

		def label_rename(row):
			if row['label'] == 1:
				val = "Bajo"
			elif row['label'] == 2:
				val = "Medio"
			else:
				val = "Alto"
			return val

		def label_predicted_rename(row):
			if row['label'] == 1:
				val = "Bajo"
			elif row['label'] == 2:
				val = "Medio"
			else:
				val = "Alto"
			return val


		#X_test =  df.drop(['label', 'date'], axis = 1)

		#y_pred = model.predict(X_test)

		#df["label_predicted"] = y_pred

		df = df[['date', 'line', 'station', 'label', 'label_predicted']]

		df['label_c'] = df.apply(label_rename, axis=1)
		df['label_predicted_c'] = df.apply(label_predicted_rename, axis=1)

		with self.output().open('w') as output_file:
			df.to_csv(output_file)

	def output(self):
		output_path = "s3://{}/year={}/month={}/{}.csv". \
			format(self.bucket, str(self.year), str(self.month).zfill(2), str(self.year)+str(self.month).zfill(2))
		return luigi.contrib.s3.S3Target(path=output_path)

if __name__ == "__main__":
	luigi.run()

