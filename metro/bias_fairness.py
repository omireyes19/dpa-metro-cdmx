import luigi
import luigi.contrib.s3
import boto3
from training_metadata import training_task_metadata
from datetime import date
import pickle
import pandas as pd
from io import StringIO
from io import BytesIO

class bias_fairness_task(luigi.Task):
	bucket = 'dpa-metro-biasfairness'
	today = date.today().strftime("%d%m%Y")
	year = luigi.IntParameter()
	month = luigi.IntParameter()

	def requires(self):
		return training_task_metadata(self.year,self.month)

	def run(self):
		ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
		s3_resource = ses.resource('s3')

		with BytesIO() as data:
			s3_resource.Bucket('dpa-metro-model').download_fileobj("year={}/month={}/{}.pkl".format(str(self.year), str(self.month).zfill(2), str(self.year)+str(self.month).zfill(2)), data)
			data.seek(0)
			model = pickle.load(data)

		obj = s3_resource.Object("dpa-metro-label", "year={}/month={}/{}.csv".format(str(self.year), str(self.month).zfill(2), str(self.year)+str(self.month).zfill(2)))

		file_content = obj.get()['Body'].read().decode('utf-8')
		df = pd.read_csv(StringIO(file_content))

		df = df[['date', 'day', 'month', 'station', 'line', 'day_of_week', 'holiday', 'label']]

		X_test =  df.drop(['label', 'date'], axis = 1)

		y_pred = model.predict(X_test)

		df["label_predicted"] = y_pred

		with self.output().open('w') as output_file:
			df.to_csv(output_file)

	def output(self):
		output_path = "s3://{}/year={}/month={}/{}.pkl". \
			format(self.bucket, str(self.year), str(self.month).zfill(2), str(self.year)+str(self.month).zfill(2))
		return luigi.contrib.s3.S3Target(path=output_path, format=luigi.format.Nop)

if __name__ == "__main__":
	luigi.run()

