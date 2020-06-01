import luigi
import luigi.contrib.s3
import boto3
from label_creation_unittest import label_unittest_task
from io import StringIO
import pandas as pd
from math import floor
from train.interquartile_range import interquartile_range

class label_task(luigi.Task):
	bucket = 'dpa-metro-label'
	year = luigi.IntParameter()
	month = luigi.IntParameter()
	station = luigi.Parameter()

	def requires(self):
		return label_unittest_task(self.year,self.month,self.station)

	def run(self):
		date = "date"

		ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
		s3_resource = ses.resource('s3')

		obj = s3_resource.Object("dpa-metro-cleaned","year={}/month={}/station={}/{}.csv".format(str(self.year),str(self.month).zfill(2),self.station,self.station.replace(' ', '')))
		print(ses)

		file_content = obj.get()['Body'].read().decode('utf-8')
		df = pd.read_csv(StringIO(file_content))

		df['month_year'] = pd.DatetimeIndex(df[date]).to_period('M')
		dif_months = df['month_year'].unique()
		num_dif_months = len(dif_months)
		n = floor(num_dif_months * 0.7)
		filter_month = dif_months[[n]]
		df = df[df['month_year']<=filter_month[0]].drop('month_year', axis = 1)

		intquar_ran = interquartile_range()
		final = intquar_ran.create_label(intquar_ran.join_range(df_subset, intquar_ran.calculate_range(df_subset)))

		with self.output().open('w') as output_file:
			final.to_csv(output_file)

	def output(self):
		output_path = "s3://{}/year={}/month={}/station={}/{}.csv".\
		format(self.bucket,str(self.year),str(self.month).zfill(2),self.station,self.station.replace(' ', ''))
		return luigi.contrib.s3.S3Target(path=output_path)

if __name__ == "__main__":
	luigi.run()

