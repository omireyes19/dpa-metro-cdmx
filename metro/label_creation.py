import luigi
import luigi.contrib.s3
import boto3
from cleaned_ingest_metadata import cleaned_task_metadata
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
		return cleaned_task_metadata(self.year,self.month,self.station)

	def run(self):
		station = "station"
		year = "year"
		month = "month"
		date = "date"

		ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
		s3_resource = ses.resource('s3')

		obj = s3_resource.Object("dpa-metro-cleaned","year={}/month={}/station={}/{}.csv".format(str(self.year),str(self.month).zfill(2),self.station,self.station.replace(' ', '')))
		print(ses)

		file_content = obj.get()['Body'].read().decode('utf-8')
		df = pd.read_csv(StringIO(file_content))

		df[year] = self.year
		df[month] = self.month
		df[station] = self.station

		n = floor(df.shape[0] * .7)
		df = df.sort_values(by = date, axis = 0)
		df_subset = df[0:n]

		final = interquartile_range.create_label(interquartile_range.join_range(df_subset, interquartile_range.calculate_range(df_subset)))

		with self.output().open('w') as output_file:
			final.to_csv(output_file)

	def output(self):
		output_path = "s3://{}/year={}/month={}/station={}/{}.csv".\
		format(self.bucket,str(self.year),str(self.month).zfill(2),self.station,self.station.replace(' ', ''))
		return luigi.contrib.s3.S3Target(path=output_path)

if __name__ == "__main__":
	luigi.run()

