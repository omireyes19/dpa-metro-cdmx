import luigi
import luigi.contrib.s3
import boto3
from datetime import date, timedelta, datetime
import numpy as np
from math import floor
from datetime import datetime
from dateutil.relativedelta import *
from calendar import monthrange
from train.interquartile_range import interquartile_range
from bias_fairness_metadata import bias_fairness_task_metadata
from datetime import date
from ingest.date_variables import date_variables
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
		line = "line"
		station = "station"

		def months_of_history(year, month):
			day = monthrange(year, month)[1]
			d1 = datetime(year, month, day)
			return (d1.year - 2019) * 12 + d1.month

		def first_day_of_month(month, year):
			fdom = '01-' + str(month).zfill(2) + '-' + str(year)
			return fdom

		def get_stations_and_lines(df):
			df_fdom = df[[line, station]]
			df_fdom.columns = ['line', 'station']
			return df_fdom.drop_duplicates()

		def days_of_next_month(fdom):
			days_in_month = lambda dt: monthrange(dt.year, dt.month)[1]
			last_month =  datetime.strptime(fdom, "%d-%m-%Y").date()
			first_day = last_month + timedelta(days_in_month(last_month))
			next_month = [first_day + timedelta(days=x) for x in range(days_in_month(first_day))]
			return next_month

		def create_df(fdom, df):
			num_line_station = len(df)
			days = days_of_next_month(fdom)
			data = {'date': np.repeat(days, num_line_station)}
			next_month_df = pd.DataFrame(data, columns=['date'])
			next_month_df['day'] = pd.DatetimeIndex(next_month_df['date']).day
			next_month_df['month'] = pd.DatetimeIndex(next_month_df['date']).month
			next_month_df['label'] = "NA"
			lines = pd.concat([df]*len(days)).reset_index()
			final_df = next_month_df.join(lines).drop('index', axis = 1)
			return final_df

		cut_date = floor(months_of_history(self.year, self.month) * 1)

		ses = boto3.session.Session(profile_name='omar', region_name='us-east-1')
		s3_resource = ses.resource('s3')

		df = pd.DataFrame()
		for i in range(cut_date):
			reference_date = datetime(2019, 1, 1) + relativedelta(months=i)
			obj = s3_resource.Object("dpa-metro-cleaned", "year={}/month={}/{}.csv".format(str(reference_date.year), str(reference_date.month).zfill(2), str(reference_date.year)+str(reference_date.month).zfill(2)))

			file_content = obj.get()['Body'].read().decode('utf-8')
			aux = pd.read_csv(StringIO(file_content))

			df = pd.concat([df, aux])

		with BytesIO() as data:
			s3_resource.Bucket('dpa-metro-model').download_fileobj("year={}/month={}/{}.pkl".format(str(self.year), str(self.month).zfill(2), str(self.year)+str(self.month).zfill(2)), data)
			data.seek(0)
			model = pickle.load(data)

		obj = s3_resource.Object("dpa-metro-prelabel", "year={}/month={}/{}.csv".format(str(self.year), str(self.month).zfill(2), str(self.year)+str(self.month).zfill(2)))
		file_content = obj.get()['Body'].read().decode('utf-8')
		labels = pd.read_csv(StringIO(file_content))

		intquar_ran = interquartile_range()
		final = intquar_ran.create_label(intquar_ran.join_range(df, labels))

		df = final[['date', 'day', 'month', 'station', 'line', 'day_of_week', 'holiday', 'label']]

		fdom = first_day_of_month(self.month, self.year)
		line_station_df = get_stations_and_lines(df)

		new_month_df = create_df(fdom, line_station_df)

		date_var = date_variables()
		df_date = date_var.add_date_variables(new_month_df)

		df_date = df_date[['date', 'day', 'month', 'station', 'line', 'day_of_week', 'holiday', 'label']]
		final = pd.concat([df, df_date])

		X_test =  final.drop(['label', 'date'], axis = 1)

		y_pred = model.predict(X_test)

		final["label_predicted"] = y_pred

		df = final[['date', 'day', 'month', 'station', 'line', 'day_of_week', 'holiday', 'label', 'label_predicted']]

		def label_rename(row):
			if row['label'] == 1:
				val = "Bajo"
			elif row['label'] == 2:
				val = "Medio"
			elif row['label'] == 3:
				val = "Alto"
			else:
				val = "NA"
			return val

		def label_predicted_rename(row):
			if row['label_predicted'] == 1:
				val = "Bajo"
			elif row['label_predicted'] == 2:
				val = "Medio"
			elif row['label_predicted'] == 3:
				val = "Alto"
			else:
				val = "NA"
			return val

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

