import luigi
import luigi.contrib.s3
from datetime import date
from raw_ingest import raw_task

class raw_task_metadata(luigi.Task):
	bucket_metadata = 'dpa-metro-metadata'
	today = date.today().strftime("%d%m%Y")
	year = luigi.IntParameter()
	month = luigi.IntParameter()
	station = luigi.Parameter()

	def requires(self):
		return raw_task(self.year,self.month,self.station)

	def run(self):
		with self.output().open('w') as output_file:
			output_file.write(str(self.today)+","+str(self.year)+","+str(self.month)+","+self.station)

	def output(self):
		output_path = "s3://{}/raw/DATE={}/{}.csv".\
		format(self.bucket_metadata,str(self.today),str(self.today))
		return luigi.contrib.s3.S3Target(path=output_path)

if __name__ == '__main__':
	luigi.run()
