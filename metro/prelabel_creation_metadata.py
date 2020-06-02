import luigi
import luigi.contrib.s3
from datetime import date
from prelabel_creation import prelabel_task

class prelabel_task_metadata(luigi.Task):
	bucket_metadata = 'dpa-metro-metadata'
	today = date.today().strftime("%d%m%Y")
	year = luigi.IntParameter()
	month = luigi.IntParameter()

	def requires(self):
		return prelabel_task(self.year,self.month)

	def run(self):
		with self.output().open('w') as output_file:
			output_file.write(str(self.today)+","+str(self.year)+","+str(self.month))

	def output(self):
		output_path = "s3://{}/prelabel/DATE={}/{}.csv".format(self.bucket_metadata,str(self.year)+"-"+str(self.month),str(self.today))
		return luigi.contrib.s3.S3Target(path=output_path)

if __name__ == "__main__":
	luigi.run()

