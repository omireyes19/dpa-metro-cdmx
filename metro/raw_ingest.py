import luigi
import luigi.contrib.s3
import json
from ingest.call_to_api import call_to_api

class raw_task(luigi.Task):
	bucket = 'dpa-metro-raw'
	year = luigi.IntParameter()
	month = luigi.IntParameter()
	station = luigi.Parameter()

	def run(self):

		cta = call_to_api()
		records = cta.get_information(self.year, self.month, self.station)

		with self.output().open('w') as output_file:
			json.dump(records, output_file)

	def output(self):
		output_path = "s3://{}/year={}/month={}/station={}/{}.json".\
		format(self.bucket,str(self.year),str(self.month).zfill(2),self.station,self.station.replace(' ', ''))
		return luigi.contrib.s3.S3Target(path=output_path)

if __name__ == '__main__':
	luigi.run()
