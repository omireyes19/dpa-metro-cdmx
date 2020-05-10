import luigi
import luigi.contrib.s3
from ex1 import TestMarbles
from datetime import date

class testing_task(luigi.Task):
    bucket_metadata = 'dpa-metro-metadata'
    today = date.today().strftime("%d%m%Y")
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    station = luigi.Parameter()

    def run(self):
        TestMarbles.test_upper_w_marbles()
        TestMarbles.test_isupper_w_marbles()
        TestMarbles.test_split_w_marbles()

        with self.output().open('w') as output_file:
            output_file.write(str(self.today)+","+str(self.year)+","+str(self.month)+","+self.station)

    def output(self):
        output_path = "s3://{}/unittest_prueba/DATE={}/{}.csv".format(self.bucket_metadata,str(self.today),str(self.today))
        return luigi.contrib.s3.S3Target(path=output_path)

if __name__ == "__main__":
    luigi.run()

