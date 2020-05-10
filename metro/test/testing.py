import luigi
import luigi.contrib.s3

class testing_task(luigi.Task):
    bucket = 'dpa-metro-metadata'
    year = luigi.IntParameter()
    month = luigi.IntParameter()
    station = luigi.Parameter()

    def run(self):
        with self.output().open('w') as output_file:
            output_file.write(str(self.year)+","+str(self.month)+","+self.station)

    def output(self):
        output_path = "s3://{}/year={}/month={}/station={}/{}.json". \
            format(self.bucket,str(self.year),str(self.month).zfill(2),self.station,self.station.replace(' ', ''))
        return luigi.contrib.s3.S3Target(path=output_path)

if __name__ == '__main__':
    luigi.run()
