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
        def test_upper_w_marbles():
            self.assertEqual('foo'.upper(), 'FOO', note=" the names should be uppercase because bla bla bla")

        def test_isupper_w_marbles():
            self.assertTrue('FOO'.isupper())
            self.assertFalse('Foo'.isupper())

        def test_split_w_marbles():
            s = 'hello world'
            self.assertEqual(s.split(), ['hello', 'world'])
            # check that s.split fails when the separator is not a string
            with self.assertRaises(TypeError):
                s.split(2)

        test_upper_w_marbles()
        test_isupper_w_marbles()
        test_split_w_marbles()

        with self.output().open('w') as output_file:
            output_file.write(str(self.today)+","+str(self.year)+","+str(self.month)+","+self.station)

    def output(self):
        output_path = "s3://{}/unittest_prueba/DATE={}/{}.csv".format(self.bucket_metadata,str(self.today),str(self.today))
        return luigi.contrib.s3.S3Target(path=output_path)

if __name__ == "__main__":
    luigi.run()

