import luigi
from boto3 import Session
from pyspark.sql import SparkSession
from luigi.contrib.s3 import S3Target
from luigi.contrib.spark import SparkSubmitTask, PySparkTask


class prueba_task(PySparkTask):
    driver_memory = '2g'
    executor_memory = '3g'

    def input(self):
        return S3Target("s3://dpa-metro-label/year=2018/month=12/station=Chabacano/Chabacano.csv")

    def output(self):
        return S3Target("s3://dpa-metro-label/year=2018/month=12/station=Chabacano/Chabacano2.csv")

    def main(self, sc):
        session = Session()
        credentials = session.get_credentials()
        current_credentials = credentials.get_frozen_credentials()

        spark = SparkSession.builder()
          .master("local[1]")
          .appName("SparkByExamples.com")
          .getOrCreate()

        spark.sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", current_credentials.access_key)
        spark.sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", current_credentials.secret_key)
        spark.sc._jsc.hadoopConfiguration().set("fs.s3.session.token", current_credentials.token)
        spark.sc._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3.S3FileSystem")

        p = spark.read.csv(self.input().path)

        conteo = p.count()

        conteo.write.csv(self.output().path)


if __name__ == "__main__":
    sc = SparkContext()
    
