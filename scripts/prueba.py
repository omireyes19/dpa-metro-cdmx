import luigi
from boto3 import Session
from pyspark.sql import SparkSession
from luigi.contrib.s3 import S3Target
from luigi.contrib.spark import SparkSubmitTask, PySparkTask


class prueba_task(PySparkTask):
    driver_memory = '2g'
    executor_memory = '3g'

    def input(self):
        return S3Target("s3a://dpa-metro-label/year=2020/month=01/station=Chabacano/Chabacano.csv")

    def output(self):
        return S3Target("s3a://dpa-metro-label/year=2020/month=01/station=Chabacano/Chabacano2.csv")

    def main(self, sc):
        session = Session()
        credentials = session.get_credentials()
        current_credentials = credentials.get_frozen_credentials()

        spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()

        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", current_credentials.access_key)
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", current_credentials.secret_key)
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.session.token", current_credentials.token)

        #spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", current_credentials.access_key)
        #spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", current_credentials.secret_key)
        #spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3.session.token", current_credentials.token)


        p = spark.read.format('csv').options(header='true', inferSchema='true').load(self.input().path)

        p.write.option("header","true").csv(self.output().path)


if __name__ == "__main__":
    sc = SparkContext()
    
