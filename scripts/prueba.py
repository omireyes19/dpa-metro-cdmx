import luigi
from boto3 import Session
from pyspark.sql import SparkSession
from luigi.contrib.s3 import S3Target
from luigi.contrib.spark import SparkSubmitTask, PySparkTask


class prueba_task(PySparkTask):
    driver_memory = '2g'
    executor_memory = '3g'

    def input(self):
        return S3Target("s3a://dpa-metro-label/year=2020/month=02/station=Chabacano/Chabacano.csv")

    def output(self):
        return S3Target("s3a://dpa-metro-label/year=2020/month=02/station=Chabacano/Chabacano2.csv")

    def main(self, sc):
        session = Session()
        credentials = session.get_credentials()
        current_credentials = credentials.get_frozen_credentials()

        spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()

        print("prueba_key"+current_credentials.access_key)
        print("prueba_secretkey"+current_credentials.secret_key)
        print("prueba_token"+current_credentials.token)

        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", current_credentials.access_key)
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", current_credentials.secret_key)
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.session.token", current_credentials.token)
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3.S3FileSystem")
 
        print("aqui"+str(self.input()))

        p = spark.read.format('csv').options(header='true', inferSchema='true').load(self.input().path)

        p.write.option("header","true").csv(self.output().path)


if __name__ == "__main__":
    sc = SparkContext()
    
