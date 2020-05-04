import luigi
from luigi.contrib.s3 import S3Target
from luigi.contrib.spark import SparkSubmitTask, PySparkTask


class prueba_task(PySparkTask):
    """
    This task runs a :py:class:`luigi.contrib.spark.PySparkTask` task
    over the target data in :py:meth:`wordcount.input` (a file in S3) and
    writes the result into its :py:meth:`wordcount.output` target (a file in S3).
    This class uses :py:meth:`luigi.contrib.spark.PySparkTask.main`.
    Example luigi configuration::
        [spark]
        spark-submit: /usr/local/spark/bin/spark-submit
        master: spark://spark.example.org:7077
        # py-packages: numpy, pandas
    """
    driver_memory = '2g'
    executor_memory = '3g'

    def input(self):
        return S3Target("s3://dpa-metro-label/year=2018/month=12/station=Chabacano/Chabacano.csv")

    def output(self):
        return S3Target("s3://dpa-metro-label/year=2018/month=12/station=Chabacano/Chabacano2.csv")

    def main(self, sc, *args):
        sc.textFile(self.input().path) \
          .flatMap(lambda line: line.split()) \
          .map(lambda word: (word, 1)) \
          .reduceByKey(lambda a, b: a + b) \
          .saveAsTextFile(self.output().path)