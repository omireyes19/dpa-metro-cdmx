from pyspark.sql.functions import col,monotonically_increasing_id
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.ml.tuning import CrossValidator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import OneHotEncoderEstimator
from pyspark.sql.types import IntegerType
import numpy as np
from math import floor

class predictions:
    def get_predictions(self,spark,df):
        data = spark.createDataFrame(df)

        n = data.count()

        data = data.withColumn('line_crossing', col('line_crossing').cast(IntegerType()))
        data = data.withColumnRenamed('label', 'label_prev')

        cut_date = floor(n * .7)

        categoricalColumns = ['day_of_week', 'line']
        stages = []
        for categoricalCol in categoricalColumns:
            stringIndexer = StringIndexer(inputCol=categoricalCol, outputCol=categoricalCol + "Index")
            encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
            stages += [stringIndexer, encoder]

        label_stringIdx = StringIndexer(inputCol="label_prev", outputCol="label")
        stages += [label_stringIdx]

        numericCols = ["year", "month", "line_crossing"]
        assemblerInputs = [c + "classVec" for c in categoricalColumns] + numericCols
        assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
        stages += [assembler]

        partialPipeline = Pipeline().setStages(stages)
        pipelineModel = partialPipeline.fit(data)
        preppedDataDF = pipelineModel.transform(data)

        id_data = preppedDataDF.withColumn('id', monotonically_increasing_id())
        trainingData = id_data.filter(col('id') < cut_date).drop('id')
        testingData = id_data.filter(col('id') > cut_date).drop('id')

        rf = RandomForestClassifier(labelCol="label", featuresCol="features")

        paramGrid = ParamGridBuilder() \
            .addGrid(rf.numTrees, [int(x) for x in np.linspace(start = 10, stop = 50, num = 1)]) \
            .addGrid(rf.maxDepth, [int(x) for x in np.linspace(start = 5, stop = 25, num = 1)]) \
            .build()

        crossval = CrossValidator(estimator=rf,
                                  estimatorParamMaps=paramGrid,
                                  evaluator=MulticlassClassificationEvaluator(),
                                  numFolds=3)

        cvModel = crossval.fit(trainingData)

        predictions = cvModel.transform(testingData).toPandas()

        return predictions