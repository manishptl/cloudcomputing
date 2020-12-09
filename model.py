from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.mllib.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.mllib.evaluation import MulticlassMetrics

import pyspark.sql.functions as func
import pyspark

config = SparkConf().setAppName("Wine Quality Prediction").setMaster("local[4]")
sc = SparkContext(conf=config)
myspark = SparkSession.builder.getOrCreate()

# Read the data and Print the schema
print("\nProgram Starting...\n")
defTrain = myspark.read.format('csv').options(header='true', inferSchema='true', delimiter=';').csv("s3://winedataset/TrainingDataset.csv")
print("\nTraining Schema\n")
defTrain.printSchema()
defTrain.count()

featureData = [col for col in defTrain.columns if (col != '""""quality"""""')]

assembler = VectorAssembler(inputCols=featureData, outputCol='features')

dataDF = assembler.transform(defTrain)

print("\n\nPrinting Training Schema with Features Table\n\n")
dataDF.printSchema()

# Random Forest Regression on TrainingDataset

rf = RandomForestClassifier(featuresCol='features', labelCol='""""quality"""""',
                            numTrees=100, maxBins=484, maxDepth=25, minInstancesPerNode=5, seed=34)
rfPipeline = Pipeline(stages=[assembler, rf])
rfPipelineModel = rfPipeline.fit(trainingDF)
evaluator = RegressionEvaluator(
    labelCol='""""quality"""""', predictionCol="prediction", metricName="rmse")
rfTrainingPredictions = rfPipelineModel.transform(defTrain)

rf.save("s3://myprogrambucket/rfwine_model.model")
