


from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS

def bool_to_int(val):
    if val:
        return 1
    else:
        return 0

bool_to_int_udf = udf(bool_to_int, IntegerType())

def main():
    spark = SparkSession.builder.appName("LinearRegression").getOrCreate()
    df = spark.read.format("org.apache.phoenix.spark").option("table", "ROLL_INFO_AGG") \
        .option("zkUrl", "namenode:2181").load()
    df = df.filter("SOURCE_INFO_DATE = '2022-05-01'") \
        .withColumn("OWNER_NAME_CHANGE", bool_to_int_udf("OWNER_NAME_CHANGE")) \
        .withColumn("MA_STREET_ADDRESS_CHANGE", bool_to_int_udf("MA_STREET_ADDRESS_CHANGE")) \
        .withColumn("MA_CITY_CHANGE", bool_to_int_udf("MA_CITY_CHANGE")) \
        .withColumn("MA_STATE_CHANGE", bool_to_int_udf("MA_STATE_CHANGE")) \
        .withColumn("MA_DIFFERENT_CITY", bool_to_int_udf("MA_DIFFERENT_CITY")) \
        .withColumn("MA_DIFFERENT_ADDR", bool_to_int_udf("MA_DIFFERENT_ADDR")) \
        .withColumn("FUTURE_SALE", bool_to_int_udf("FUTURE_SALE")) \
        .withColumn("LAST_DOC_DATE_CHANGE", bool_to_int_udf("LAST_DOC_DATE_CHANGE"))

    df = df.select("OWNER_NAME_CHANGE", "MA_STREET_ADDRESS_CHANGE", "MA_CITY_CHANGE", "MA_STATE_CHANGE",
                   "MA_STATE_CHANGE", "MA_DIFFERENT_CITY", "MA_DIFFERENT_ADDR", "LAST_DOC_DATE_CHANGE", "FUTURE_SALE")
    (training, test) = df.randomSplit([0.8, 0.2])

    als = ALS(maxIter=20, regParam=0.01, userCol="OWNER_NAME_CHANGE", itemCol="MA_CITY_CHANGE",
          ratingCol="FUTURE_SALE", coldStartStrategy="drop")
    model = als.fit(training)
    predictions = model.transform(test)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="LAST_DOC_DATE_CHANGE",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error = " + str(rmse))

    bestSell = model.recommendForAllUsers(10)
    print(bestSell)
    bestSell.show(10, truncate=False)

main()

