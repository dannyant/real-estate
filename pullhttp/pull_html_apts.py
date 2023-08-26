from pyspark.shell import sc, spark

df = spark.read.format("org.apache.phoenix.spark").option("table", "apartments_property").option("zkUrl", "192.168.1.162:2181").load()

df.collect()
print(df)