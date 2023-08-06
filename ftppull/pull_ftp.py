

#config = get_spark_config()

df = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "172.18.0.2:9092")
        .option("subscribe", "json_topic")
        .option("startingOffsets", "earliest") // From starting
        .load()
