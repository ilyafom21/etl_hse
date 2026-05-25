from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("kafka-consumer").getOrCreate()

df = spark.read \
    .format("kafka") \
    .option(
        "kafka.bootstrap.servers",
        "rc1e-i2k1lc5cfnp64b58.mdb.yandexcloud.net:9091"
    ) \
    .option("subscribe", "events") \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option(
        "kafka.sasl.jaas.config",
        'org.apache.kafka.common.security.scram.ScramLoginModule required username="etl_user" password="Etl123456";'
    ) \
    .load()

result = df.selectExpr(
    "CAST(key AS STRING)",
    "CAST(value AS STRING)"
)

result.show(truncate=False)

spark.stop()