from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("kafka-producer") \
    .getOrCreate()

data = [
    (1, "click"),
    (2, "purchase"),
    (3, "login")
]

df = spark.createDataFrame(data, ["id", "event"])

kafka_df = df.selectExpr(
    "CAST(id AS STRING) AS key",
    "CAST(event AS STRING) AS value"
)

kafka_df.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "rc1e-i2k1lc5cfnp64b58.mdb.yandexcloud.net:9091") \
    .option("topic", "events") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option(
        "kafka.sasl.jaas.config",
        'org.apache.kafka.common.security.scram.ScramLoginModule required username="etl_user" password="Etl123456";'
    ) \
    .save()

spark.stop()