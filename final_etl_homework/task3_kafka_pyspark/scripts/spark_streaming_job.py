from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName("KafkaRailwayBatch").getOrCreate()

bootstrap_servers = "rc1e-rsk9162u1c95l9d7.mdb.yandexcloud.net:9091"
topic_name = "railway_events"
username = "etl_user"
password = "Etl123456!"

schema = StructType([
    StructField("train_number", StringType()),
    StructField("station_name", StringType()),
    StructField("arrival_time", StringType()),
    StructField("departure_time", StringType())
])

df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{username}" password="{password}";'
    ) \
    .load()

json_df = df.selectExpr("CAST(value AS STRING) as json_value")

parsed_df = json_df.select(
    from_json(col("json_value"), schema).alias("data")
)

result_df = parsed_df.select(
    col("data.train_number").alias("train_number"),
    col("data.station_name").alias("station_name"),
    col("data.arrival_time").alias("arrival_time"),
    col("data.departure_time").alias("departure_time")
)

result_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("s3a://etl-homework-bucket-12345/kafka_result_csv/")

spark.stop()