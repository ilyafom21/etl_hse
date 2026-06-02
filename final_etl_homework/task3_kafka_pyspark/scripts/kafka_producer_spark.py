from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct, lit

spark = SparkSession.builder.appName("KafkaRailwayProducer").getOrCreate()

bootstrap_servers = "rc1e-rsk9162u1c95l9d7.mdb.yandexcloud.net:9091"
topic_name = "railway_events"

username = "etl_user"
password = "Etl123456!"

df = spark.read.option("multiline", True).json(
    "s3a://etl-homework-bucket-12345/schedules.json"
)

messages_df = df.select(
    lit("railway").alias("key"),
    to_json(struct("*")).alias("value")
)

messages_df.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("topic", topic_name) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{username}" password="{password}";'
    ) \
    .save()

spark.stop()