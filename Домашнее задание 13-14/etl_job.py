from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("etl-job").getOrCreate()

data = [
    (1, "Laptop", 1000),
    (2, "Mouse", 50),
    (3, "Keyboard", 120)
]

df = spark.createDataFrame(data, ["id", "name", "price"])

result = df.where(df.price > 100)

result.write.mode("overwrite").json(
    "s3a://etl-homework-bucket-12345/result/"
)

spark.stop()