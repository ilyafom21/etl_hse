from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("FaceDataProcessing").getOrCreate()

df = spark.read.option("header", True).csv(
    "s3a://etl-homework-bucket-12345/face_data/age_gender.csv"
)

filtered_df = df.filter(col("age") > 30)

filtered_df.write.mode("overwrite").parquet(
    "s3a://etl-homework-bucket-12345/face_data_result/"
)

spark.stop()