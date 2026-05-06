from pyspark.sql import functions as F

employees = spark.createDataFrame(
    [
        ("Alice", "IT", 1000),
        ("Bob", "IT", 1500),
        ("Charlie", "HR", 1200),
        ("David", "HR", 1100),
        ("Eva", "Finance", 2000)
    ],
    ["name", "department", "salary"]
)

employees.show()

result = employees.groupBy("department").agg(
    F.avg("salary").alias("avg_salary"),
    F.count("*").alias("employees_cnt")
)

result.show()

path = "/tmp/bigdata_etl_result/"

result.write.mode("overwrite").parquet(path)

check = spark.read.parquet(path)

check.show()