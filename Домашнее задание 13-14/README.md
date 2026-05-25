# Домашнее задание 13-14 ETL

# Ход выполнения

## 1. Создание PostgreSQL кластера

Создан кластер Managed Service for PostgreSQL.

![Создание PostgreSQL](./Создайте%20кластер-источник%20Managed%20Service%20for%20PostgreSQL.png)

---

## 2. Создание bucket в Object Storage

Создан bucket для хранения данных и результатов ETL.

![Bucket](./Создали%20bucket.png)

---

## 3. Создание таблицы и тестовых данных

В PostgreSQL была создана таблица и подготовлены тестовые данные.

![Таблица и данные](./Создаем%20таблицу%20и%20тестовые%20данные.png)

---

## 4. Создание Data Transfer

Настроен трансфер данных из PostgreSQL в Object Storage.

![Transfer](./Создание%20трансфера.png)

---

## 5. Проверка копирования данных

Проверено успешное копирование данных.

![Копирование](./Копирование%20подтверждено.png)

---

# Data Processing и Spark

## 6. Создание Data Processing кластера

Создан Spark-кластер для обработки данных.

![Data Processing](./Создание%20кластера.png)

---

## 7. Spark ETL Job

Запущен Spark job для обработки данных.

![Spark Job](./Spark%20job.png)

---

# Apache Airflow

## 8. Создание Airflow кластера

Создан и успешно запущен кластер Apache Airflow.

![Airflow](./Airflow%20кластер.png)

---

# Spark ETL Script

Файл `etl_job.py` использовался для запуска ETL-обработки в Spark.

```python
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