
# Homework

## Описание

В рамках первой части домашнего задания был развернут кластер Yandex Data Processing в Yandex Cloud и настроена среда Apache Zeppelin для выполнения ETL-задач с использованием PySpark.

В ходе выполнения задания были подготовлены тестовые данные, реализована ETL-обработка и выполнено сохранение результата в формате Parquet.

Для удобства чтения и проверки кода дополнительно был загружен отдельный файл `BigDataHW.py` (в Zeppelin  он начинается с %spark.pyspark) с основной логикой обработки данных.

---

## Инструменты для выполнения дз

- Yandex Cloud
- Yandex Data Processing
- Apache Spark
- PySpark
- Apache Zeppelin
- HDFS

---

## Выполненные этапы

### 1. Развертывание инфраструктуры

- создан кластер Yandex Data Processing
- настроено окружение Spark
- подключен Apache Zeppelin
- настроен доступ к сервисам кластера

  <img width="882" height="884" alt="кластер" src="https://github.com/user-attachments/assets/0f7838f1-98bc-4dd3-a4e9-ab9548c71d09" />


### 2. Проверка работы Spark

В Zeppelin выполнен тестовый PySpark-код:
- создание DataFrame
- обработка данных
- вывод результата

  <img width="657" height="511" alt="zeppelin" src="https://github.com/user-attachments/assets/36066fa2-ec89-42c1-a0c2-ade127a68ad4" />


### 3. Подготовка файлов проекта

В репозиторий добавлены:
- `BigDataHW.py` — код ETL-обработки
- скриншоты работы кластера и Zeppelin
- README с описанием выполненной работы

Начальные данные
<img width="206" height="152" alt="начальные данные" src="https://github.com/user-attachments/assets/e2700078-1c22-4f44-91b3-3cd77a94be9f" />

Измененные данные
<img width="261" height="123" alt="измененные данные" src="https://github.com/user-attachments/assets/8a9d5db3-1fa1-4e82-b2b7-3bf8cf638e98" />


---

## Структура

```text
Задание к вебинару 11 и 12/
├── README.md
├── BigDataHW.py
└── screenshots/
