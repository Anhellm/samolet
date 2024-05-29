from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql import Window
from datetime import datetime
import os

spark_host = os.environ.get("SPARK_HOST")
spark_port = os.environ.get("SPARK_PORT")
hdfs_host = os.environ.get("HDFS_HOST")
hdfs_port = os.environ.get("HDFS_PORT")


def read(file_name: str):
    """
    Обработка данных из файла.

    :param p1: Путь к csv файлу.
    :return: Датафрейм.
    """
    conf = (
        SparkConf()
        .setAppName("FApi")
        .setMaster(f"spark://{spark_host}:{spark_port}")
        .set("spark.cores.max", "1")
        .set("spark.executor.memory", "1g")
    )

    schema = (
        StructType()
        .add("announcement_id", IntegerType(), True)
        .add("name", StringType(), True)
        .add("price", IntegerType(), True)
        .add("seller", StringType(), True)
        .add("ram", StringType(), True)
        .add("memory", StringType(), True)
    )

    print("Обработка данных через Спарк", file_name)
    try:
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        df = (
            spark.read.format("csv")
            .option("header", "false")
            .schema(schema)
            .load(file_name)
        )

        window = Window.orderBy("brand")
        split_columns_func = split(df["name"], " ", 2)

        return (
            df.withColumn("brand", split_columns_func.getItem(0))
            .withColumn("model", split_columns_func.getItem(1))
            .groupBy(["model", "brand"])
            .avg("price")
            .select(
                row_number().over(window).alias("id"), "brand", "model", "avg(price)"
            )
        )
    except Exception as e:
        print("Ошибка обработки данных через Спарк", e)
        raise


def send_to_hdfs(df: DataFrame):
    """
    Отправка датафрейма в hdfs.

    :param p1: Датафрейм.
    """
    print("Отправка данных в hdfs")

    if df is None:
        print("Датафрейм не определен")
        return

    try:
        current_date = datetime.now().strftime("%m.%d.%y_%H.%M.%S")
        filename = f"phonedata_{current_date}.parquet"
        df.write.format("parquet").options(
            header="true",
            delimiter=",",
            path=f"hdfs://{hdfs_host}:{hdfs_port}/hadoop/dfs/name/{filename}",
        ).save()
    except Exception as e:
        print("Ошибка записи данных в hdfs", e)
        raise


def send_to_db(df: DataFrame, insert_func):
    """
    Отправка датафрейма в db.

    :param p1: Датафрейм.
    :param p1: Функция для отправки.
    """
    print("Отправка данных в DB")

    if df is None:
        print("Датафрейм не определен")
        return
    if insert_func is None:
        print("Не определена функция для передачи данных")
        return

    data = []
    try:
        for i in df.collect():
            data.append(tuple(i))

        insert_func(data)
    except Exception as e:
        print("Ошибка отправки данных в БД", e)
        raise