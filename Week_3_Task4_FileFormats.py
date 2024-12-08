from re import split

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys

import os
import urllib.request
import ssl

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['JAVA_HOME'] = r''

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()


orcdf = (
    spark
    .read
    .format("orc")
    .load("data.orc")
)

orcdf.show()

csvdf = (
    spark
    .read
    .format("csv")
    .option("header", "true")
    .load("df.csv")
)

csvdf.show()
#
# sqldf = (
#     spark
#     .read
#     .format("jdbc")
#     .option("url", "jdbc:mysql://karpdb.c5mme8020mxc.ap-southeast-1.rds.amazonaws.com:3306/karp")
#     .option("driver", "com.mysql.jdbc.Driver")
#     .option("user", "root")
#     .option("password", "Karpusa908")
#     .option("dbtable", "fruits")
#     .load()
# )
#
# sqldf.show()

parquetdf = (
    spark
    .read
    .format("parquet")
    .load("file5.parquet")
)

jsondf = (
    spark
    .read
    .format("json")
    .load("file4.json")
)

jsondf.show(1000)
