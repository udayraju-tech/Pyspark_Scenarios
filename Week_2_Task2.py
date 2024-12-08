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

print("================Task 2 for today========")

data = [
    "bigdata~spark~hadoop~hive"
]

rdds = sc.parallelize(data)

print("===============RAW DATA= ============")
print(rdds.collect())
print()

data = ["bigdata~spark~hadoop~hive"]

data = sc.parallelize(data)

flat_data = data.flatMap(lambda x: x.split('~'))

# formatted_data = flat_data.map(lambda x: f"Tech ->{x.upper()}  TRAINER->SAI")

formatted_data = flat_data.map(lambda x: "Tech ->" + x.upper() + "TRAINER->SAI")

print("===============Output DATA= ============")

formatted_data.foreach(print)