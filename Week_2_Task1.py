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

print("================Task 1 for today========")

data = sc.textFile("usdata.csv")
print("================RAW DATA========")
print()
data.foreach(print)
print()

lendata = data.filter(lambda x: len(x) > 200)
print("================lendata DATA========")
print()
lendata.foreach(print)
print()

flatdata = lendata.flatMap(lambda x: x.split(","))
print("================flatdata DATA========")
print()
flatdata.foreach(print)
print()

repdata = flatdata.map(lambda x: x.replace("-", ""))
print("================repdata DATA========")
print()
repdata.foreach(print)
print()


condata = repdata.map(lambda x: x + ", zeyo")
print("================condata DATA========")
print()
condata.foreach(print)
print()
