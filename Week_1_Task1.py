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

spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)


lisin = [ 1 , 2 , 3 , 4]

print("=============Scenario 1 on ADD, MULTI, DIVISION=============")

rddin = sc.parallelize(lisin)

print("=============RAW RDD=============")
print()
print(rddin.collect())


addin = rddin.map(lambda x : x + 2)
print("=============ADD RDD============")
print()
print(addin.collect())


mulin  = rddin.map(lambda x : x * 10)
print("=============mulin RDD============")
print()
print(mulin.collect())

#   [ 1 , 2 , 3 , 4]

filrdd = rddin.filter(lambda x : x > 2)
print("=============filrdd RDD============")
print()
print(filrdd.collect())

