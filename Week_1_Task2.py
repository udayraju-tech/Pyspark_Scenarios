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

print("=========Scenario 2=========")

listr  = ["zeyobron" , "zeyo" , "analytics"]

print("===============RAW STRING LIST===========")
rddstr = sc.parallelize(listr)
print(rddstr.collect())

print("===============STRING JOINS LIST===========")
StrJoins = rddstr.map(lambda x: x+ "analytics")
print(StrJoins.collect())

print("===============STRING REPLACE===========")
replaceString = rddstr.map(lambda x: x.replace("zeyo", "tera"))
print(replaceString.collect())


print("===============STRING Filter===========")
FilterString = rddstr.filter(lambda x: 'zeyo' in x)
print(FilterString.collect())

print("===============Split and FlatMap Operation===========")
rawlist=[ "A~B" , "C~D" ]

rddstr= sc.parallelize(rawlist)
print(rddstr.collect())

print("===============FlatMap Filter===========")
FlatMapp = rddstr.flatMap(lambda x: x.split("~"))
print(FlatMapp.collect())

print("===============Split and FlatMap Multiple Operations===========")
rawlist1=[ "A~B,C" , "C~D,E" ]

rddstr1= sc.parallelize(rawlist1)
print(rddstr1.collect())

print("===============FlatMap multiple Split's===========")
FlatMapp1 = rddstr1.flatMap(lambda x: x.split("~")).flatMap(lambda x: x.split(","))
print(FlatMapp1.collect())

