from re import split

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys

from pyspark.sql.functions import *

import os, urllib.request, ssl; ssl_context = ssl._create_unverified_context(); [open(path, 'wb').write(urllib.request.urlopen(url, context=ssl_context).read()) for url, path in { "https://github.com/saiadityaus1/SparkCore1/raw/refs/heads/master/data.orc": "data.orc"}.items()]

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

data = [("1",)]
df1 = spark.createDataFrame(data, ["col1"])

df1.show()

datal = [("2",)]

df2 = spark.createDataFrame(datal, ["col2"])

df2.show()

uniondf = df1.union(df2).withColumnRenamed(existing="col1", new="value")
uniondf.show()


data = [("m1", "m1,m2", "m1,m2,m3", "m1,m2,m3,m4")]

df = spark.createDataFrame(data, ["col1", "col2", "col3", "col4"])
df.show()

df1 = (df.select("col1")
           .union(df.select("col2"))
           .union(df.select("col31m8l.f"))
           .union(df.select("col4"))
       )

df1.show()