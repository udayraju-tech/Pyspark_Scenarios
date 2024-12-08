from re import split

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys

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

print("================Task 1 for today========")

data = sc.textFile("dt.txt")
print("================RAW DATA========")
print()
data.foreach(print)
print()

print("====== STARTED =======")

data = sc.textFile("dt.txt")
print("====== RAW RDD =======")

print()
data.foreach(print)
print()

print("====== RAW RDD =======")

mapsplit = data.map(lambda x: x.split(","))
print("====== mapsplit RDD =======")

print()
mapsplit.foreach(print)

from collections import namedtuple

columns = namedtuple('columns', ['id', 'tno', 'amt', 'category', 'product', 'mode'])

assigncol = mapsplit.map(lambda x: columns(x[0], x[1], x[2], x[3], x[4], x[5]))

prodfilter = assigncol.filter(lambda x: 'Gymnastics' in x.product)

print("====== prodfilter RDD =======")
print()

prodfilter.foreach(print)
print()

df = prodfilter.toDF()

df.show()

df.createOrReplaceTempView("trump")


spark.sql("select id,tno from trump").show()