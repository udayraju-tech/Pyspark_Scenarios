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
source_rdd = spark.sparkContext.parallelize([
    (1, "A"),
    (2, "B"),
    (3, "C"),
    (4, "D")
], 1)

target_rdd = spark.sparkContext.parallelize([
    (1, "A"),
    (2, "B"),
    (4, "X"),
    (5, "F")
], 2)

# Convert RDDs to DataFrames using toDF()
df1 = source_rdd.toDF(["id", "name"])
df2 = target_rdd.toDF(["id", "name1"])

# Show the DataFrames
df1.show()
df2.show()

print("===== FULL JOIN=====")

fulljoin = df1.join(df2, ["id"], "full")
fulljoin.show()

from pyspark.sql.functions import *

print("=====NAME AND NAME 1 MATCH=====")

fulljoin = df1.join(df2, ["id"], "full")
fulljoin.show()

procdf = fulljoin.withColumn("status", expr("case when name=name1 then 'match' else 'mismatch' end"))
procdf.show()

print("=====FILTER MISMATCH=====")

fildf = procdf.filter("status='mismatch'")
fildf.show()

print("=====NULL CHECKS=====")

procdf1 = (

    fildf.withColumn("status", expr("""
                                            case
                                            when name1 is null then 'New In Source'
                                            when name is null  then 'New In target'
                                            else
                                            status
                                            end


                                            """))

)

procdf1.show()

print("=====FINAL PROC=====")

finaldf = procdf1.drop("name", "name1").withColumnRenamed("status", "comment")

finaldf.show()