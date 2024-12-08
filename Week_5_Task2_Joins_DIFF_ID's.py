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

data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]

df1 = spark.createDataFrame(data4, ["id", "name"])
print("************Test DATA Prepared for DF1************")
df1.show()

data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]

df2 = spark.createDataFrame(data3, ["id1", "product"])

print("************Test DATA Prepared for DF2************")
df2.show()

print("************INNER JOIN************")
innerjoin = df1.join(df2, df1["id"] == df2["id1"] , "inner").orderBy("id").drop("id1")
innerjoin.show()


print("************LEFT JOIN************")
leftjoin = df1.join(df2,  df1["id"] == df2["id1"] ,"left").orderBy("id").drop("id1")
leftjoin.show()


print("************RIGHT JOIN************")
rightjoin = df1.join(df2, df1["id"] == df2["id1"]  , "right" ).orderBy("id1").drop("id")
rightjoin.show()


from pyspark.sql.functions import *

print("************FULL JOIN************")
print("************FULL JOIN with NULL's in ID Column to Replace CASE************")
fulljoin  = (
            df1.join(df2,  df1["id"] == df2["id1"]  , "full")
            .withColumn("id",expr("case when id is null then id1 else id end"))
            .drop("id1")
            .orderBy("id")

)

fulljoin.show()