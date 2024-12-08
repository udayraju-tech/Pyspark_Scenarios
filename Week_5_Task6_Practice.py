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
df1.show()

data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]

df2 = spark.createDataFrame(data3, ["id", "product"])
df2.show()



listval = df2.select("id").rdd.flatMap(lambda x : x).collect()
print(listval)

from pyspark.sql import functions as F


finaldf = df1.filter(~F.col('id').isin(listval))
finaldf.show()

antijoin = df1.join(df2, ["id"] , "left_anti")
antijoin.show()