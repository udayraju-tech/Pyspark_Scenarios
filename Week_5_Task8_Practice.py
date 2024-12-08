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

## AGG


data = [("sai", 40), ("zeyo", 30), ("sai", 50), ("zeyo", 40), ("sai", 10)]

df = spark.createDataFrame(data, ["name", "amount"])

df.show()
df.printSchema()

from  pyspark.sql.functions import *

aggdf = (

            df
            .groupBy("name")
            .agg(
                sum("amount").alias("total") ,
                count("name").alias("cnt")
            )

)

aggdf.show()




data1 = [("sai","chennai", 40), ("sai","hydb", 50), ("sai","chennai", 10), ("sai","hydb", 60)]

df1 = spark.createDataFrame(data1, ["name", "location", "amount"])

df1.show()
df1.printSchema()




from pyspark.sql.functions import *

aggdf1 =(

    df1.groupby("name","location")
      .agg(sum("amount").alias("total"))

)

aggdf1.show()