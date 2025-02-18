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

from pyspark.sql.functions import  *


data = [("DEPT3", 500),
        ("DEPT3", 200),
        ("DEPT1", 1000),
        ("DEPT1", 700),
        ("DEPT1", 700),
        ("DEPT1", 500),
        ("DEPT2", 400),
        ("DEPT2", 200)]
columns = ["dept", "salary"]
df = spark.createDataFrame(data, columns)
df.show()

# 🔴🔴 STEP 1  CREATE WINDOW ON DEPT WITH DESC ORDER OF SALARY

from  pyspark.sql.window import Window
deptwindow = Window.partitionBy("dept").orderBy(col("salary").desc())

# 🔴🔴 STEP 2  Applying window on DF with dense rank #########

dfrank = df.withColumn("drank",dense_rank().over(deptwindow))
dfrank.show()

# 🔴🔴 Step 3 Filter Rank 2 and Drop drank #####

finaldf = dfrank.filter("drank=2").drop("drank")
finaldf.show()