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


data1 = [
    (1, "A", "A", 1000000),
    (2, "B", "A", 2500000),
    (3, "C", "G", 500000),
    (4, "D", "G", 800000),
    (5, "E", "W", 9000000),
    (6, "F", "W", 2000000),
]
df1 = spark.createDataFrame(data1, ["emp_id", "name", "dept_id", "salary"])
df1.show()

data2 = [("A", "AZURE"), ("G", "GCP"), ("W", "AWS")]
df2 = spark.createDataFrame(data2, ["dept_id1", "dept_name"])
df2.show()



joindf = df1.join(df2,df1["dept_id"]==df2["dept_id1"] , "left")

joindf.show()


seldf = joindf.select("emp_id","name","dept_name","salary").orderBy("dept_name","salary")
seldf.show()


from pyspark.sql.functions import *

exprdf =(
            seldf.groupby("dept_name")
                 .agg(min("salary").alias("salary"))

)

exprdf.show()



finaldf = exprdf.join(df1,["salary"],"inner").drop("dept_id")

finaldf.select("emp_id","name","dept_name","salary").show()